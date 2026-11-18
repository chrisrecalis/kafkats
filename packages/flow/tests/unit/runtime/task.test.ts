import { describe, expect, it } from 'vitest'
import { FailedProcessingError, IllegalStateError } from '@/api/errors.js'
import { string } from '@/codec.js'
import type {
	InternalProcessorContext,
	MaybePromise,
	ProcessorContext,
	ProcessorSupplier,
	RecordMetadata,
} from '@/processor/api.js'
import { decodeCommitMetadata, encodeCommitMetadata, ProcessorMetadataImpl } from '@/processor/metadata.js'
import type { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { PartitionStart, StreamTask, TaskState } from '@/runtime/task.js'
import type { KeyValueStore, StateStore } from '@/state.js'
import type { RegisteredStore } from '@/state/state-manager.js'
import type { StoreClock } from '@/state/time-clipping.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { kvStore, passThrough, pipe, TaskHarness } from '../support/task-harness.js'

type Ctx = ProcessorContext<string, string>

/** A processor that records what it sees and forwards it; its context is kept for the test. */
function recorder() {
	const state = {
		instances: 0,
		inits: 0,
		closes: 0,
		seen: [] as StreamRecord<string, string>[],
		metadata: [] as (RecordMetadata | null)[],
		context: undefined as Ctx | undefined,
	}
	const supplier: ProcessorSupplier<string, string, string, string> = () => {
		state.instances++
		let context: Ctx
		return {
			init: ctx => {
				state.inits++
				context = ctx
				state.context = ctx
			},
			process: record => {
				state.seen.push(record)
				state.metadata.push(context.recordMetadata())
				return context.forward(record)
			},
			close: () => {
				state.closes++
			},
		}
	}
	return { state, supplier }
}

/** Two co-partitioned topics `a` and `b` into one processor. */
function twoTopics(
	supplier: ProcessorSupplier<string, string, string, string>,
	stores = [kvStore('w', { clock: {} })]
) {
	return (b: InternalTopologyBuilder) => {
		for (const topic of ['a', 'b']) {
			b.addSource({
				name: `src-${topic}`,
				topics: [topic],
				keyCodec: string(),
				valueCodec: string(),
				internal: false,
			})
		}
		for (const store of stores) b.addStateStore(store)
		b.addProcessor({
			name: 'processor',
			parents: ['src-a', 'src-b'],
			stores: stores.map(s => s.name),
			supplier: supplier as ProcessorSupplier<unknown, unknown, unknown, unknown>,
		})
		b.addSink({
			name: 'sink',
			parents: ['processor'],
			topic: 'output',
			keyCodec: string(),
			valueCodec: string(),
			internal: false,
		})
	}
}

function committed(metadata: Record<string, number>, partitionTime: number, offset = 10n): PartitionStart {
	const encoded = new ProcessorMetadataImpl()
	encoded.replaceAll([new Map(Object.entries(metadata))], new Set(Object.keys(metadata)))
	return { committed: { offset, metadata: encodeCommitMetadata(partitionTime, encoded) }, resolved: offset }
}

const records = (...timestamps: number[]) =>
	timestamps.map(timestamp => ({ key: 'k', value: `v${timestamp}`, timestamp }))

async function inState(h: TaskHarness, state: TaskState): Promise<StreamTask> {
	const task = h.task()
	if (state === 'CREATED') return task
	await task.initialize()
	if (state === 'RESTORING') return task
	task.setStarts(new Map())
	await task.completeRestoration(new Map())
	if (state === 'RUNNING') return task
	await task.suspend()
	if (state === 'SUSPENDED') return task
	await task.close('clean')
	return task
}

/** Store clocks come with the clipping layer (state/time-clipping.ts): until then the test gives store `w` one. */
function giveClock(task: StreamTask): StoreClock {
	let time = -1
	const raise = (to: number) => {
		time = Math.max(time, to)
	}
	const clock: StoreClock = {
		get time() {
			return time
		},
		advance: raise,
		seed: raise,
		readFloor: Number.MIN_SAFE_INTEGER,
		expiryCutoff: Number.MIN_SAFE_INTEGER,
	}
	const stores = task.stores as RegisteredStore[]
	const index = stores.findIndex(store => store.builder.name === 'w')
	stores[index] = { ...stores[index]!, clock }
	return clock
}

const STATES: readonly TaskState[] = ['CREATED', 'RESTORING', 'RUNNING', 'SUSPENDED', 'CLOSED']

describe('StreamTask lifecycle (4.4)', () => {
	it('goes through every state with its effects; a new processor instance per RUNNING incarnation', async () => {
		const { state, supplier } = recorder()
		const h = new TaskHarness({ build: b => pipe(b, { processor: supplier, stores: [kvStore('s')] }) })
		const task = h.task()
		expect(task.state).toBe('CREATED')
		expect(task.inputPartitions).toEqual([{ topic: 'input', partition: 0 }])

		// The builder calls the supplier twice to check that it makes new instances.
		const checked = state.instances
		await task.initialize()
		expect(task.state).toBe('RESTORING')
		expect(h.state(task).registered).toBe(true)
		expect(state.instances).toBe(checked)

		task.setStarts(new Map())
		await task.completeRestoration(new Map())
		expect(task.state).toBe('RUNNING')
		expect([state.instances - checked, state.inits]).toEqual([1, 1])

		h.enqueue(task, 'input', records(1, 2))
		await task.processOne(h.clock.now())
		await task.suspend()
		expect(task.state).toBe('SUSPENDED')
		expect(state.closes).toBe(1)
		expect(h.state(task).stackClosed).toBe(false)

		task.resume()
		expect(task.state).toBe('RESTORING')
		await task.completeRestoration(new Map())
		expect([state.instances - checked, state.inits]).toEqual([2, 2])
		// The record queued before the suspension was dropped: it is fetched again from the committed offset.
		expect(await h.process(task)).toBe(0)

		await task.suspend()
		expect(await task.close('clean')).toBe('clean')
		expect(task.state).toBe('CLOSED')
		expect(h.state(task).stackClosed).toBe(true)
		expect(h.state(task).released).toEqual({})
		expect(h.state(task).bufferDiscards).toBe(0)
	})

	const ops = {
		initialize: (t: StreamTask) => t.initialize(),
		completeRestoration: (t: StreamTask) => t.completeRestoration(new Map()),
		suspend: (t: StreamTask) => t.suspend(),
		resume: async (t: StreamTask) => t.resume(),
		closeClean: (t: StreamTask) => t.close('clean'),
	}
	const legal: Record<TaskState, Partial<Record<keyof typeof ops, TaskState>>> = {
		CREATED: { initialize: 'RESTORING', suspend: 'SUSPENDED' },
		RESTORING: { completeRestoration: 'RUNNING', suspend: 'SUSPENDED' },
		RUNNING: { suspend: 'SUSPENDED' },
		SUSPENDED: { resume: 'RESTORING', closeClean: 'CLOSED' },
		CLOSED: { closeClean: 'CLOSED' },
	}
	for (const from of STATES) {
		for (const [op, run] of Object.entries(ops) as [keyof typeof ops, (t: StreamTask) => Promise<unknown>][]) {
			const to = legal[from][op]
			it(`${from} ${op}: ${to ?? 'IllegalStateError'}`, async () => {
				const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
				const task = await inState(h, from)
				if (to === undefined) {
					await expect(run(task)).rejects.toThrow(IllegalStateError)
					expect(task.state).toBe(from)
				} else {
					await run(task)
					expect(task.state).toBe(to)
				}
			})
		}
	}

	for (const from of STATES) {
		it(`a dirty close from ${from} suspends, drops buffers and releases (invalidate), and never throws`, async () => {
			let failClose = false
			const h = new TaskHarness({
				build: b =>
					pipe(b, {
						processor: () => ({
							process() {},
							close() {
								if (failClose) throw new Error('close failed')
							},
						}),
					}),
			})
			const task = await inState(h, from)
			failClose = true
			const state = h.state(task)
			const discardsBefore = state.bufferDiscards
			state.release = () => Promise.reject(new Error('release failed'))
			expect(await task.close('dirty', { invalidate: true })).toBe(from === 'CLOSED' ? 'clean' : 'dirty')
			expect(task.state).toBe('CLOSED')
			if (from !== 'CLOSED') {
				expect(state.stackClosed).toBe(true)
				expect(state.bufferDiscards).toBe(discardsBefore + 1)
			}
		})
	}

	it('a dirty close passes invalidate to the release', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await inState(h, 'RUNNING')
		await task.close('dirty', { invalidate: true })
		expect(h.state(task).released).toEqual({ invalidate: true })
	})

	it('a clean close of a task with uncommitted work closes dirty (KS)', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1))
		await h.process(task)
		await task.suspend()
		expect(await task.close('clean')).toBe('dirty')
		expect(h.state(task).bufferDiscards).toBe(1)
		expect(h.logger.warnings.some(w => w.includes('uncommitted'))).toBe(true)
		expect(await task.close('clean')).toBe('dirty')
	})

	it('a clean close that fails closes dirty', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await inState(h, 'SUSPENDED')
		let releases = 0
		h.state(task).release = async () => {
			if (releases++ === 0) throw new Error('release failed')
		}
		expect(await task.close('clean')).toBe('dirty')
		expect(releases).toBe(2)
	})

	it('a failing init() closes only the processors initialized before it and leaves the task RESTORING (KS)', async () => {
		const events: string[] = []
		const node =
			(name: string, init = () => {}): ProcessorSupplier<unknown, unknown, unknown, unknown> =>
			() => ({
				init: () => {
					events.push(`${name}.init`)
					init()
				},
				process() {},
				close: () => void events.push(`${name}.close`),
			})
		const failing = () => {
			throw new Error('init failed')
		}
		const h = new TaskHarness({
			build: b => {
				b.addSource({ name: 'source', topics: ['input'], internal: false })
				b.addProcessor({ name: 'a', parents: ['source'], stores: [], supplier: node('a') })
				b.addProcessor({ name: 'b', parents: ['a'], stores: [], supplier: node('b', failing) })
				b.addProcessor({ name: 'c', parents: ['b'], stores: [], supplier: node('c') })
			},
		})
		const task = h.task()
		await task.initialize()
		await expect(task.completeRestoration(new Map())).rejects.toThrow('failed to initialize processor')
		expect(task.state).toBe('RESTORING')
		expect(task.initializing).toBe(false)
		expect(events).toEqual(['a.init', 'b.init', 'a.close'])
		expect(await task.close('dirty')).toBe('dirty')
		expect(events).toHaveLength(3)
	})
})

describe('incarnation boundary (CR9)', () => {
	it('a forward or store write of a suspended incarnation throws and changes nothing', async () => {
		let context!: Ctx
		let store!: KeyValueStore<string, string>
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					stores: [kvStore('s')],
					processor: () => ({
						init: ctx => {
							context = ctx
							store = ctx.getStateStore<KeyValueStore<string, string>>('s')
						},
						process: record => context.forward(record),
					}),
				}),
		})
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1))
		await h.process(task)
		await store.put('k', 'before')
		const record = h.producer.sent.length
		await task.suspend()
		expect(() => context.forward({ key: 'k', value: 'late', timestamp: 2, headers: {} } as never)).toThrow(
			IllegalStateError
		)
		await expect(store.put('k', 'late')).rejects.toThrow(IllegalStateError)
		expect(store.name).toBe(`app.s.p0`)
		expect(await (task.store('s') as KeyValueStore<string, string>).get('k')).toBe('before')
		expect(h.producer.sent).toHaveLength(record)
	})

	it('a record or punctuation still running when its incarnation closed counts for nothing (orphaned slice)', async () => {
		let release!: () => void
		const blocked = () => new Promise<void>(resolve => (release = resolve))
		const start = async (process: () => MaybePromise<void>) => {
			const h = new TaskHarness({
				build: b =>
					pipe(b, {
						processor: () => ({ init: ctx => void ctx.schedule(10, 'stream-time', blocked), process }),
					}),
			})
			const task = await h.start(h.task())
			h.enqueue(task, 'input', records(1))
			return { h, task }
		}
		// A record: not consumed, so the task has nothing to commit.
		const first = await start(blocked)
		const record = first.task.processOne(first.h.clock.now())
		await first.task.suspend()
		release()
		expect(await record).toBe(false)
		expect(await first.task.close('clean')).toBe('clean')
		// A punctuation: it ran, but its incarnation's work is void.
		const second = await start(() => {})
		expect(await second.task.processOne(second.h.clock.now())).toBe(true)
		expect(await second.h.commit()).toBe('committed')
		const punctuation = second.task.maybePunctuateStreamTime()
		await second.task.suspend()
		release()
		expect(await punctuation).toBe(true)
		expect(await second.task.close('clean')).toBe('clean')
	})

	it('a failure of an incarnation closed meanwhile counts for nothing; one of the running incarnation is thrown', async () => {
		let release!: () => void
		const blocked = () => new Promise<void>(resolve => (release = resolve))
		const start = async (value: string) => {
			const h = new TaskHarness({
				build: b =>
					pipe(b, {
						stores: [kvStore('s')],
						processor: () => {
							let store: KeyValueStore<string, string>
							return {
								init: ctx => {
									store = ctx.getStateStore('s')
									ctx.schedule(10, 'stream-time', async () => {
										await blocked()
										await store.put('k', 'punctuated')
									})
								},
								process: async record => {
									await blocked()
									if (record.value === 'fail') throw new Error('process failed')
									await store.put(record.key!, record.value!)
								},
							}
						},
					}),
			})
			const task = await h.start(h.task())
			h.enqueue(task, 'input', [{ key: 'k', value, timestamp: 1 }])
			return { h, task, stored: () => (task.store('s') as KeyValueStore<string, string>).get('k') }
		}
		// The running incarnation: the processing handler's 'fail' reaches the caller.
		const running = await start('fail')
		const failing = running.task.processOne(running.h.clock.now())
		release()
		await expect(failing).rejects.toThrow(FailedProcessingError)
		// A record whose write the close forbade: no failure, nothing written, nothing to commit.
		const record = await start('late')
		const late = record.task.processOne(record.h.clock.now())
		await record.task.suspend()
		release()
		expect(await late).toBe(false)
		expect(await record.stored()).toBeUndefined()
		expect(record.h.logger.warnings).toContain('ignoring a failure of processors closed meanwhile')
		expect(await record.task.close('clean')).toBe('clean')
		// A punctuation whose write the close forbade: the same.
		const punctuated = await start('v')
		const processing = punctuated.task.processOne(punctuated.h.clock.now())
		release()
		expect(await processing).toBe(true)
		expect(await punctuated.h.commit()).toBe('committed')
		const punctuation = punctuated.task.maybePunctuateStreamTime()
		await punctuated.task.suspend()
		release()
		expect(await punctuation).toBe(false)
		expect(await punctuated.stored()).toBe('v')
		expect(await punctuated.task.close('clean')).toBe('clean')
	})

	it('processors whose init() outlived a close that did not wait for it are closed once initialized; their failure counts for nothing', async () => {
		const start = async (init: (store: KeyValueStore<string, string>) => Promise<void>) => {
			let release!: () => void
			let closes = 0
			const h = new TaskHarness({
				build: b =>
					pipe(b, {
						stores: [kvStore('s')],
						processor: () => ({
							init: async ctx => {
								const store = ctx.getStateStore<KeyValueStore<string, string>>('s')
								await new Promise<void>(resolve => (release = resolve))
								await init(store)
							},
							process() {},
							close() {
								closes++
							},
						}),
					}),
			})
			const task = h.task()
			await task.initialize()
			task.setStarts(new Map())
			const completing = task.completeRestoration(new Map())
			expect(task.initializing).toBe(true)
			expect(await task.close('dirty')).toBe('dirty')
			// The output no longer takes its sends.
			expect(task.initializing).toBe(false)
			release()
			await completing
			expect(task.state).toBe('CLOSED')
			expect(task.hasWork(h.clock.now(), h.lag)).toBe(false)
			return { h, closes }
		}
		// A store write the close forbade: no failure, and no close() of the processor whose init() failed.
		const failed = await start(store => store.put('k', 'v'))
		expect(failed.h.logger.warnings).toContain('ignoring a failure of processors closed meanwhile')
		expect(failed.closes).toBe(0)
		// An init() that succeeds after the close: the task stays CLOSED, without processors.
		const succeeded = await start(async () => {})
		expect(succeeded.h.logger.warnings).toEqual([])
		expect(succeeded.closes).toBe(1)
	})

	it('records delivered to a suspended task are dropped', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await inState(h, 'SUSPENDED')
		expect(h.enqueue(task, 'input', records(1))).toBe(0)
	})

	it('a lost task processes, punctuates and sends nothing more', async () => {
		let context!: Ctx
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => ({
						init: ctx => {
							context = ctx
							ctx.schedule(10, 'stream-time', () => {})
						},
						process: record => context.forward(record),
					}),
				}),
		})
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1, 2))
		task.markLost()
		expect(task.lost).toBe(true)
		const now = h.clock.now()
		expect(task.isProcessable(now, h.lag)).toBe(false)
		expect(task.hasWork(now, h.lag)).toBe(false)
		expect(await task.processOne(now)).toBe(false)
		expect(await task.maybePunctuateStreamTime()).toBe(false)
		h.collector.forTask(task).send('sink', { topic: 'output', key: null, value: null })
		expect(h.producer.sent).toHaveLength(0)
	})
})

describe('seeding from the commits (D5, KF7)', () => {
	it('replaces the processor metadata by the max-merge of the committed one, restricted to the known keys', async () => {
		let metadata!: InternalProcessorContext['metadata']
		const supplier: ProcessorSupplier<string, string, string, string> = () => {
			let clock: ObservedStreamTime
			let context: Ctx
			return {
				init: ctx => {
					context = ctx
					metadata = (ctx as InternalProcessorContext<string, string>).metadata
					clock = new ObservedStreamTime(ctx as InternalProcessorContext<string, string>, 'w')
				},
				process: record => {
					clock.advance(record.timestamp)
					return context.forward(record)
				},
			}
		}
		const h = new TaskHarness({ build: twoTopics(supplier) })
		const task = await h.start(h.task())
		h.enqueue(task, 'a', records(500))
		await h.process(task)
		expect(metadata.get('w')).toBe(500)
		await task.suspend()

		task.resume()
		task.setStarts(
			new Map([
				['a:0', committed({ w: 120, 'w.observed': 80, removed: 7 }, 90)],
				['b:0', committed({ w: 100 }, 130)],
			])
		)
		const clock = giveClock(task)
		await task.completeRestoration(new Map([['w', 95]]))
		expect(metadata.get('w')).toBe(120)
		expect(metadata.get('w.observed')).toBe(80)
		expect(metadata.get('removed')).toBeUndefined()
		expect(task.streamTime).toBe(130)
		// The committed clock, not the restored one: the replay of the records after the commit advances it again.
		expect(clock.time).toBe(80)
		// Seeding is not a change to commit.
		expect(task.needsCommit(h.consumer.nextOffset.bind(h.consumer))).toBe(false)
	})

	it('B14: seeding the clocks reads no store', async () => {
		const h = new TaskHarness({ build: twoTopics(passThrough) })
		const task = h.task()
		await task.initialize()
		const clock = giveClock(task)
		const reads: string[] = []
		for (const { raw } of task.stores) {
			const store = raw as unknown as Record<string, (...args: never[]) => unknown>
			for (const method of ['get', 'all', 'range']) {
				const original = store[method]!.bind(store)
				store[method] = (...args) => (reads.push(method), original(...args))
			}
		}
		task.setStarts(new Map([['a:0', committed({ w: 100, 'w.observed': 80 }, 90)]]))
		await task.completeRestoration(new Map([['w', 95]]))
		expect(clock.time).toBe(80)
		expect(task.streamTime).toBe(90)
		expect(reads).toEqual([])
	})

	it('a store clock is seeded with its committed time; with no commit metadata, with the time the restore applied', async () => {
		const cases: [PartitionStart | undefined, number][] = [
			[committed({ 'w.observed': 300 }, 10), 300],
			// At-least-once: the restore applied the writes made after the commit, at later key times.
			[committed({ 'w.observed': 100 }, 10), 100],
			// The store was not written before the commit.
			[committed({ other: 5 }, 10), -1],
			[{ committed: { offset: 1n, metadata: '' }, resolved: 1n }, 200],
			[{ committed: { offset: 1n, metadata: 'garbage!' }, resolved: 1n }, 200],
			[undefined, 200],
		]
		for (const [start, seeded] of cases) {
			const h = new TaskHarness({ build: twoTopics(passThrough) })
			const task = h.task()
			await task.initialize()
			const clock = giveClock(task)
			task.setStarts(new Map(start === undefined ? [] : [['a:0', start]]))
			await task.completeRestoration(new Map([['w', 200]]))
			expect(clock.time, start?.committed?.metadata ?? 'no commit').toBe(seeded)
		}
	})

	it('metadata that cannot be decoded seeds nothing and warns once', async () => {
		const h = new TaskHarness({ build: twoTopics(passThrough) })
		const task = h.task()
		await task.initialize()
		task.setStarts(
			new Map([
				['a:0', { committed: { offset: 1n, metadata: 'garbage!' }, resolved: 1n }],
				['b:0', { committed: { offset: 1n, metadata: Buffer.from([9]).toString('base64') }, resolved: 1n }],
			])
		)
		await task.completeRestoration(new Map())
		expect(task.streamTime).toBe(-1)
		expect(h.logger.warnings.filter(w => w.includes('commit metadata'))).toHaveLength(1)
	})
})

describe('processing', () => {
	it('passes each record with its input record metadata, timestamp and headers to the sink', async () => {
		const { state, supplier } = recorder()
		const h = new TaskHarness({ build: b => pipe(b, { processor: supplier }) })
		const task = await h.start(h.task())
		h.enqueue(task, 'input', [{ key: 'k', value: 'v', timestamp: 42, headers: { h: Buffer.from('1') } }])
		// A synchronous subtree is processed without a promise (lean path, SI14).
		expect(task.processOne(h.clock.now())).toBe(true)
		expect(state.metadata).toEqual([expect.objectContaining({ topic: 'input', partition: 0, offset: 0n })])
		expect(h.producer.sent[0]!.message).toMatchObject({ timestamp: new Date(42), headers: { h: Buffer.from('1') } })
		expect(h.output('output')).toEqual([{ key: 'k', value: 'v', timestamp: 42 }])
		expect(await task.processOne(h.clock.now())).toBe(false)
		expect(task.queueDrained).toBe(false)
	})

	it('a record whose subtree is asynchronous returns a promise that settles once it was processed', async () => {
		let release!: () => void
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => ({
						process: () => new Promise<void>(resolve => (release = resolve)),
					}),
				}),
		})
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1))
		const result = task.processOne(h.clock.now())
		expect(result).toBeInstanceOf(Promise)
		expect(task.needsCommit(() => undefined)).toBe(false)
		expect(task.input).toMatchObject({ topic: 'input', partition: 0, offset: 0n })
		release()
		expect(await result).toBe(true)
		expect(task.queueDrained).toBe(true)
		expect(task.needsCommit(() => undefined)).toBe(true)
		// The record context ends with the record (4.5.6): a later send is not reported with it.
		expect(task.input).toBeNull()
	})

	it('every processor reads the global stores without being connected to them (KS)', async () => {
		const seen: (string | undefined)[] = []
		const h = new TaskHarness({
			build: b => {
				pipe(b, {
					stores: [kvStore('s')],
					processor: () => {
						let context: Ctx
						return {
							init: ctx => {
								context = ctx
							},
							process: async record => {
								const local = context.getStateStore<KeyValueStore<string, string>>('s')
								const global = context.getStateStore<KeyValueStore<string, string>>('g')
								seen.push(await local.get(record.key!), await global.get(record.key!))
							},
						}
					},
				})
				b.addGlobalStore({
					topic: 'global',
					sourceName: 'global-source',
					processorName: 'global-processor',
					store: kvStore('g'),
					supplier: () => ({ process() {} }),
				})
			},
		})
		const view = { name: 'g', get: (key: string) => Promise.resolve(`global ${key}`) }
		h.globalStores.set('g', view as unknown as StateStore)
		const task = await h.start(h.task())
		await (task.store('s') as KeyValueStore<string, string>).put('k', 'local k')
		h.enqueue(task, 'input', records(1))
		await h.process(task)
		expect(seen).toEqual(['local k', 'global k'])
	})

	it('every source node of the topic gets the record, in registration order', async () => {
		const seen: string[] = []
		const h = new TaskHarness({
			build: b => {
				for (const name of ['s1', 's2']) {
					b.addSource({ name, topics: ['input'], keyCodec: string(), valueCodec: string(), internal: false })
					b.addProcessor({
						name: `p-${name}`,
						parents: [name],
						stores: [],
						supplier: () => ({
							process: async (record: StreamRecord<unknown, unknown>) => {
								await Promise.resolve()
								seen.push(`${name}:${String(record.value)}`)
							},
						}),
					})
				}
			},
		})
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1, 2))
		await h.process(task)
		expect(seen).toEqual(['s1:v1', 's2:v1', 's1:v2', 's2:v2'])
	})

	it('backpressure: a partition over the limit is reported once its queue dropped back to it', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), bufferedRecordsPerPartition: 2 })
		const task = await h.start(h.task())
		expect(h.enqueue(task, 'input', records(1, 2))).toBe(2)
		expect(h.enqueue(task, 'input', records(3, 4))).toBe(4)
		await task.processOne(h.clock.now())
		expect(task.partitionsToResume()).toEqual([])
		await task.processOne(h.clock.now())
		expect(task.partitionsToResume()).toEqual([{ topic: 'input', partition: 0 }])
		expect(task.partitionsToResume()).toEqual([])
		await h.process(task)
		expect(task.partitionsToResume()).toEqual([])
	})

	it('a skipped record only advances the committed offset', async () => {
		const h = new TaskHarness({
			build: b => pipe(b, { processor: passThrough }),
			extractor: record => record.timestamp,
		})
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(-1, -1))
		expect(await task.processOne(h.clock.now())).toBe(true)
		expect(h.producer.sent).toHaveLength(0)
		expect(task.committableOffsets(tp => h.consumer.nextOffset(tp))).toMatchObject([{ offset: 2n }])
	})

	it('hasWork: records ready or a punctuation due', async () => {
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => ({
						init: ctx => void ctx.schedule(100, 'wall-clock', () => {}),
						process() {},
					}),
				}),
		})
		const task = h.task()
		expect(task.hasWork(0, h.lag)).toBe(false)
		await h.start(task)
		const now = h.clock.now()
		expect(task.hasWork(now, h.lag)).toBe(false)
		expect(task.nextWallClockDue()).toBe(now + 100)
		expect(task.hasWork(now + 100, h.lag)).toBe(true)
		h.enqueue(task, 'input', records(1))
		expect(task.hasWork(now, h.lag)).toBe(true)
	})
})

describe('punctuation (4.9)', () => {
	it('stream time: due from the first known stream time, with the stream time; marks a commit needed', async () => {
		const calls: [number, RecordMetadata | null][] = []
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => {
						let context: Ctx
						return {
							init: ctx => {
								context = ctx
								ctx.schedule(
									10,
									'stream-time',
									time => void calls.push([time, context.recordMetadata()])
								)
							},
							process() {},
						}
					},
				}),
		})
		const task = await h.start(h.task())
		expect(task.streamTimePunctuationDue()).toBe(false)
		expect(await task.maybePunctuateStreamTime()).toBe(false)
		h.enqueue(task, 'input', records(5, 8, 27))
		await task.processOne(h.clock.now())
		await h.commit()
		expect(task.needsCommit(() => undefined)).toBe(false)
		expect(await task.maybePunctuateStreamTime()).toBe(true)
		expect(task.needsCommit(() => undefined)).toBe(true)
		await task.processOne(h.clock.now())
		expect(await task.maybePunctuateStreamTime()).toBe(false)
		await task.processOne(h.clock.now())
		expect(await task.maybePunctuateStreamTime()).toBe(true)
		expect(calls).toEqual([
			[5, null],
			[27, null],
		])
	})

	it('a task without records punctuates once its seeded stream time is known (KF13)', async () => {
		const calls: number[] = []
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => ({
						init: ctx => void ctx.schedule(10, 'stream-time', time => void calls.push(time)),
						process() {},
					}),
				}),
		})
		const task = h.task()
		await task.initialize()
		task.setStarts(new Map([['input:0', committed({}, 70)]]))
		await task.completeRestoration(new Map())
		expect(task.hasWork(h.clock.now(), h.lag)).toBe(true)
		expect(await task.maybePunctuateStreamTime()).toBe(true)
		expect(calls).toEqual([70])
	})

	it('wall clock: due one interval after scheduling, called with the wall clock', async () => {
		const calls: number[] = []
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => {
						let context: Ctx
						return {
							init: ctx => {
								context = ctx
								ctx.schedule(100, 'wall-clock', time => {
									calls.push(time)
									expect(context.currentSystemTimeMs()).toBe(time)
								})
							},
							process() {},
						}
					},
				}),
		})
		const task = await h.start(h.task())
		const start = h.clock.now()
		expect(await task.maybePunctuateSystemTime(start + 99)).toBe(false)
		expect(await task.maybePunctuateSystemTime(start + 150)).toBe(true)
		expect(task.nextWallClockDue()).toBe(start + 200)
		expect(await task.maybePunctuateSystemTime(start + 450)).toBe(true)
		expect(calls).toEqual([start + 150, start + 450])
		expect(task.nextWallClockDue()).toBe(start + 500)
	})
})

describe('commit bookkeeping (4.8.3)', () => {
	it('commits the head of the queue while records are queued, else the next delivery offset', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await h.start(h.task())
		const next = h.consumer.nextOffset.bind(h.consumer)
		expect(task.needsCommit(next)).toBe(false)
		expect(task.committableOffsets(next)).toEqual([])
		h.enqueue(task, 'input', records(1, 2, 3))
		await task.processOne(h.clock.now())
		expect(task.committableOffsets(next)).toMatchObject([{ topic: 'input', partition: 0, offset: 1n }])
		await h.process(task)
		h.consumer.partition('input:0').position = 5n
		const [offset] = task.committableOffsets(next)
		expect(offset!.offset).toBe(5n)
		expect(decodeCommitMetadata(offset!.metadata)).toEqual({ partitionTime: 3, metadata: new Map() })
	})

	it('commits only the consumed partitions, unless the processor metadata changed', async () => {
		const clocked: ProcessorSupplier<string, string, string, string> = () => {
			let clock: ObservedStreamTime
			return {
				init: ctx =>
					void (clock = new ObservedStreamTime(ctx as InternalProcessorContext<string, string>, 'w')),
				process: record => void clock.advance(record.timestamp),
			}
		}
		for (const [name, supplier, partitions] of [
			['unchanged', passThrough, ['a']],
			['changed', clocked, ['a', 'b']],
		] as const) {
			const h = new TaskHarness({ build: twoTopics(supplier) })
			const task = await h.start(h.task())
			h.enqueue(task, 'a', records(1))
			h.enqueue(task, 'b', records(2))
			await task.processOne(h.clock.now())
			const offsets = task.committableOffsets(tp => h.consumer.nextOffset(tp))
			expect(
				offsets.map(o => o.topic),
				name
			).toEqual(partitions)
			if (name === 'changed') {
				expect(offsets.map(o => o.offset)).toEqual([1n, 0n])
				expect(decodeCommitMetadata(offsets[1]!.metadata).metadata).toEqual(new Map([['w', 1]]))
			}
		}
	})

	it('an empty partition whose fetch position moved past the last commit needs a commit (markers, KIP-1094)', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = h.task()
		await task.initialize()
		task.setStarts(new Map([['input:0', { committed: { offset: 5n, metadata: null }, resolved: 5n }]]))
		await task.completeRestoration(new Map())
		const next = h.consumer.nextOffset.bind(h.consumer)
		h.consumer.partition('input:0').position = 5n
		expect(task.needsCommit(next)).toBe(false)
		h.consumer.partition('input:0').position = 7n
		expect(task.needsCommit(next)).toBe(true)
		expect(task.committableOffsets(next)).toMatchObject([{ offset: 7n }])
		await task.postCommit(new Map(), task.committableOffsets(next))
		expect(task.lastCommitted({ topic: 'input', partition: 0 })).toBe(7n)
		expect(task.needsCommit(next)).toBe(false)
	})

	it('postCommit clears the commit flags, records the committed offsets of its partitions, then checkpoints', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await h.start(h.task())
		h.enqueue(task, 'input', records(1))
		await h.process(task)
		task.requestCommit()
		expect(task.commitRequested).toBe(true)
		expect(h.commitRequests).toBe(1)
		const offsets = task.committableOffsets(tp => h.consumer.nextOffset(tp))
		const other = { topic: 'other', partition: 0, offset: 9n, metadata: '' }
		await task.postCommit(new Map([['app-s-changelog:0', 4n]]), [...offsets, other])
		expect(task.commitRequested).toBe(false)
		expect(task.needsCommit(tp => h.consumer.nextOffset(tp))).toBe(false)
		expect(task.lastCommitted({ topic: 'input', partition: 0 })).toBe(1n)
		expect(task.lastCommitted({ topic: 'other', partition: 0 })).toBeUndefined()
		expect(h.state(task).checkpoints).toEqual([{ positions: new Map([['app-s-changelog:0', 4n]]), eos: false }])
	})

	it('context.commit() requests a commit', async () => {
		const { state, supplier } = recorder()
		const h = new TaskHarness({ build: b => pipe(b, { processor: supplier }) })
		const task = await h.start(h.task())
		state.context!.commit()
		expect(task.commitRequested).toBe(true)
		expect(h.commitRequests).toBe(1)
	})

	it('last committed offsets come from the assignment, else from the settled start positions (CR13)', async () => {
		const h = new TaskHarness({ build: twoTopics(passThrough) })
		const task = h.task()
		await task.initialize()
		task.setStarts(
			new Map([
				['a:0', { committed: { offset: 4n, metadata: null }, resolved: 4n }],
				['b:0', { committed: null, resolved: 6n }],
			])
		)
		expect(task.lastCommitted({ topic: 'b', partition: 0 })).toBeUndefined()
		task.setSettledPositions(
			new Map([
				['a:0', 99n],
				['b:0', 7n],
			])
		)
		expect(task.lastCommitted({ topic: 'a', partition: 0 })).toBe(4n)
		expect(task.lastCommitted({ topic: 'b', partition: 0 })).toBe(7n)
		expect(task.starts.get('b:0')).toEqual({ committed: null, resolved: 6n })
	})

	it('restartStarts() after two commits carries the second commit (offset and metadata)', async () => {
		const clocked: ProcessorSupplier<string, string, string, string> = () => {
			let clock: ObservedStreamTime
			return {
				init: ctx =>
					void (clock = new ObservedStreamTime(ctx as InternalProcessorContext<string, string>, 'w')),
				process: record => void clock.advance(record.timestamp),
			}
		}
		const h = new TaskHarness({ build: twoTopics(clocked) })
		const task = h.task()
		await h.start(
			task,
			new Map([
				['a:0', { committed: { offset: 0n, metadata: null }, resolved: 0n }],
				['b:0', { committed: null, resolved: 3n }],
			])
		)
		h.consumer.partition('b:0').position = 3n
		for (const timestamp of [10, 20]) {
			h.enqueue(task, 'a', records(timestamp))
			await h.process(task)
			expect(await h.commit()).toBe('committed')
		}
		const starts = task.restartStarts()
		const second = h.consumer.commits[1]!
		expect(starts.get('a:0')).toEqual({
			committed: { offset: 2n, metadata: second.find(o => o.topic === 'a')!.metadata },
			resolved: 2n,
		})
		expect(decodeCommitMetadata(starts.get('a:0')!.committed!.metadata).metadata.get('w')).toBe(20)
		expect(starts.get('b:0')).toEqual({
			committed: { offset: 3n, metadata: second.find(o => o.topic === 'b')!.metadata },
			resolved: 3n,
		})
	})
})
