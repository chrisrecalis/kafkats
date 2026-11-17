import { afterEach, describe, expect, it, vi } from 'vitest'
import { FailedProcessingError } from '@/api/errors.js'
import { string } from '@/codec.js'
import type { MaybePromise, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { encodeCommitMetadata, ProcessorMetadataImpl } from '@/processor/metadata.js'
import type { StreamRecord } from '@/processor/record.js'
import type { CommitReason } from '@/runtime/commit.js'
import type { FailureSite } from '@/runtime/errors.js'
import type { GroupConsumerHooks } from '@/runtime/io.js'
import { Scheduler } from '@/runtime/scheduler.js'
import { tpKey } from '@/runtime/task-id.js'
import type { PartitionStart, StreamTask } from '@/runtime/task.js'
import type { KeyValueStore } from '@/state.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { deferred, type Deferred, type TestRecord } from '../support/fake-io.js'
import { kvStore, pipe, TaskHarness, type TaskHarnessOptions } from '../support/task-harness.js'

type Ctx = ProcessorContext<string, string>
type Body = (record: StreamRecord<string, string>, context: Ctx) => MaybePromise<void>

/** Runs `body` for each record, then forwards it; `init` once per processor instance. */
function processor(body: Body, init?: (context: Ctx) => void): ProcessorSupplier<string, string, string, string> {
	return () => {
		let context: Ctx
		return {
			init: ctx => {
				context = ctx
				init?.(ctx)
			},
			process: record => {
				const result = body(record, context)
				return result instanceof Promise ? result.then(() => context.forward(record)) : context.forward(record)
			},
		}
	}
}

/** Topics `a` and `b` into one processor: tasks with two input partitions. */
function twoTopics(supplier: ProcessorSupplier<string, string, string, string>) {
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
		b.addProcessor({
			name: 'processor',
			parents: ['src-a', 'src-b'],
			stores: [],
			supplier: supplier as ProcessorSupplier<unknown, unknown, unknown, unknown>,
		})
	}
}

const values = (...list: string[]): TestRecord[] => list.map(value => ({ key: 'k', value, timestamp: 1 }))

/** Lets the scheduler run until it waits for what the test controls: the clock, a gate, a hung call. */
async function settle(): Promise<void> {
	for (let i = 0; i < 20; i++) {
		await new Promise(resolve => setImmediate(resolve))
	}
}

const fixtures: Fixture[] = []

afterEach(async () => {
	for (const fixture of fixtures.splice(0)) {
		await fixture.scheduler.stop(AbortSignal.timeout(100))
	}
})

/** A Scheduler over the task harness (real tasks, collector, commits; fake consumer and producer; manual clock). */
class Fixture {
	readonly h: TaskHarness
	readonly scheduler: Scheduler
	readonly failures: { error: unknown; site: FailureSite }[] = []
	/** Events the test records, in order. */
	readonly events: string[] = []
	paused = false
	/** The runtime's reaction to a failure (FlowRuntime.fail): by default the run fails. */
	onFailure: (error: unknown, site: FailureSite) => void = () => {
		this.h.failed = true
	}

	constructor(options: TaskHarnessOptions & { commitIntervalMs?: number }) {
		this.h = new TaskHarness(options)
		this.scheduler = new Scheduler({
			tasks: { tasks: () => this.h.tasks },
			commits: this.h.commits,
			collector: () => this.h.collector,
			consumer: this.h.consumer,
			clock: this.h.clock,
			config: {
				eos: this.h.eos,
				commitIntervalMs: options.commitIntervalMs ?? 60_000,
				transactionTimeoutMs: 10_000,
			},
			paused: () => this.paused,
			failed: () => this.h.failed,
			hardStop: this.h.hardStop.signal,
			onFailure: (error, site) => {
				this.failures.push({ error, site })
				this.onFailure(error, site)
			},
			logger: this.h.logger,
		})
		fixtures.push(this)
	}

	/** RUNNING tasks `0_0` .. `0_<count - 1>`. */
	async tasks(count = 1, starts?: ReadonlyMap<string, PartitionStart>): Promise<StreamTask[]> {
		const tasks: StreamTask[] = []
		for (let partition = 0; partition < count; partition++) {
			tasks.push(await this.h.start(this.h.task(0, partition), starts))
		}
		return tasks
	}

	/** Starts the scheduler and lets its first round run. */
	async start(): Promise<void> {
		this.scheduler.start()
		await settle()
	}

	/** Records delivered to the task (the consumer's handler, 4.5.1). */
	deliver(task: StreamTask, records: readonly TestRecord[], topic = 'input'): void {
		this.h.enqueue(task, topic, records)
		this.scheduler.markWork(task)
	}

	/** Every commit the scheduler asks for, as an event, still committing. */
	recordCommits(): void {
		const commit = this.h.commits.commit.bind(this.h.commits)
		vi.spyOn(this.h.commits, 'commit').mockImplementation((reason: CommitReason, signal: AbortSignal) => {
			this.events.push(`commit ${reason}`)
			return commit(reason, signal)
		})
	}

	commitCount(): number {
		return this.events.filter(event => event.startsWith('commit')).length
	}

	/** Moves the clock in steps, letting the scheduler run after each. */
	async advance(ms: number, step = ms): Promise<void> {
		for (let moved = 0; moved < ms; moved += step) {
			this.h.clock.advance(Math.min(step, ms - moved))
			await settle()
		}
	}

	/** Resolves `ms` later on the manual clock. */
	sleep(ms: number): Promise<void> {
		return new Promise(resolve => this.h.clock.setTimer(this.h.clock.now() + ms, resolve))
	}
}

/** One gate per record value: the record's processing waits until the test opens its gate. */
function gates(): { gate(value: string): Deferred<void>; wait(value: string): Promise<void> } {
	const all = new Map<string, Deferred<void>>()
	const gate = (value: string) => {
		let d = all.get(value)
		if (!d) {
			d = deferred()
			all.set(value, d)
		}
		return d
	}
	return { gate, wait: value => gate(value).promise }
}

describe('Scheduler: slices (4.5.5, 4.13)', () => {
	it('slices overlap: a task awaiting 1 s per record does not hold up one processing 1500 records between commits (SI5)', async () => {
		const processed = [0, 0]
		const active = [0, 0]
		const maxActive = [0, 0]
		let f!: Fixture
		const body: Body = (_record, context) => {
			const p = context.taskId.partition
			active[p]!++
			maxActive[p] = Math.max(maxActive[p]!, active[p]!)
			const done = () => {
				active[p]!--
				processed[p]!++
			}
			if (p === 1) return done()
			return f.sleep(1000).then(done)
		}
		f = new Fixture({ partitions: 2, commitIntervalMs: 5000, build: b => pipe(b, { processor: processor(body) }) })
		const [slow, fast] = await f.tasks(2)
		f.deliver(slow!, values('s1', 's2', 's3'))
		f.deliver(fast!, values(...Array.from({ length: 1500 }, (_, i) => `f${i}`)))
		await f.start()

		expect(processed).toEqual([0, 1500])
		expect(active[0]).toBe(1)
		expect(f.h.consumer.commits).toEqual([])

		await f.advance(5000, 500)
		expect(processed).toEqual([3, 1500])
		expect(maxActive).toEqual([1, 1])
		const committed = f.h.consumer.commits.flat().map(o => `${tpKey(o)}@${o.offset}`)
		expect(committed).toEqual(['input:0@3', 'input:1@1500'])
	})

	it('runs the stream-time punctuation of a task without records once it is RUNNING (KF13)', async () => {
		const punctuations: number[] = []
		let records = 0
		const f = new Fixture({
			build: b =>
				pipe(b, {
					processor: processor(
						() => void records++,
						ctx => ctx.schedule(10, 'stream-time', time => void punctuations.push(time))
					),
				}),
		})
		const metadata = encodeCommitMetadata(500, new ProcessorMetadataImpl())
		const starts = new Map([['input:0', { committed: { offset: 7n, metadata }, resolved: 7n }]])
		const [task] = await f.tasks(1, starts)
		f.scheduler.markWork(task!)
		await f.start()

		expect(punctuations).toEqual([500])
		expect(records).toBe(0)
	})

	it('runs a wall-clock punctuation at its due time without records (timer)', async () => {
		const punctuations: number[] = []
		const f = new Fixture({
			build: b =>
				pipe(b, {
					processor: processor(
						() => {},
						ctx => ctx.schedule(100, 'wall-clock', time => void punctuations.push(time))
					),
				}),
		})
		await f.tasks(1)
		await f.start()
		await f.advance(99)
		expect(punctuations).toEqual([])
		await f.advance(1)
		expect(punctuations).toEqual([1100])
		await f.advance(100)
		expect(punctuations).toEqual([1100, 1200])
	})

	it('resumes a partition paused for backpressure after the slice that drained it to the limit', async () => {
		let f!: Fixture
		const body: Body = (record, context) => {
			f.events.push(`process ${context.recordMetadata()!.offset}`)
			// 5 ms per record: a slice processes two records.
			f.h.clock.advance(5)
		}
		f = new Fixture({ bufferedRecordsPerPartition: 3, build: b => pipe(b, { processor: processor(body) }) })
		const resume = f.h.consumer.resume.bind(f.h.consumer)
		vi.spyOn(f.h.consumer, 'resume').mockImplementation(tps => {
			f.events.push(`resume ${tps.map(tpKey).join(',')}`)
			resume(tps)
		})
		const [task] = await f.tasks(1)
		f.deliver(task!, values('1', '2', '3', '4', '5', '6'))
		await f.start()

		expect(f.events).toEqual([
			'process 0',
			'process 1',
			'process 2',
			'process 3',
			'resume input:0',
			'process 4',
			'process 5',
		])
	})

	it('yields to the event loop while processing many records synchronously (SI15)', async () => {
		let processed = 0
		let f!: Fixture
		const body: Body = () => {
			// At least 20 µs per record, and 1 ms on the manual clock: slices of 10 records.
			const until = performance.now() + 0.02
			while (performance.now() < until) {
				/* busy */
			}
			f.h.clock.advance(1)
			processed++
		}
		f = new Fixture({ build: b => pipe(b, { processor: processor(body) }) })
		const [task] = await f.tasks(1)
		f.deliver(task!, values(...Array.from({ length: 2000 }, (_, i) => `${i}`)))
		const seen: number[] = []
		const turn = () => {
			seen.push(processed)
			if (processed < 2000) setImmediate(turn)
		}
		setImmediate(turn)
		await f.start()
		await vi.waitFor(() => expect(processed).toBe(2000), { timeout: 10_000, interval: 5 })

		const gaps = seen.slice(1).map((count, i) => count - seen[i]!)
		expect(seen.length).toBeGreaterThan(2)
		expect(Math.max(...gaps)).toBeLessThan(1000)
	})

	it('re-checks a task that waits for a fetch: the client tells nothing when an empty partition was fetched', async () => {
		let processed = 0
		const f = new Fixture({ build: twoTopics(processor(() => void processed++)) })
		const [task] = await f.tasks(1)
		f.deliver(task!, values('x'), 'a')
		await f.start()
		expect(processed).toBe(0)

		// The first fetch of b returned nothing: its lag is known (0) now, without any delivery.
		f.h.consumer.partition('b:0').end = 0n
		await f.advance(99)
		expect(processed).toBe(0)
		await f.advance(1)
		expect(processed).toBe(1)
	})

	it('wakes an idling task at its idle deadline (KIP-695 enforced processing)', async () => {
		let processed = 0
		const f = new Fixture({ maxTaskIdleMs: 50, build: twoTopics(processor(() => void processed++)) })
		const [task] = await f.tasks(1)
		f.h.consumer.partition('b:0').end = 0n
		f.deliver(task!, values('x'), 'a')
		await f.start()
		await f.advance(49)
		expect(processed).toBe(0)
		await f.advance(1)
		expect(processed).toBe(1)
	})

	it('a slice stops after the current record of a task lost meanwhile, without its punctuations (W5)', async () => {
		const { gate, wait } = gates()
		let records = 0
		let punctuations = 0
		const f = new Fixture({
			build: b =>
				pipe(b, {
					processor: processor(
						record => wait(record.value!).then(() => void records++),
						ctx => ctx.schedule(1, 'stream-time', () => void punctuations++)
					),
				}),
		})
		const [task] = await f.tasks(1)
		f.deliver(task!, values('1', '2'))
		await f.start()
		task!.markLost()
		gate('1').resolve()
		gate('2').resolve()
		await settle()

		expect(records).toBe(1)
		expect(punctuations).toBe(0)
	})

	it('starts no slice while processing is paused; commits go on', async () => {
		let processed = 0
		const f = new Fixture({
			commitIntervalMs: 100,
			build: b => pipe(b, { processor: processor(() => void processed++) }),
		})
		f.recordCommits()
		const [task] = await f.tasks(1)
		f.paused = true
		f.deliver(task!, values('x'))
		await f.start()
		await f.advance(100)
		expect(processed).toBe(0)
		expect(f.events).toEqual(['commit interval'])

		f.paused = false
		f.scheduler.wake()
		await settle()
		expect(processed).toBe(1)
	})
})

describe('Scheduler: commits and exclusive ops are barriers (4.13)', () => {
	it('a commit waits for every slice in flight, its punctuation included; no slice starts meanwhile', async () => {
		const punctuated = deferred()
		let f!: Fixture
		const body: Body = record => void f.events.push(`process ${record.value}`)
		f = new Fixture({
			partitions: 2,
			commitIntervalMs: 100,
			build: b =>
				pipe(b, {
					processor: processor(body, ctx => {
						if (ctx.taskId.partition === 0) {
							ctx.schedule(60, 'wall-clock', async () => {
								f.events.push('punctuating')
								await punctuated.promise
								f.events.push('punctuated')
							})
						}
					}),
				}),
		})
		f.recordCommits()
		const [, other] = await f.tasks(2)
		await f.start()
		// The punctuation starts at 1060; the commit is due at 1100.
		await f.advance(60)
		await f.advance(40)
		f.deliver(other!, values('b1'))
		await settle()
		expect(f.events).toEqual(['punctuating'])

		punctuated.resolve()
		await settle()
		expect(f.events).toEqual(['punctuating', 'punctuated', 'commit interval', 'process b1'])
	})

	it('A throws while B is in a store write: the recovery closes dirty after B wrote (CR4)', async () => {
		const { gate, wait } = gates()
		let f!: Fixture
		const writer: ProcessorSupplier<string, string, string, string> = () => {
			let store: KeyValueStore<string, string>
			let context: Ctx
			return {
				init: ctx => {
					context = ctx
					store = ctx.getStateStore('s')
				},
				process: async record => {
					if (context.taskId.partition === 0) throw new Error('A failed')
					f.events.push(`B writing ${record.value}`)
					await wait(record.value!)
					await store.put(record.key!, record.value!)
					f.events.push(`B wrote ${record.value}`)
				},
			}
		}
		f = new Fixture({ partitions: 2, build: b => pipe(b, { processor: writer, stores: [kvStore('s')] }) })
		f.onFailure = () => {
			f.h.failed = true
			void f.scheduler.exclusive(async () => {
				f.events.push('closeDirty')
				for (const task of f.h.tasks) await task.close('dirty')
			})
		}
		const [a, b] = await f.tasks(2)
		await f.start()
		f.deliver(b!, values('b1', 'b2'))
		await settle()
		f.deliver(a!, values('a1'))
		await settle()
		expect(f.failures.map(({ site }) => site)).toEqual(['processing'])
		expect(f.failures[0]!.error).toBeInstanceOf(FailedProcessingError)
		expect(f.events).toEqual(['B writing b1'])

		gate('b1').resolve()
		gate('b2').resolve()
		await settle()
		expect(f.events).toEqual(['B writing b1', 'B wrote b1', 'closeDirty'])
	})

	it('exclusive ops wait for the slices in flight and run in FIFO order; no slice starts until they settled', async () => {
		const { gate, wait } = gates()
		let f!: Fixture
		const body: Body = async record => {
			if (record.value === 'a1') await wait('a1')
			f.events.push(`process ${record.value}`)
		}
		f = new Fixture({ partitions: 2, build: b => pipe(b, { processor: processor(body) }) })
		const [a, b] = await f.tasks(2)
		await f.start()
		f.deliver(a!, values('a1'))
		await settle()
		const op = (name: string, work: () => Promise<void> = async () => {}) =>
			f.scheduler.exclusive(async () => {
				f.events.push(name)
				await work()
				return name
			})
		const first = op('op1', () => wait('op1'))
		const second = op('op2', () => Promise.reject(new Error('op2 failed')))
		const third = op('op3')
		f.deliver(b!, values('b1'))
		await settle()
		expect(f.events).toEqual([])

		gate('a1').resolve()
		await settle()
		expect(f.events).toEqual(['process a1', 'op1'])

		gate('op1').resolve()
		await expect(first).resolves.toBe('op1')
		await expect(second).rejects.toThrow('op2 failed')
		await expect(third).resolves.toBe('op3')
		await settle()
		expect(f.events).toEqual(['process a1', 'op1', 'op2', 'op3', 'process b1'])
		expect(f.failures).toEqual([])
	})

	it('nothing starts once the run failed: no record, punctuation or commit; exclusive ops still run (fail-stop)', async () => {
		let f!: Fixture
		const body: Body = record => {
			if (record.value === 'boom') throw new Error('boom')
			f.events.push(`process ${record.value}`)
		}
		f = new Fixture({
			partitions: 2,
			commitIntervalMs: 100,
			build: b =>
				pipe(b, {
					processor: processor(body, ctx =>
						ctx.schedule(10, 'wall-clock', () => void f.events.push('punctuated'))
					),
				}),
		})
		f.recordCommits()
		const [a, b] = await f.tasks(2)
		await f.start()
		f.deliver(a!, values('boom'))
		await settle()
		expect(f.failures.map(({ site }) => site)).toEqual(['processing'])

		f.deliver(b!, values('b1'))
		await f.advance(200, 10)
		expect(f.events).toEqual([])
		await expect(f.scheduler.exclusive(async () => 'ran')).resolves.toBe('ran')
	})

	it('commits at the interval and on request; a failed commit is reported at the commit site', async () => {
		const f = new Fixture({ commitIntervalMs: 1000, build: b => pipe(b, { processor: processor(() => {}) }) })
		f.recordCommits()
		const [task] = await f.tasks(1)
		await f.start()
		await f.advance(999)
		expect(f.events).toEqual([])
		await f.advance(1)
		expect(f.events).toEqual(['commit interval'])

		f.scheduler.requestCommit()
		await settle()
		expect(f.events).toEqual(['commit interval', 'commit user'])

		f.deliver(task!, values('x'))
		await settle()
		f.h.consumer.steps.commit.next(new Error('coordinator gone'))
		f.scheduler.requestCommit()
		await settle()
		expect(f.failures.map(({ site, error }) => `${site}: ${(error as Error).message}`)).toEqual([
			'commit: coordinator gone',
		])
	})

	it('retries a commit skipped for a pending rebalance no sooner than 100 ms, and at once when it completed (CR12)', async () => {
		const f = new Fixture({ commitIntervalMs: 5000, build: b => pipe(b, { processor: processor(() => {}) }) })
		f.recordCommits()
		const hooks: GroupConsumerHooks = {
			onPartitionsLost: async () => {},
			onPartitionsRevoked: async () => {},
			onPartitionsAssigned: async () => {},
		}
		void f.h.consumer.run(['input'], () => {}, hooks)
		f.h.consumer.requestRebalance()
		await f.start()
		await f.advance(5000)
		expect(f.commitCount()).toBe(1)
		await settle()
		await f.advance(99)
		expect(f.commitCount()).toBe(1)
		await f.advance(1)
		expect(f.commitCount()).toBe(2)

		// A user request waits for the retry time too.
		f.scheduler.requestCommit()
		await settle()
		expect(f.commitCount()).toBe(2)

		await f.h.consumer.rebalance()
		await settle()
		expect(f.commitCount()).toBe(3)
		expect(f.h.commits.lastCommitAt).toBe(f.h.clock.now())
		await f.advance(4999, 1000)
		expect(f.commitCount()).toBe(3)
	})

	it('an op queued after stop waits for the commit in progress', async () => {
		const f = new Fixture({ commitIntervalMs: 100, build: b => pipe(b, { processor: processor(() => {}) }) })
		const [task] = await f.tasks(1)
		await f.start()
		f.deliver(task!, values('x'))
		await settle()
		f.h.consumer.steps.commit.next('hang')
		await f.advance(100)
		expect(f.h.consumer.steps.commit.hung).toHaveLength(1)

		await f.scheduler.stop(AbortSignal.timeout(1000))
		let ran = false
		const op = f.scheduler.exclusive(async () => {
			ran = true
		})
		await settle()
		expect(ran).toBe(false)

		f.h.consumer.steps.commit.answerAll()
		await op
		expect(f.h.consumer.commits.flat().map(o => o.offset)).toEqual([1n])
	})
})

describe('Scheduler: stop', () => {
	it('stops starting slices; the slice in flight finishes its current record; stop resolves once it settled', async () => {
		const { gate, wait } = gates()
		const processed: string[] = []
		const f = new Fixture({
			build: b =>
				pipe(b, {
					processor: processor(record => wait(record.value!).then(() => void processed.push(record.value!))),
				}),
		})
		const [task] = await f.tasks(1)
		f.deliver(task!, values('1', '2'))
		await f.start()
		let stopped = false
		const stopping = f.scheduler.stop(AbortSignal.timeout(10_000)).then(() => {
			stopped = true
		})
		gate('2').resolve()
		await settle()
		expect(stopped).toBe(false)

		gate('1').resolve()
		await stopping
		f.deliver(task!, values('3'))
		gate('3').resolve()
		await settle()
		expect(processed).toEqual(['1'])
	})

	it('resolves when its signal aborts while a slice hangs', async () => {
		const f = new Fixture({ build: b => pipe(b, { processor: processor(() => new Promise<void>(() => {})) }) })
		const [task] = await f.tasks(1)
		f.deliver(task!, values('x'))
		await f.start()
		const controller = new AbortController()
		const stopping = f.scheduler.stop(controller.signal)
		controller.abort()
		await expect(stopping).resolves.toBeUndefined()
	})
})

describe('Scheduler: exactly-once', () => {
	it('begins one transaction before the first slice and none while no task has work (SI13)', async () => {
		let processed = 0
		const f = new Fixture({
			eos: true,
			partitions: 2,
			commitIntervalMs: 100,
			build: b => pipe(b, { processor: processor(() => void processed++) }),
		})
		const [a, b] = await f.tasks(2)
		// The harness began one for the processors' init().
		expect(f.h.producer.transactions).toHaveLength(1)
		await f.start()
		f.deliver(a!, values('a1'))
		f.deliver(b!, values('b1'))
		await settle()
		f.deliver(a!, values('a2'))
		await settle()
		expect(processed).toBe(3)
		expect(f.h.producer.transactions).toHaveLength(1)

		await f.advance(100)
		expect(f.h.producer.transactions.map(t => t.state)).toEqual(['committed'])
		await f.advance(300, 100)
		expect(f.h.producer.transactions).toHaveLength(1)

		f.deliver(a!, values('a3'))
		f.deliver(b!, values('b2'))
		await settle()
		expect(f.h.producer.transactions.map(t => t.state)).toEqual(['committed', 'open'])
		expect(f.h.producer.transactions[1]!.sent.map(r => r.message.value?.toString())).toEqual(['a3', 'b2'])
	})

	it('a transaction that cannot begin is reported at the commit site and no slice starts', async () => {
		let processed = 0
		const f = new Fixture({
			eos: true,
			commitIntervalMs: 100,
			build: b => pipe(b, { processor: processor(() => void processed++) }),
		})
		const [task] = await f.tasks(1)
		await f.start()
		await f.advance(100)
		f.h.producer.steps.beginTransaction.next(new Error('coordinator not available'))
		f.deliver(task!, values('x'))
		await settle()
		expect(f.failures.map(({ site }) => site)).toEqual(['commit'])
		expect(processed).toBe(0)
	})
})
