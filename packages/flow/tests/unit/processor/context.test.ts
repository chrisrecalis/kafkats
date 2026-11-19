import { describe, expect, it, vi } from 'vitest'
import type { Logger } from '@kafkats/client'
import { FailedProcessingError, IllegalStateError, StreamsError, TopologyError } from '@/api/errors.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { InternalProcessorContext, Processor, ProcessorContext } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { inMemory } from '@/state/memory.js'
import type { KeyValueStore } from '@/state.js'
import { string } from '@/codec.js'
import { capture, ProcessorHarness, processorDef, sourceDef } from '../support/processor-harness.js'

type Ctx = ProcessorContext<unknown, unknown>

/** A processor whose process() is `body` with its context. */
function using(body: (ctx: Ctx, record: StreamRecord<unknown, unknown>) => unknown, init?: (ctx: Ctx) => void) {
	return (): Processor<unknown, unknown, unknown, unknown> => {
		let ctx: Ctx
		return {
			init: context => {
				ctx = context
				init?.(context)
			},
			process: record => body(ctx, record) as void | Promise<void>,
		}
	}
}

function logging(log: string[], name: string, forward = true) {
	return using((ctx, record) => {
		log.push(name)
		return forward ? ctx.forward(record) : undefined
	})
}

function deferred() {
	let resolve!: () => void
	let reject!: (error: unknown) => void
	const promise = new Promise<void>((res, rej) => {
		resolve = res
		reject = rej
	})
	return { promise, resolve, reject }
}

const asyncChild = (gate: Promise<void>) => processorDef('child', ['a'], () => ({ process: () => gate }))

describe('forward', () => {
	it('goes depth-first to the children in connection order', async () => {
		const log: string[] = []
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('a', ['src'], logging(log, 'a')),
			processorDef('b', ['a'], logging(log, 'b')),
			processorDef('c', ['a'], logging(log, 'c')),
			processorDef('d', ['b'], logging(log, 'd')),
		])
		await harness.process('in', { key: 'k', value: 1, timestamp: 0 })
		expect(log).toEqual(['a', 'b', 'd', 'c'])
	})

	it('goes depth-first through asynchronous children too', async () => {
		const log: string[] = []
		const later = (name: string) =>
			using(async (ctx, record) => {
				await Promise.resolve()
				log.push(name)
				return ctx.forward(record)
			})
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('a', ['src'], logging(log, 'a')),
			processorDef('b', ['a'], later('b')),
			processorDef('c', ['a'], logging(log, 'c')),
			processorDef('d', ['b'], later('d')),
		])
		await harness.process('in', { key: 'k', value: 1, timestamp: 0 })
		expect(log).toEqual(['a', 'b', 'd', 'c'])
	})

	it('with a child name reaches only that child', async () => {
		const b = capture('b', ['a'])
		const c = capture('c', ['a'])
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using((ctx, record) => ctx.forward(record, 'c'))
			),
			b.def,
			c.def,
		])
		await harness.process('in', { key: 'k', value: 1, timestamp: 0 })
		expect(b.records).toHaveLength(0)
		expect(c.records).toHaveLength(1)
	})

	it('to an unknown child is a StreamsError for the processing handler', async () => {
		const defs = [
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using((ctx, r) => ctx.forward(r, 'nope'))
			),
		]
		const failing = await ProcessorHarness.start(defs)
		const error = await failing.process('in', { key: 'k', value: 1, timestamp: 0 }).catch((e: unknown) => e)
		expect(error).toBeInstanceOf(FailedProcessingError)
		expect((error as Error).cause).toBeInstanceOf(StreamsError)
		const continuing = await ProcessorHarness.start(defs, { processing: () => 'continue' })
		await continuing.process('in', { key: 'k', value: 1, timestamp: 0 })
		expect(continuing.task.droppedCount('processing')).toBe(1)
	})
})

describe('forward guards', () => {
	it('a second forward while the first is pending throws, whatever the handler says', async () => {
		const gate = deferred()
		const processing = vi.fn(() => 'continue' as const)
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using((ctx, record) => {
						void ctx.forward(record)
						return ctx.forward(record)
					})
				),
				asyncChild(gate.promise),
			],
			{ processing }
		)
		await expect(harness.process('in', { key: 'k', value: 1, timestamp: 0 })).rejects.toThrow(
			/previous forward is pending/
		)
		expect(processing).not.toHaveBeenCalled()
		gate.resolve()
	})

	it('a forward outside process(), from a timer or in init(), throws', async () => {
		let ctx!: Ctx
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using(
					() => undefined,
					context => (ctx = context)
				)
			),
		])
		await harness.process('in', { key: 'k', value: 1, timestamp: 0 })
		const fromTimer = await new Promise(resolve =>
			setTimeout(() => {
				try {
					resolve(ctx.forward(new StreamRecord('k', 1, 0)))
				} catch (error) {
					resolve(error)
				}
			})
		)
		expect(fromTimer).toBeInstanceOf(StreamsError)
		expect((fromTimer as Error).message).toMatch(/outside its process/)

		const initForward = ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('b', ['src'], () => ({
				init: context => context.forward(new StreamRecord('k', 1, 0)),
				process: () => {},
			})),
		])
		await expect(initForward).rejects.toMatchObject({
			message: "failed to initialize processor 'b'",
			cause: expect.objectContaining({ message: expect.stringMatching(/outside its process/) }),
		})
	})

	it('a process() that settles with a pending forward throws, and the late failure stays handled', async () => {
		const unhandled = vi.fn()
		process.on('unhandledRejection', unhandled)
		const gate = deferred()
		const processing = vi.fn(() => 'fail' as const)
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using(async (ctx, record) => {
						void ctx.forward(record)
					})
				),
				asyncChild(gate.promise),
			],
			{ processing }
		)
		try {
			await expect(harness.process('in', { key: 'k', value: 1, timestamp: 0 })).rejects.toThrow(
				/settled with a pending forward/
			)
			expect(processing).not.toHaveBeenCalled()
			gate.reject(new Error('late'))
			await new Promise(resolve => setTimeout(resolve, 10))
			expect(unhandled).not.toHaveBeenCalled()
		} finally {
			process.off('unhandledRejection', unhandled)
		}
	})

	it('a synchronous process() that drops an asynchronous forward throws', async () => {
		const gate = deferred()
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using((ctx, record) => void ctx.forward(record))
			),
			asyncChild(gate.promise),
		])
		await expect(harness.process('in', { key: 'k', value: 1, timestamp: 0 })).rejects.toThrow(
			/settled with a pending forward/
		)
		gate.resolve()
	})

	it('awaited and returned forwards are fine, one after the other', async () => {
		const sink = capture('sink', ['child'])
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using(async (ctx, record) => {
					await ctx.forward(record)
					return ctx.forward(record.withValue(2))
				})
			),
			processorDef('child', ['a'], () => {
				let ctx: Ctx
				return {
					init: context => void (ctx = context),
					process: async record => {
						await Promise.resolve()
						return ctx.forward(record)
					},
				}
			}),
			sink.def,
		])
		await harness.process('in', { key: 'k', value: 1, timestamp: 0 })
		expect(sink.records.map(r => r.value)).toEqual([1, 2])
	})
})

describe('record context', () => {
	it('follows forwards and is restored after the children returned', async () => {
		const seen: unknown[] = []
		const snapshot = (name: string, ctx: Ctx) => {
			const rc = (ctx as InternalProcessorContext<unknown, unknown>).recordContext()
			seen.push({ name, timestamp: rc?.timestamp, headers: rc?.headers, offset: rc?.offset, topic: rc?.topic })
		}
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				using(async (ctx, record) => {
					snapshot('a', ctx)
					await ctx.forward(record.withTimestamp(20).withHeaders({ h: Buffer.from('b') }))
					snapshot('a after', ctx)
				})
			),
			processorDef(
				'b',
				['a'],
				using(async ctx => {
					await Promise.resolve()
					snapshot('b', ctx)
					expect(ctx.recordMetadata()).toMatchObject({ topic: 'in', partition: 0, offset: 7n })
				})
			),
		])
		const headers = { h: Buffer.from('a') }
		await harness.process('in', { key: 'k', value: 1, timestamp: 10, headers, offset: 7n })
		expect(seen).toEqual([
			{ name: 'a', timestamp: 10, headers, offset: 7n, topic: 'in' },
			{ name: 'b', timestamp: 20, headers: { h: Buffer.from('b') }, offset: 7n, topic: 'in' },
			{ name: 'a after', timestamp: 10, headers, offset: 7n, topic: 'in' },
		])
	})

	it('is absent in init() and outside processing', async () => {
		let ctx!: InternalProcessorContext<unknown, unknown>
		const inInit: unknown[] = []
		await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('a', ['src'], () => ({
				init: context => {
					ctx = context as InternalProcessorContext<unknown, unknown>
					inInit.push(ctx.recordContext(), ctx.recordMetadata())
				},
				process: () => {},
			})),
		])
		expect(inInit).toEqual([null, null])
		expect(ctx.recordContext()).toBeNull()
	})
})

describe('context services', () => {
	const definition: StoreDefinition<KeyValueStore<string, string>> = { name: 'kv' }

	it('gives connected stores only, by name or definition', async () => {
		const store = inMemory().createKeyValueStore('kv', { keyCodec: string(), valueCodec: string() })
		let ctx!: Ctx
		let unconnected!: Ctx
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using(
						() => {},
						c => (ctx = c)
					),
					['kv']
				),
				processorDef(
					'b',
					['src'],
					using(
						() => {},
						c => (unconnected = c)
					)
				),
			],
			{ stores: [store] }
		)
		expect(ctx.getStateStore('kv')).toBe(store)
		const typed: KeyValueStore<string, string> = ctx.getStateStore(definition)
		expect(typed).toBe(store)
		expect(() => unconnected.getStateStore('kv')).toThrow(TopologyError)
		await harness.close()
	})

	it('reads the task clocks and requests commits', async () => {
		let ctx!: Ctx
		const times: number[][] = []
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using(
						c => void times.push([c.currentStreamTimeMs(), c.currentSystemTimeMs()]),
						c => (ctx = c)
					)
				),
			],
			{ wallClock: 1000 }
		)
		expect(ctx.currentStreamTimeMs()).toBe(-1)
		await harness.process('in', { key: 'k', value: 1, timestamp: 50 })
		await harness.process('in', { key: 'k', value: 1, timestamp: 30 })
		expect(times).toEqual([
			[50, 1000],
			[50, 1000],
		])
		ctx.commit()
		expect(harness.commitRequests).toBe(1)
		expect(ctx.applicationId).toBe('test-app')
		expect(ctx.taskId.key).toBe('0_0')
		expect(ctx.processorName).toBe('a')
	})

	it('a closed context throws IllegalStateError (CR9)', async () => {
		let ctx!: Ctx
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using(
						() => {},
						c => (ctx = c)
					),
					['kv']
				),
			],
			{ stores: [inMemory().createKeyValueStore('kv', { keyCodec: string(), valueCodec: string() })] }
		)
		await harness.close()
		expect(() => ctx.forward(new StreamRecord('k', 1, 0))).toThrow(IllegalStateError)
		expect(() => ctx.schedule(10, 'wall-clock', () => {})).toThrow(IllegalStateError)
		expect(() => ctx.commit()).toThrow(IllegalStateError)
		expect(() => ctx.getStateStore('kv')).toThrow(IllegalStateError)
		expect(harness.commitRequests).toBe(0)
	})

	it('logs dropped records at most once per second per reason, and counts every one', async () => {
		const warn = vi.fn()
		const logger = { warn, error: vi.fn(), info: vi.fn(), debug: vi.fn() } as unknown as Logger
		let ctx!: InternalProcessorContext<unknown, unknown>
		const harness = await ProcessorHarness.start(
			[
				sourceDef('src', ['in']),
				processorDef(
					'a',
					['src'],
					using(
						() => {},
						c => (ctx = c as InternalProcessorContext<unknown, unknown>)
					)
				),
			],
			{ logger }
		)
		ctx.recordDropped('late', { window: 1 })
		ctx.recordDropped('late')
		ctx.recordDropped('null-key')
		harness.wallClock += 999
		ctx.recordDropped('late')
		expect(warn).toHaveBeenCalledTimes(2)
		harness.wallClock += 1
		ctx.recordDropped('late')
		expect(warn).toHaveBeenCalledTimes(3)
		expect(warn.mock.calls.map(call => call[1])).toEqual([
			{ task: '0_0', processor: 'a', reason: 'late', dropped: 1, window: 1 },
			{ task: '0_0', processor: 'a', reason: 'null-key', dropped: 1 },
			{ task: '0_0', processor: 'a', reason: 'late', dropped: 3 },
		])
		expect(harness.task.droppedCount('late')).toBe(4)
		expect(harness.task.droppedCount('null-key')).toBe(1)
		expect(harness.task.droppedCount('expired-window')).toBe(0)
	})
})
