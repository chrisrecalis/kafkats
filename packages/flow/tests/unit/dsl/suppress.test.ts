import { describe, expect, it } from 'vitest'
import { FailedProcessingError, TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { SuppressOptions } from '@/api/options.js'
import { codec } from '@/codec.js'
import { DslContext } from '@/dsl/context.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { windowCloseGraceMs } from '@/dsl/ops/suppress.js'
import { SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import { bufferedChangeCodec } from '@/operators/suppress-entry.js'
import { WindowedCodecs, type WindowedCodec } from '@/operators/windowed.js'
import type { RecordHeaders } from '@/processor/record.js'
import type { KeyValueStore } from '@/state.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { APP, TaskHarness } from '../support/task-harness.js'
import { updatesOf } from '../support/table-updates.js'

const str = codec.string()
const num = codec.json<number>()
const S = { key: str, value: str }
const FINAL: SuppressOptions = { until: 'window-close' }
const limit = (timeLimit: number, options: Partial<SuppressOptions> = {}): SuppressOptions =>
	({ until: 'time-limit', timeLimit, ...options }) as SuppressOptions

type Final = [key: string, start: number, end: number, count: number, timestamp: number]

const send = (ctx: TestContextApi, topic: string, key: string, value: string | null, timestamp: number) =>
	ctx.send(topic, value === null ? null : Buffer.from(value), { key: Buffer.from(key), timestamp })

/** Final counts of the records of 'in' in `windows`, written to 'out'; returns the codec of their keys. */
function finalCounts(
	driver: TestDriver,
	windows: TimeWindows | SessionWindows,
	options: SuppressOptions = FINAL
): WindowedCodec<string> {
	const grouped = driver.input('in', S).groupByKey()
	const counts =
		windows instanceof SessionWindows ? grouped.windowedBy(windows).count() : grouped.windowedBy(windows).count()
	counts.suppress(options).toStream().to('out', { value: num })
	return windows instanceof SessionWindows
		? WindowedCodecs.sessionWindowed(str)
		: WindowedCodecs.timeWindowed(str, windows.sizeMs)
}

/** What was written to 'out' by finalCounts. */
function finals(ctx: TestContextApi, keys: WindowedCodec<string>): Final[] {
	return ctx.producer.messagesFor('out').map(({ key, value, timestamp }) => {
		const { key: base, window } = keys.decode(key!)
		return [base, window.start, window.end, num.decode(value!), Number(timestamp)]
	})
}

/** Values written to 'out' as `key=value@timestamp`. */
const written = (ctx: TestContextApi) =>
	ctx.producer
		.messagesFor('out')
		.map(({ key, value, timestamp }) => `${key?.toString()}=${value?.toString() ?? null}@${timestamp}`)

describe.each(GUARANTEES)('suppress until windows close (6.16; %s)', (_, config) => {
	it('forwards the final result of each time window once stream time passes its end plus grace', async () => {
		const driver = new TestDriver(config)
		const keys = finalCounts(driver, TimeWindows.ofSizeAndGrace(2, 1))
		await driver.run(async ctx => {
			for (const timestamp of [0, 1, 2, 1, 0]) await send(ctx, 'in', 'k', 'v', timestamp)
			expect(finals(ctx, keys)).toEqual([])
			await send(ctx, 'in', 'k', 'v', 5) // closes [0,2) and [2,4)
			await send(ctx, 'in', 'k', 'v', 0) // late for the aggregation: dropped
			expect(finals(ctx, keys)).toEqual([
				['k', 0, 2, 4, 1],
				['k', 2, 4, 1, 2],
			])
		})
	})

	it('closes a window exactly at its end plus grace, keys of one window end in key byte order', async () => {
		const driver = new TestDriver(config)
		const keys = finalCounts(driver, TimeWindows.ofSizeAndGrace(10, 5))
		await driver.run(async ctx => {
			await send(ctx, 'in', 'b', 'v', 3)
			await send(ctx, 'in', 'a', 'v', 4)
			await send(ctx, 'in', 'a', 'v', 14)
			expect(finals(ctx, keys)).toEqual([])
			await send(ctx, 'in', 'c', 'v', 15)
			expect(finals(ctx, keys)).toEqual([
				['a', 0, 10, 1, 4],
				['b', 0, 10, 1, 3],
			])
		})
	})

	it('waits the gap and the grace for a session, and forwards no session that was merged away', async () => {
		const driver = new TestDriver(config)
		const keys = finalCounts(driver, SessionWindows.ofInactivityGapAndGrace(5, 2))
		await driver.run(async ctx => {
			for (const timestamp of [0, 3, 7]) await send(ctx, 'in', 'k', 'v', timestamp) // [0,0] and [0,3] merge into [0,7]
			await send(ctx, 'in', 'k', 'v', 13) // a new session; [0,7] may still merge until 7 + 5 + 2
			await send(ctx, 'in', 'j', 'v', 14)
			expect(finals(ctx, keys)).toEqual([])
			await send(ctx, 'in', 'j', 'v', 15)
			expect(finals(ctx, keys)).toEqual([['k', 0, 7, 3, 7]])
		})
	})

	it('forwards a session once, when it can no longer merge, whatever the retention of its store', async () => {
		const results: Final[][] = []
		for (const emitFinal of [false, true]) {
			const driver = new TestDriver(config)
			const sessions = driver.input('in', S).groupByKey().windowedBy(SessionWindows.ofInactivityGapWithNoGrace(5))
			const table = emitFinal
				? sessions.count({ emit: 'on-window-close', retention: 100 })
				: sessions.count({ retention: 100 }).suppress(FINAL)
			table.toStream().to('out', { value: num })
			await driver.run(async ctx => {
				await send(ctx, 'in', 'k', 'v', 0)
				await send(ctx, 'in', 'j', 'v', 5) // stream time 0 + 5 + 0: a record of time 5 still merges into [0,0]
				expect(finals(ctx, WindowedCodecs.sessionWindowed(str))).toEqual([])
				await send(ctx, 'in', 'k', 'v', 5) // [0,0] -> [0,5]
				await send(ctx, 'in', 'x', 'v', 11)
				results.push(finals(ctx, WindowedCodecs.sessionWindowed(str)))
			})
		}
		expect(results[0]).toEqual([
			['j', 5, 5, 1, 5],
			['k', 0, 5, 2, 5],
		])
		expect(results[1]).toEqual(results[0])
	})

	it('waits the grace alone after the end of a sliding window', async () => {
		const driver = new TestDriver(config)
		const keys = WindowedCodecs.timeWindowed(str, 10)
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(10, 5))
			.count()
			.suppress(FINAL)
			.toStream()
			.to('out', { value: num })
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'v', 5) // [0,10]
			await send(ctx, 'in', 'k', 'v', 12) // [2,12], and [6,16]: the right window of 5
			await send(ctx, 'in', 'j', 'v', 14)
			await send(ctx, 'in', 'j', 'v', 15)
			// The end is inclusive: at stream time 10 + 5 a record of time 10 still lands in [0,10].
			expect(finals(ctx, keys)).toEqual([])
			await send(ctx, 'in', 'x', 'v', 16)
			expect(finals(ctx, keys)).toEqual([['k', 0, 10, 1, 5]])
			await send(ctx, 'in', 'x', 'v', 22)
			expect(finals(ctx, keys)).toEqual([
				['k', 0, 10, 1, 5],
				['k', 2, 12, 2, 12],
				['j', 4, 14, 1, 14],
				['j', 5, 15, 2, 15],
				['k', 6, 16, 1, 12],
				['x', 6, 16, 1, 16],
			])
		})
	})

	it('forwards a sliding window once, when it can no longer change: the results of emit on-window-close', async () => {
		const inputs: Array<[key: string, timestamp: number]> = [
			['k', 100],
			['k', 100], // updates [90,100] at stream time 100 + 0
			['k', 95], // and again; its right window [96,106] counts both records of 100
			['j', 200], // closes them; [190,200] may still change
		]
		const results: Final[][] = []
		for (const emitFinal of [false, true]) {
			const driver = new TestDriver(config)
			const counts = driver.input('in', S).groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
			const table = emitFinal ? counts.count({ emit: 'on-window-close' }) : counts.count().suppress(FINAL)
			table.toStream().to('out', { value: num })
			await driver.run(async ctx => {
				for (const [key, timestamp] of inputs.slice(0, -1)) await send(ctx, 'in', key, 'v', timestamp)
				expect(finals(ctx, WindowedCodecs.timeWindowed(str, 10))).toEqual([])
				const [key, timestamp] = inputs.at(-1)!
				await send(ctx, 'in', key, 'v', timestamp)
				results.push(finals(ctx, WindowedCodecs.timeWindowed(str, 10)))
			})
		}
		expect(results[0]).toEqual([
			['k', 90, 100, 3, 100],
			['k', 96, 106, 2, 100],
		])
		expect(results[1]).toEqual(results[0])
	})

	it('drops deleted windows from the final results', async () => {
		const driver = new TestDriver(config)
		const keys = WindowedCodecs.timeWindowed(str, 10)
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
			.count()
			.filter((_key, count) => count < 3)
			.suppress(FINAL)
			.toStream()
			.to('out', { value: num })
		await driver.run(async ctx => {
			for (const timestamp of [1, 2, 3]) await send(ctx, 'in', 'k', 'v', timestamp)
			await send(ctx, 'in', 'j', 'v', 4)
			await send(ctx, 'in', 'j', 'v', 10)
			expect(finals(ctx, keys)).toEqual([['j', 0, 10, 1, 4]])
		})
	})

	it('a full buffer stops the app: final results are never emitted early', async () => {
		const driver = new TestDriver(config)
		finalCounts(driver, TimeWindows.ofSizeWithNoGrace(10), { until: 'window-close', name: 'final', maxRecords: 1 })
		await driver.run(async ctx => {
			await send(ctx, 'in', 'a', 'v', 1)
			const failure = send(ctx, 'in', 'b', 'v', 2)
			await expect(failure).rejects.toThrow(FailedProcessingError)
			const error = (await failure.catch((e: unknown) => e)) as Error
			expect((error.cause as Error).message).toMatch(
				/^final buffer exceeded its max capacity\. Currently \[2\/1\] records and \[\d+\/Infinity\] bytes\.$/
			)
			expect(ctx.producer.messagesFor('out')).toEqual([])
		})
	})

	it('a buffer the same record brings back within bounds by closing windows is not full (KS enforceConstraints)', async () => {
		const driver = new TestDriver(config)
		const keys = finalCounts(driver, TimeWindows.ofSizeWithNoGrace(10), { ...FINAL, maxRecords: 1 })
		await driver.run(async ctx => {
			await send(ctx, 'in', 'a', 'v', 1)
			await send(ctx, 'in', 'b', 'v', 12) // buffers [10,20) and closes [0,10)
			expect(finals(ctx, keys)).toEqual([['a', 0, 10, 1, 1]])
		})
	})

	it('takes the largest grace of the windowed aggregations upstream', async () => {
		const driver = new TestDriver(config)
		const counts = (topic: string, graceMs: number) =>
			driver.input(topic, S).groupByKey().windowedBy(TimeWindows.ofSizeAndGrace(10, graceMs)).count()
		counts('a', 5)
			.join(counts('b', 20), (a, b) => a + b, { materialized: { value: num } })
			.suppress(FINAL)
			.toStream()
			.to('out', { value: num })
		const keys = WindowedCodecs.timeWindowed(str, 10)
		await driver.run(async ctx => {
			for (const timestamp of [0, 25]) {
				await send(ctx, 'a', 'k', 'v', timestamp)
				await send(ctx, 'b', 'k', 'v', timestamp)
			}
			expect(finals(ctx, keys)).toEqual([]) // [0,10) would have closed at 15 with a grace of 5
			await send(ctx, 'a', 'k', 'v', 30)
			await send(ctx, 'b', 'k', 'v', 30)
			expect(finals(ctx, keys)).toEqual([['k', 0, 10, 2, 0]])
		})
	})

	it('a table whose keys are not windowed fails the record', async () => {
		const driver = new TestDriver(config)
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
			.count()
			.toStream()
			.selectKey(key => key!.key)
			.toTable({ key: str, value: num })
			.suppress(FINAL)
		await driver.run(async ctx => {
			const failure = send(ctx, 'in', 'k', 'v', 1)
			await expect(failure).rejects.toThrow(FailedProcessingError)
			const error = (await failure.catch((e: unknown) => e)) as Error
			expect((error.cause as Error).message).toMatch(
				/suppresses until windows close, but .* keys are not windowed/
			)
		})
	})
})

describe.each(GUARANTEES)('suppress until a time limit (6.16; %s)', (_, config) => {
	it('forwards the latest update of a key once the limit passed since its first buffered update', async () => {
		const driver = new TestDriver(config)
		const updates = updatesOf(driver.table('t', S).suppress(limit(10)))
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			await send(ctx, 't', 'k', 'b', 5)
			await send(ctx, 't', 'k', 'c', 9) // does not reset the key's buffer time (0)
			expect(updates).toEqual([])
			await send(ctx, 't', 'x', 'x', 10)
			await send(ctx, 't', 'k', 'd', 11)
			await send(ctx, 't', 'y', 'y', 21)
		})
		// The old value is what downstream last saw of the key (KF14).
		expect(updates).toEqual([
			['k', 'c', null, 9],
			['x', 'x', null, 10],
			['k', 'd', 'c', 11],
		])
	})

	it('holds back the counts of a table aggregation, an emptied group included (BC11)', async () => {
		const driver = new TestDriver(config)
		const counts = driver
			.table('t', S)
			.groupBy((_key, value) => [value, value], { key: str, value: str })
			.count()
			.suppress(limit(5))
		const updates = updatesOf(counts)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'A', 0)
			await send(ctx, 't', 'k', 'B', 1) // A goes back to 0
			await send(ctx, 't', 'x', 'X', 10)
		})
		expect(updates).toEqual([
			['A', 0, null, 1],
			['B', 1, null, 1],
		])
	})

	it('forwards the latest update with its timestamp and headers, and forwards deletions', async () => {
		const driver = new TestDriver(config)
		driver.table('t', S).suppress(limit(10)).toStream().to('out', S)
		const headers = (h: string): RecordHeaders => ({ h: Buffer.from(h) })
		await driver.run(async ctx => {
			await ctx.send('t', Buffer.from('a'), { key: Buffer.from('k'), timestamp: 0, headers: headers('1') })
			await ctx.send('t', Buffer.from('b'), { key: Buffer.from('k'), timestamp: 3, headers: headers('2') })
			await send(ctx, 't', 'j', 'v', 4)
			await send(ctx, 't', 'j', null, 5)
			await send(ctx, 't', 'x', 'x', 15)
			expect(written(ctx)).toEqual(['k=b@3', 'j=null@5'])
			expect(ctx.producer.messagesFor('out')[0]!.headers).toEqual(headers('2'))
		})
	})

	it('emits the earliest keys early when the buffer is full (the default)', async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.suppress(limit(100, { maxRecords: 2 }))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'b', '1', 0)
			await send(ctx, 't', 'a', '2', 1)
			expect(written(ctx)).toEqual([])
			await send(ctx, 't', 'c', '3', 2)
			expect(written(ctx)).toEqual(['b=1@0'])
			await send(ctx, 't', 'a', '4', 3) // an update of a buffered key adds no record
			expect(written(ctx)).toEqual(['b=1@0'])
		})
	})

	it('counts bytes against maxBytes: a buffer smaller than one key emits every update at once', async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.suppress(limit(100, { maxBytes: 10 }))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			await send(ctx, 't', 'k', 'b', 1)
			expect(written(ctx)).toEqual(['k=a@0', 'k=b@1'])
		})
	})

	it("stops the app when the buffer is full and whenFull is 'shutdown'", async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.suppress(limit(100, { maxRecords: 1, whenFull: 'shutdown' }))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'a', '1', 0)
			await expect(send(ctx, 't', 'b', '2', 1)).rejects.toThrow(FailedProcessingError)
			expect(written(ctx)).toEqual([])
		})
	})

	it("a buffer the same record brings back within the limit is not full, whenFull 'shutdown' too", async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.suppress(limit(10, { maxRecords: 1, whenFull: 'shutdown' }))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k1', 'a', 0)
			await send(ctx, 't', 'k2', 'b', 20) // forwards k1 before the buffer is checked (KS enforceConstraints)
			expect(written(ctx)).toEqual(['k1=a@0'])
		})
	})

	it('writes every buffer change through to its store (6.16)', async () => {
		const driver = new TestDriver(config)
		driver.table('t', S).suppress(limit(5, { name: 'held' }))
		const entries = bufferedChangeCodec(str)
		const buffered = async (ctx: TestContextApi) => {
			const store = ctx.getStateStore<KeyValueStore<Buffer, Buffer>>('held-store')
			const result: string[] = []
			for await (const [key, bytes] of store.all()) {
				const { bufferTime, priorValue, newValue, timestamp } = entries.decode(bytes)
				result.push(`${key.toString()}:${bufferTime}:${priorValue}->${newValue}@${timestamp}`)
			}
			return result.sort()
		}
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			expect(await buffered(ctx)).toEqual(['k:0:null->a@0'])
			await send(ctx, 't', 'j', 'b', 2)
			expect(await buffered(ctx)).toEqual(['j:2:null->b@2', 'k:0:null->a@0'])
			await send(ctx, 't', 'k', 'c', 4)
			expect(await buffered(ctx)).toEqual(['j:2:null->b@2', 'k:0:null->c@4'])
			await send(ctx, 't', 'x', 'd', 5) // evicts k
			expect(await buffered(ctx)).toEqual(['j:2:null->b@2', 'x:5:null->d@5'])
			await send(ctx, 't', 'k', 'e', 7) // evicts j; k comes back with what downstream saw of it
			expect(await buffered(ctx)).toEqual(['k:7:c->e@7', 'x:5:null->d@5'])
		})
	})
})

describe.each(GUARANTEES)('a suppressed table (6.16; %s)', (_, config) => {
	it('is read as what downstream last saw: the prior value of a buffered key, else the parent table', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S).suppress(limit(10))
		driver
			.input('s', S)
			.leftJoin(table, (value, tableValue) => `${value}+${tableValue}`)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			await send(ctx, 's', 'k', 's1', 1) // buffered without a prior value
			await send(ctx, 't', 'z', 'z', 10) // evicts k
			await send(ctx, 's', 'k', 's2', 11)
			await send(ctx, 't', 'k', 'b', 12)
			await send(ctx, 's', 'k', 's3', 13) // buffered: still a
			await send(ctx, 's', 'q', 's4', 14)
			expect(written(ctx)).toEqual(['k=s1+null@1', 'k=s2+a@11', 'k=s3+a@13', 'q=s4+null@14'])
		})
	})

	it('KF19: makes a mapValues before it send old values, so prior values are mapped ones', async () => {
		const driver = new TestDriver(config)
		const upper = driver.table('t', S).mapValues(v => v.toUpperCase(), { materialized: { value: str } })
		const updates = updatesOf(upper.suppress(limit(10)))
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			await send(ctx, 't', 'x', 'x', 10)
			await send(ctx, 't', 'k', 'b', 20)
			await send(ctx, 't', 'y', 'y', 30)
		})
		expect(updates).toEqual([
			['k', 'A', null, 0],
			['x', 'X', null, 10],
			['k', 'B', 'A', 20],
		])
	})

	it('KF14: suppress -> groupBy -> count converges to the counts without suppress', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S)
		const countByValue = (source: KTable<string, string>, topic: string) =>
			source
				.groupBy((_key, value) => [value, value], { key: str, value: str })
				.count()
				.toStream()
				.to(topic, { value: num })
		const suppressed = table.suppress(limit(10))
		const changes = updatesOf(suppressed)
		countByValue(suppressed, 'suppressed')
		countByValue(table, 'direct')
		await driver.run(async ctx => {
			const updates: [string, string, number][] = [
				['k', 'A', 0],
				['k', 'B', 1],
				['x', 'X', 10],
				['k', 'C', 20],
				['k', 'D', 21], // emitted with the prior value B, not C: B goes back to 0
				['y', 'Y', 40],
				['z', 'Z', 100],
			]
			for (const [key, value, timestamp] of updates) await send(ctx, 't', key, value, timestamp)
			const finalCounts = (topic: string) => {
				const counts = new Map<string, number>()
				for (const { key, value } of ctx.producer.messagesFor(topic)) {
					counts.set(key!.toString(), value === null ? 0 : num.decode(value))
				}
				return counts
			}
			expect(changes).toEqual([
				['k', 'B', null, 1],
				['x', 'X', null, 10],
				['k', 'D', 'B', 21],
				['y', 'Y', null, 40],
			])
			const counted = finalCounts('suppressed')
			const direct = finalCounts('direct')
			for (const category of ['A', 'B', 'C', 'D', 'X', 'Y']) {
				expect([category, counted.get(category) ?? 0]).toEqual([category, direct.get(category) ?? 0])
			}
			expect(counted.get('B')).toBe(0)
			expect(counted.has('Z')).toBe(false) // still buffered
		})
	})
})

describe('suppress across a restart', () => {
	const keys = WindowedCodecs.timeWindowed(str, 10)

	for (const eos of [false, true]) {
		it(`${eos ? 'exactly-once' : 'at-least-once'}: a new task loads the buffer from its store`, async () => {
			const h = new TaskHarness({
				eos,
				build: builder => {
					new DslContext(builder, { applicationId: APP })
						.stream('in', S)
						.groupByKey()
						.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
						.count()
						.suppress(FINAL)
						.toStream()
						.to('out', { key: keys, value: num })
				},
			})
			const emitted = () =>
				h.producer.committedRecords('out').map(({ message }) => {
					const { window }: Windowed<string> = keys.decode(message.key!)
					return [window.start, num.decode(message.value!)]
				})
			const first = await h.start(h.task())
			h.enqueue(first, 'in', [
				{ key: 'k', value: 'v', timestamp: 5 },
				{ key: 'k', value: 'v', timestamp: 12 }, // emits [0,10)
			])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			const restarted = await h.restart()
			h.enqueue(restarted, 'in', [{ key: 'k', value: 'v', timestamp: 25 }]) // emits [10,20), buffered before
			await h.process(restarted)
			expect(await h.commit()).toBe('committed')
			expect(emitted()).toEqual([
				[0, 1],
				[10, 1],
			])
		})

		it(`${eos ? 'exactly-once' : 'at-least-once'}: a new task continues from the committed clock`, async () => {
			const h = new TaskHarness({
				eos,
				build: builder => {
					new DslContext(builder, { applicationId: APP })
						.stream('t', S)
						.toTable()
						.suppress(limit(10))
						.toStream()
						.to('out', S)
				},
			})
			const first = await h.start(h.task())
			h.enqueue(first, 't', [{ key: 'k', value: 'a', timestamp: 20 }])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			// With the clock at 20, an update at 5 is past the limit already: forwarded at once.
			const restarted = await h.restart()
			h.enqueue(restarted, 't', [{ key: 'j', value: 'b', timestamp: 5 }])
			await h.process(restarted)
			expect(await h.commit()).toBe('committed')
			expect(h.producer.committedRecords('out').map(({ message }) => message.key?.toString())).toEqual(['j'])
		})
	}

	it('at-least-once: a key evicted after the last commit is evicted again by the revived task', async () => {
		const h = new TaskHarness({
			build: builder => {
				new DslContext(builder, { applicationId: APP })
					.stream('in', S)
					.groupByKey()
					.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
					.count()
					.suppress({ until: 'window-close', name: 'final' })
					.toStream()
					.to('out', { key: keys, value: num })
			},
		})
		const emitted = () =>
			h.producer.committedRecords('out').map(({ message }) => keys.decode(message.key!).window.start)
		const tombstones = () =>
			h.producer.sent.filter(
				({ topic, message }) => topic === 'app-final-store-changelog' && message.value === null
			)
		const first = await h.start(h.task())
		h.enqueue(first, 'in', [
			{ key: 'k', value: 'v', timestamp: 5 },
			{ key: 'k', value: 'v', timestamp: 15 }, // evicts [0,10)
		])
		await h.process(first)
		expect(await h.commit()).toBe('committed')
		expect(tombstones()).toHaveLength(1)

		const evicting = { key: 'k', value: 'v', timestamp: 50 }
		h.enqueue(first, 'in', [evicting]) // evicts [10,20): its delete waits for the commit
		await h.process(first)
		expect(tombstones()).toHaveLength(1)
		// Revived as after a failed send: the store still has [10,20), and the replay evicts it again.
		const revived = await h.revive(first)
		h.enqueue(revived, 'in', [evicting])
		await h.process(revived)
		expect(emitted()).toEqual([0, 10, 10])
		expect(await h.commit()).toBe('committed')
		expect(tombstones()).toHaveLength(2)
	})
})

describe('suppress topology', () => {
	const windowedCounts = (driver: TestDriver, graceMs = 0) =>
		driver.input('in', S).groupByKey().windowedBy(TimeWindows.ofSizeAndGrace(10, graceMs)).count()

	it('names its processor and its store after the options, else KTABLE-SUPPRESS-<n> and -STATE-STORE-<n>', async () => {
		const driver = new TestDriver()
		const named = windowedCounts(driver).suppress({ until: 'window-close', name: 'final' }) as KTableImpl<
			Windowed<string>,
			number
		>
		const generated = named.suppress(FINAL) as KTableImpl<Windowed<string>, number>
		expect(named.node).toBe('final')
		expect(generated.node).toMatch(/^KTABLE-SUPPRESS-\d{10}$/)
		await driver.run(async ctx => {
			expect(ctx.getStateStore('final-store').name).toBeDefined()
			expect(ctx.getStateStore('KTABLE-SUPPRESS-STATE-STORE-1').name).toBeDefined()
		})
	})

	it('finds the grace of the windowed aggregation upstream, across derived tables, streams and repartitions', () => {
		const driver = new TestDriver()
		const counts = windowedCounts(driver, 7)
		const derived = counts
			.filter((_key, count) => count > 0)
			.toStream()
			.repartition()
			.toTable() as KTableImpl<Windowed<string>, number>
		expect(windowCloseGraceMs(derived.ctx, derived.node)).toBe(7)
		const sessions = driver
			.input('s', S)
			.groupByKey()
			.windowedBy(SessionWindows.ofInactivityGapAndGrace(5, 3))
			.count() as KTableImpl<Windowed<string>, number>
		expect(windowCloseGraceMs(sessions.ctx, sessions.node)).toBe(9) // its end is inclusive
		const sliding = driver
			.input('w', S)
			.groupByKey()
			.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(10, 4))
			.count() as KTableImpl<Windowed<string>, number>
		expect(windowCloseGraceMs(sliding.ctx, sliding.node)).toBe(5) // its end is inclusive
	})

	it('needs a windowed aggregation upstream of every parent until windows close', () => {
		const driver = new TestDriver()
		expect(() => driver.table('t', S).suppress(FINAL)).toThrow(
			/^suppress\(\) until 'window-close' needs a windowed aggregation upstream: .* Got \[KSTREAM-SOURCE-\d+->KTABLE-SOURCE-\d+\]\.$/
		)
		const windowedTable = driver.table('w', { key: WindowedCodecs.timeWindowed(str, 10), value: num })
		const joined = windowedCounts(driver).join(windowedTable, (a, b) => a + b, { materialized: { value: num } })
		expect(() => joined.suppress(FINAL)).toThrow(TopologyError)
	})

	it('validates its options and needs the codecs of the table', () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		for (const maxRecords of [-1, 1.5, Number.NaN]) {
			expect(() => table.suppress(limit(10, { maxRecords }))).toThrow(
				/^suppress\(\) maxRecords must be a non-negative whole number/
			)
		}
		expect(() => table.suppress(limit(10, { maxBytes: -1 }))).toThrow(TopologyError)
		expect(() => table.suppress(limit(-1))).toThrow(/^The suppress time limit must not be negative/)
		expect(() => table.suppress(limit(10, { maxRecords: Number.POSITIVE_INFINITY }))).not.toThrow()
		expect(() => table.mapValues(v => v.length).suppress(limit(10))).toThrow(
			/^suppress\(\) needs a value codec to buffer the table's values/
		)
	})

	it('forces old values on its parent and answers for the parent', () => {
		const driver = new TestDriver()
		const table = driver.table('t', S) as KTableImpl<string, string>
		const suppressed = table.suppress(limit(10)) as KTableImpl<string, string>
		expect(suppressed.table.enableSendingOldValues(false)).toBe(true)
	})
})
