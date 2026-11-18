import { describe, expect, it, vi } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { DslContext } from '@/dsl/context.js'
import { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import { OuterJoinStore } from '@/operators/outer-join-store.js'
import { decodeCommitMetadata } from '@/processor/metadata.js'
import type { StreamTask } from '@/runtime/task.js'
import type { SessionStore } from '@/state.js'
import type { TaskStateManager } from '@/state/state-manager.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { APP, TaskHarness } from '../support/task-harness.js'

const str = codec.string()
const S = { key: str, value: str }
const key = (k: string) => ({ key: Buffer.from(k) })
const client = { clientId: 'app', brokers: ['localhost:9092'] }

type JoinKind = 'join' | 'leftJoin' | 'outerJoin'

const joiner = (a: string | null, b: string | null) => `${a}+${b}`

/** Sends a string value with a string key at a timestamp. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp: number) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { ...key(k), timestamp })
}

/** Sends `value` with the header `h` = value. */
function sendWithHeader(ctx: TestContextApi, topic: string, k: string, value: string, timestamp: number) {
	return ctx.send(topic, Buffer.from(value), { ...key(k), timestamp, headers: { h: Buffer.from(value) } })
}

const values = (ctx: TestContextApi) => ctx.output('out', S).map(o => o.value)

/** Results as `<value>@<timestamp>`. */
const timed = (ctx: TestContextApi) => ctx.producer.messagesFor('out').map(m => `${m.value?.toString()}@${m.timestamp}`)

/** Results as `[key, value, timestamp, header h]`. */
const detailed = (ctx: TestContextApi) =>
	ctx.producer
		.messagesFor('out')
		.map(m => [m.key?.toString() ?? null, m.value?.toString(), m.timestamp, m.headers?.h?.toString()])

/** `l.<kind>(r, windows)`, results written to 'out' as `<left>+<right>`. */
function windowJoin(kind: JoinKind, windows: JoinWindows, config?: TestDriverConfig): TestDriver {
	const driver = new TestDriver(config)
	const l = driver.input('l', S)
	const r = driver.input('r', S)
	const joined =
		kind === 'join'
			? l.join(r, joiner, windows)
			: kind === 'leftJoin'
				? l.leftJoin(r, joiner, windows)
				: l.outerJoin(r, joiner, windows)
	joined.to('out', S)
	return driver
}

describe.each(GUARANTEES)('stream-stream join results (%s)', (_guarantee, config) => {
	it('a match is at the later timestamp of both records, with the headers of the record that produced it', async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(1000), config)
		await driver.run(async ctx => {
			await sendWithHeader(ctx, 'l', 'k', 'L', 100)
			await sendWithHeader(ctx, 'r', 'k', 'R', 300)
			await sendWithHeader(ctx, 'r', 'k', 'E', 50)
			expect(detailed(ctx)).toEqual([
				['k', 'L+R', 300n, 'R'],
				['k', 'L+E', 100n, 'E'],
			])
		})
	})

	it('joins every pair of matching records, each once, in timestamp order of the other side', async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(1000), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L2', 200)
			await send(ctx, 'l', 'k', 'L1', 100)
			await send(ctx, 'l', 'other', 'O', 150)
			await send(ctx, 'r', 'k', 'R1', 150)
			await send(ctx, 'r', 'k', 'R2', 160)
			expect(values(ctx)).toEqual(['L1+R1', 'L2+R1', 'L1+R2', 'L2+R2'])
		})
	})

	it('B8: records with the same key and timestamp are all kept and each joins exactly once', async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(1000), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 100)
			await send(ctx, 'l', 'k', 'L', 100)
			await send(ctx, 'r', 'k', 'R', 100)
			await send(ctx, 'r', 'k', 'S', 100)
			expect(values(ctx)).toEqual(['L+R', 'L+R', 'L+S', 'L+S'])
			// One entry per key and timestamp, whose value lists the records in arrival order.
			const buffer = ctx.getStateStore<SessionStore<string, string[]>>('stream-join-left-0')
			expect(await buffer.get({ key: 'k', windowStart: 100, windowEnd: 100 })).toEqual(['L', 'L'])
		})
	})

	it('window bounds are inclusive: left.ts - before <= right.ts <= left.ts + after; the right side sees them mirrored', async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(1000).before(0), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'early', 4999)
			await send(ctx, 'l', 'k', 'L2', 3999)
			await send(ctx, 'r', 'k', 'same', 5000)
			await send(ctx, 'r', 'k', 'last', 6000)
			await send(ctx, 'r', 'k', 'late', 6001)
			expect(values(ctx)).toEqual(['L2+early', 'L+same', 'L+last'])
		})
	})

	it("KF2: probes are clipped by the other buffer's own store clock, kept before + after + grace", async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(10).after(0), config)
		await driver.run(async ctx => {
			// The right buffer's clock is 100, so R stays readable down to 91, whatever the stream time is.
			await send(ctx, 'r', 'k', 'R', 100)
			await send(ctx, 'l', 'k', 'L', 110)
			// Once the right buffer's clock is 200, R is past its retention: L2's window [95, 105] finds nothing.
			await send(ctx, 'r', 'k', 'S', 200)
			await send(ctx, 'l', 'k', 'L2', 105)
			expect(values(ctx)).toEqual(['L+R'])
		})
	})

	it('a self-join matches each record with itself once', async () => {
		const driver = new TestDriver(config)
		const s = driver.input('s', S)
		s.join(s, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(1000)).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 's', 'k', 'A', 100)
			expect(values(ctx)).toEqual(['A+A'])
			await send(ctx, 's', 'k', 'B', 200)
			expect(values(ctx)).toEqual(['A+A', 'B+A', 'A+B', 'B+B'])
		})
	})

	it('KIP-962: an outer side joins a null-key record with null at once; inner sides and null values drop records', async () => {
		const expected: Record<JoinKind, unknown[]> = {
			join: [],
			leftJoin: [[null, 'L+null']],
			outerJoin: [
				[null, 'L+null'],
				[null, 'null+R'],
			],
		}
		for (const windows of [JoinWindows.ofTimeDifferenceWithNoGrace(1000), JoinWindows.of(1000)]) {
			for (const kind of ['join', 'leftJoin', 'outerJoin'] as const) {
				const driver = windowJoin(kind, windows, config)
				await driver.run(async ctx => {
					await ctx.send('l', Buffer.from('L'), { timestamp: 100 })
					await send(ctx, 'l', 'k', null, 100)
					await ctx.send('r', Buffer.from('R'), { timestamp: 100 })
					await send(ctx, 'r', 'k', null, 100)
					expect(ctx.output('out', S).map(o => [o.key, o.value])).toEqual(expected[kind])
				})
			}
		}
	})

	it('KF8: JoinWindows.of (deprecated) joins unmatched records of left and outer joins with null at once', async () => {
		const left = windowJoin('leftJoin', JoinWindows.of(1000), config)
		await left.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 100)
			await send(ctx, 'r', 'k', 'R', 300)
			await send(ctx, 'r', 'j', 'S', 300)
			expect(timed(ctx)).toEqual(['L+null@100', 'L+R@300'])
		})
		const outer = windowJoin('outerJoin', JoinWindows.of(1000), config)
		await outer.run(async ctx => {
			await send(ctx, 'r', 'k', 'R', 100)
			await send(ctx, 'l', 'k', 'L', 200)
			await send(ctx, 'l', 'k', 'X', 5000)
			expect(timed(ctx)).toEqual(['null+R@100', 'L+R@200', 'X+null@5000'])
		})
	})

	it('a left record that finds its match later never produces a null result', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(1000), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'R', 5500)
			await send(ctx, 'l', 'other', 'X', 10_000)
			expect(timed(ctx)).toEqual(['L+R@5500'])
		})
	})

	it('an unmatched record is joined with null at its own timestamp once stream time passed its window', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 1000)
			// A's window [900, 1100] still takes records at 1100.
			await send(ctx, 'l', 'b', 'B', 1100)
			expect(timed(ctx)).toEqual([])
			await send(ctx, 'l', 'c', 'C', 1101)
			expect(timed(ctx)).toEqual(['A+null@1000'])
		})
	})

	it('KF11: unmatched records close oldest first, right before left at one timestamp, with the closing headers', async () => {
		const driver = windowJoin('outerJoin', JoinWindows.ofTimeDifferenceWithNoGrace(10), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 100)
			await send(ctx, 'r', 'b', 'B', 100)
			await send(ctx, 'r', 'c', 'C', 105)
			await sendWithHeader(ctx, 'l', 'z', 'Z', 200)
			expect(detailed(ctx)).toEqual([
				['b', 'null+B', 100n, 'Z'],
				['a', 'A+null', 100n, 'Z'],
				['c', 'null+C', 105n, 'Z'],
			])
		})
	})

	it('a record whose window already ended is joined with null at once; a match removes a pending one (KS testLowerWindowBound)', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'K', 1000)
			// Out of order: J's window [750, 950] ended before the stream time 1000, so no record can match it any more.
			await send(ctx, 'l', 'j', 'J', 850)
			await send(ctx, 'r', 'k', 'R', 1050)
			await send(ctx, 'l', 'm', 'M', 1200)
			expect(timed(ctx)).toEqual(['J+null@850', 'K+R@1050'])
		})
	})

	it('a record whose window ends at the stream time can still be matched: it waits', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'K', 1000)
			await send(ctx, 'l', 'j', 'J', 900) // its window [800, 1000] ends at the stream time
			await send(ctx, 'r', 'j', 'R', 1000)
			expect(timed(ctx)).toEqual(['J+R@1000'])
		})
	})

	it('a pending record whose window and grace end at the stream time is still open', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 1000)
			await send(ctx, 'l', 'b', 'B', 1050)
			await send(ctx, 'l', 'c', 'C', 1150) // closes A; B's window ends at 1150
			await send(ctx, 'r', 'b', 'R', 1150)
			expect(timed(ctx)).toEqual(['A+null@1000', 'B+R@1150'])
		})
	})

	it('a left join scans its pending records up to the first one still open', async () => {
		const pending = OuterJoinStore.prototype.pending
		let read = 0
		const scans = vi
			.spyOn(OuterJoinStore.prototype, 'pending')
			.mockImplementation(async function* (this: OuterJoinStore<unknown, unknown, unknown>) {
				for await (const entry of pending.call(this)) {
					read++
					yield entry
				}
			})
		try {
			const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(10), config)
			await driver.run(async ctx => {
				for (let timestamp = 0; timestamp < 50; timestamp++) await send(ctx, 'l', 'k', 'L', timestamp)
				// Each record from 11 on closes the oldest pending one: the scan reads it and the next, still open.
				expect(timed(ctx)).toHaveLength(39)
				expect(read).toBe(2 * 39)
			})
		} finally {
			scans.mockRestore()
		}
	})

	it('in a left join only a match of a right record removes a pending record: the left side has them all', async () => {
		const removeOther = vi.spyOn(OuterJoinStore.prototype, 'removeOther')
		try {
			const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100), config)
			await driver.run(async ctx => {
				await send(ctx, 'r', 'k', 'R', 1000)
				await send(ctx, 'l', 'k', 'L', 1000)
				await send(ctx, 'r', 'k', 'S', 1000)
				expect(timed(ctx)).toEqual(['L+R@1000', 'L+S@1000'])
			})
			expect(removeOther).toHaveBeenCalledTimes(1)
		} finally {
			removeOther.mockRestore()
		}
	})

	it('asymmetric windows: a left record waits after + grace, a right one before + grace', async () => {
		// A left record at t joins right records in [t - 10, t + 100], a right record left ones in [t - 100, t + 10].
		const driver = windowJoin('outerJoin', JoinWindows.ofTimeDifferenceWithNoGrace(100).before(10), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 1000)
			await send(ctx, 'r', 'b', 'B', 1000)
			// B closed after 1010: a left record past it scans, although no left record can have closed by its own side's
			// after, 1000 + 100 (KS checks the triggering side's after and would wait for D).
			await send(ctx, 'l', 'c', 'C', 1020)
			expect(timed(ctx)).toEqual(['null+B@1000'])
			await send(ctx, 'r', 'd', 'D', 1030)
			expect(timed(ctx)).toEqual(['null+B@1000'])
			// Out of order on the right: X's window [800, 910] ended before the stream time 1030.
			await send(ctx, 'r', 'x', 'X', 900)
			expect(timed(ctx)).toEqual(['null+B@1000', 'null+X@900'])
			// A closes after 1100, D after 1040; C stays pending until after 1120.
			await send(ctx, 'l', 'e', 'E', 1101)
			expect(timed(ctx)).toEqual(['null+B@1000', 'null+X@900', 'A+null@1000', 'null+D@1030'])
		})
	})

	it('a record of the side with the longer wait emits a closed record of the other side before a late match removes it', async () => {
		// A left record at t joins right records in [t - 3, t + 4], a right record left ones in [t - 4, t + 3].
		const driver = windowJoin('outerJoin', JoinWindows.ofTimeDifferenceWithNoGrace(3).after(4), config)
		await driver.run(async ctx => {
			await send(ctx, 'r', 'b', 'R109', 109)
			await send(ctx, 'r', 'a', 'R113', 113)
			expect(timed(ctx)).toEqual(['null+R109@109'])
			await send(ctx, 'r', 'b', 'R110', 110)
			// R110's window (left records in [106, 113]) closed at stream time 114.
			await send(ctx, 'l', 'c', 'L114', 114)
			expect(timed(ctx)).toEqual(['null+R109@109', 'null+R110@110'])
			// Late, within no grace of the left buffer's clock, L107 still joins both; R110 had been emitted already.
			await send(ctx, 'l', 'b', 'L107', 107)
			await send(ctx, 'l', 'z', 'L200', 200)
			expect(timed(ctx)).toEqual([
				'null+R109@109',
				'null+R110@110',
				'L107+R109@109',
				'L107+R110@110',
				'null+R113@113',
				'L114+null@114',
			])
		})
	})

	it("a left join scans by the left records' wait, whichever side triggers", async () => {
		// A left record at t joins right records in [t - 4, t + 3]: it is pending until stream time passes t + 3.
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(3).before(4), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 100)
			await send(ctx, 'r', 'z', 'Z', 103)
			expect(timed(ctx)).toEqual([])
			// KS's right side adds its own after (4) and would skip this scan.
			await send(ctx, 'r', 'z', 'Z', 104)
			expect(timed(ctx)).toEqual(['A+null@100'])
		})
	})

	it('a record whose window and grace closed when it arrived is joined with null at once (negative after, time 0)', async () => {
		// A left record at t joins right records in [t - 10, t - 5]: at stream time 0, a left record at 0 is closed.
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(10).after(-5), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'A', 0)
			await send(ctx, 'l', 'k', 'B', 0)
			expect(timed(ctx)).toEqual(['A+null@0', 'B+null@0'])
		})
	})

	it('grace keeps a window open for late records; an unmatched record waits for its window and grace', async () => {
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceAndGrace(10, 50), config)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 100)
			await send(ctx, 'l', 'x', 'X', 150)
			// Late by 45 ms, within the grace: R still joins A, which then is no longer pending.
			await send(ctx, 'r', 'a', 'R', 105)
			await send(ctx, 'l', 'y', 'Y', 161)
			expect(timed(ctx)).toEqual(['A+R@105'])
			await send(ctx, 'l', 'z', 'Z', 211)
			expect(timed(ctx)).toEqual(['A+R@105', 'X+null@150'])
		})
	})

	it('emission scans run at most once per emitIntervalMs of wall clock', async () => {
		const throttled = { ...config, emitIntervalMs: 1000, initialWallClockTimeMs: 0 }
		const driver = windowJoin('leftJoin', JoinWindows.ofTimeDifferenceWithNoGrace(10), throttled)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'a', 'A', 100)
			await send(ctx, 'l', 'b', 'B', 200)
			await send(ctx, 'l', 'c', 'C', 300)
			expect(timed(ctx)).toEqual(['A+null@100'])
			await ctx.advanceWallClockTime(1000)
			await send(ctx, 'l', 'd', 'D', 400)
			expect(timed(ctx)).toEqual(['A+null@100', 'B+null@200', 'C+null@300'])
		})
	})
})

describe('stream-stream joins', () => {
	it('KF12: the repartition before a left join keeps null keys; the one before an inner join drops them', async () => {
		const driver = new TestDriver()
		// 'none' gets a null key, which the repartition topic before the join passes on.
		const rekeyed = driver.input('l', S).selectKey((k, v) => (v === 'none' ? null : k)) as KStream<string, string>
		rekeyed
			.leftJoin(driver.input('r', S), joiner, JoinWindows.ofTimeDifferenceWithNoGrace(1000), { key: str })
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'none', 100)
			expect(ctx.output('out', S).map(o => [o.key, o.value])).toEqual([[null, 'none+null']])
		})

		const app = flow({ applicationId: 'app', client })
		const [a, b] = ['a', 'b'].map(t => app.stream(t, S).selectKey((k, v) => v ?? k))
		a!.join(b!, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(1000), { name: 'inner', key: str })
		a!.leftJoin(b!, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(1000), { name: 'left', key: str })
		const sinks = internals(app)
			.build()
			.subtopologies.flatMap(s => s.nodes)
			.filter(n => n.kind === 'sink')
		expect(Object.fromEntries(sinks.map(s => [s.topic, s.nullKeys]))).toEqual({
			'app-inner-left-repartition': 'drop',
			'app-inner-right-repartition': 'drop',
			'app-left-left-repartition': 'keep',
			'app-left-right-repartition': 'keep',
		})
	})

	it('B8: under exactly-once a record replayed after an abort is buffered once', async () => {
		const h = new TaskHarness({
			eos: true,
			build: builder => {
				const ctx = new DslContext(builder, { applicationId: APP })
				const left: KStream<string, string> = ctx.stream('l', S)
				const right: KStream<string, string> = ctx.stream('r', S)
				left.join(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(1000)).to('out', S)
			},
		})
		const first = await h.start(h.task())
		h.enqueue(first, 'l', [{ key: 'k', value: 'L', timestamp: 100 }])
		await h.process(first)
		await h.collector.abortTransaction(h.hardStop.signal)
		expect(await first.close('dirty')).toBe('dirty')

		const replayed = await h.restart()
		h.enqueue(replayed, 'l', [{ key: 'k', value: 'L', timestamp: 100, offset: 0n }])
		h.enqueue(replayed, 'r', [{ key: 'k', value: 'R', timestamp: 100 }])
		await h.process(replayed)
		expect(await h.commit()).toBe('committed')
		expect(h.output('out').map(o => o.value)).toEqual(['L+R'])
	})

	it('Joined.within takes the size of TimeWindows and the gap of SessionWindows as a symmetric window', async () => {
		for (const within of [TimeWindows.of(1000), SessionWindows.withInactivityGap(1000)]) {
			const driver = new TestDriver()
			const l = driver.input('l', S)
			const r = driver.input('r', S)
			l.join(r, joiner, { within }).to('out', S)
			await driver.run(async ctx => {
				await send(ctx, 'l', 'k', 'L', 5000)
				await send(ctx, 'r', 'k', 'in', 4000)
				await send(ctx, 'r', 'k', 'out', 6001)
				expect(values(ctx)).toEqual(['L+in'])
			})
		}
	})

	it('Joined.within: a match is at max(left, right) (0.13: the arriving record), unmatched records join null at once', async () => {
		const driver = new TestDriver()
		driver
			.input('l', S)
			.leftJoin(driver.input('r', S), joiner, { within: TimeWindows.of('10m') })
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'r', 'k', 'B', 1000)
			await send(ctx, 'l', 'k', 'A', 500)
			await send(ctx, 'l', 'k', 'C', 2_000_000)
			expect(timed(ctx)).toEqual(['A+B@1000', 'C+null@2000000'])
		})
	})

	it('a join without a window, or with SlidingWindows, is a TopologyError', () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		expect(() => l.join(r, joiner, {})).toThrow(TopologyError)
		expect(() => l.join(r, joiner, {})).toThrow('join() with a stream needs JoinWindows as its third argument')
		expect(() => l.leftJoin(r, joiner, { within: SlidingWindows.ofTimeDifferenceWithNoGrace(10) })).toThrow(
			'leftJoin() with a stream cannot use SlidingWindows'
		)
	})

	it('B2: the StreamJoined codecs are used when no side is repartitioned', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S).mapValues(v => ({ v }))
		const r = driver.input('r', S)
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(1000)
		l.join(r, (a, b) => `${a.v}+${b}`, windows, { value: codec.json<{ v: string | null }>() }).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 100)
			await send(ctx, 'r', 'k', 'R', 100)
			expect(values(ctx)).toEqual(['L+R'])
		})
	})

	it('needs codecs for the buffers: a value changed upstream without StreamJoined.value is a TopologyError', () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S).mapValues(v => v)
		expect(() => l.join(r, (a, b) => a + b, JoinWindows.of(1000))).toThrow(
			'join() of the other stream needs a value codec'
		)
	})

	it('B22: an invalid other stream throws before anything is registered', () => {
		const app = flow({ applicationId: 'app', client })
		const foreign = flow({ applicationId: 'other', client }).stream('x')
		const rekeyed = app.stream('a', S).selectKey((_k, v) => v)
		expect(() => rekeyed.join(foreign as never, (a, b) => a + b, JoinWindows.of(1000))).toThrow(
			'join() needs a stream of the same app'
		)
		const topology = internals(app).build()
		expect(topology.internalTopics.size).toBe(0)
		expect(topology.subtopologies.flatMap(s => s.stores)).toEqual([])
	})

	it('repartitions a re-keyed side before joining; a named join names its topics, nodes and stores', async () => {
		const app = flow({ applicationId: 'app', client })
		const l = app.stream('l', S).selectKey((_k, v) => v!)
		const r = app.stream('r', S)
		l.join(r, (a, b) => a + b, JoinWindows.of(1000), { name: 'pairs', key: str })
		const topology = internals(app).build()
		expect([...topology.internalTopics.keys()].sort()).toEqual([
			'app-pairs-left-repartition',
			'app-pairs-other-join-store-changelog',
			'app-pairs-this-join-store-changelog',
		])
		const join = topology.subtopologyOf('app-pairs-left-repartition')!
		expect(join).toBe(topology.subtopologyOf('r'))
		expect(topology.subtopologyOf('l')!.nodes.map(n => n.name)).toContain('pairs-left-repartition-sink')
		expect(join.nodes.map(n => n.name).filter(n => n.startsWith('pairs'))).toEqual([
			'pairs-left-repartition-source',
			'pairs-this-join',
			'pairs-other-join',
			'pairs-merge',
		])
		expect(join.stores.map(s => s.name)).toEqual(['pairs-this-join-store', 'pairs-other-join-store'])
		expect(topology.copartitionGroups.map(group => [...group].sort())).toEqual([
			['app-pairs-left-repartition', 'r'],
		])
	})

	it('names the join nodes of left and outer joins as Kafka Streams does', () => {
		const app = flow({ applicationId: 'app', client })
		const [a, b] = ['a', 'b'].map(t => app.stream(t, S))
		a!.leftJoin(b!, joiner, JoinWindows.of(1000), { name: 'l' })
		a!.outerJoin(b!, joiner, JoinWindows.of(1000), { name: 'o' })
		a!.leftJoin(b!, joiner, JoinWindows.of(1000))
		a!.outerJoin(b!, joiner, JoinWindows.of(1000))
		const [join] = internals(app).build().subtopologies
		expect(join!.nodes.map(n => n.name.replace(/-\d+$/, '')).filter(n => n !== 'KSTREAM-SOURCE')).toEqual([
			'l-this-join',
			'l-outer-other-join',
			'l-merge',
			'o-outer-this-join',
			'o-outer-other-join',
			'o-merge',
			'KSTREAM-JOINTHIS',
			'KSTREAM-OUTEROTHER',
			'KSTREAM-MERGE',
			'KSTREAM-OUTERTHIS',
			'KSTREAM-OUTEROTHER',
			'KSTREAM-MERGE',
		])
	})

	it('StreamJoined.storeName names the buffers and the outer-join store; JoinWindows.of has no outer-join store', () => {
		const app = flow({ applicationId: 'app', client })
		const [a, b, c] = ['a', 'b', 'c'].map(t => app.stream(t, S))
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(1000)
		a!.join(b!, joiner, windows, { storeName: 'ab' })
		a!.leftJoin(b!, joiner, windows, { storeName: 'left' })
		a!.outerJoin(b!, joiner, windows, { name: 'outer' })
		a!.outerJoin(b!, joiner, JoinWindows.of(1000), { storeName: 'legacy' })
		a!.leftJoin(c!, joiner, windows)
		a!.outerJoin(c!, joiner, windows)
		const stores = internals(app)
			.build()
			.subtopologies.flatMap(s => s.stores.map(store => store.name))
		expect(stores).toEqual([
			'ab-this-join-store',
			'ab-other-join-store',
			'left-this-join-store',
			'left-other-join-store',
			'left-left-shared-join-store',
			'outer-this-join-store',
			'outer-other-join-store',
			'outer-outer-shared-join-store',
			'legacy-this-join-store',
			'legacy-other-join-store',
			'stream-left-join-left-0',
			'stream-left-join-right-1',
			'stream-left-join-shared-2',
			'stream-outer-join-left-3',
			'stream-outer-join-right-4',
			'stream-outer-join-shared-5',
		])
	})

	it('buffers are session-layout stores kept before + after + grace, logged a day longer; the outer-join store is compacted', () => {
		const app = flow({ applicationId: 'app', client })
		const windows = JoinWindows.ofTimeDifferenceAndGrace(10_000, 5_000).before(2_000)
		app.stream('l', S).leftJoin(app.stream('r', S), joiner, windows)
		const [join] = internals(app).build().subtopologies
		expect(join!.stores.map(s => [s.name, s.kind, s.retentionMs, s.clock])).toEqual([
			['stream-left-join-left-0', 'session', 17_000, {}],
			['stream-left-join-right-1', 'session', 17_000, {}],
			['stream-left-join-shared-2', 'key-value', undefined, undefined],
		])
		const configs = join!.stores.map(store => {
			expect(store.logging).toMatchObject({ enabled: true, topic: `app-${store.name}-changelog` })
			return (store.logging as { configs: Record<string, string> }).configs
		})
		expect(configs.map(c => [c['cleanup.policy'], c['retention.ms']])).toEqual([
			['delete,compact', String(17_000 + 86_400_000)],
			['delete,compact', String(17_000 + 86_400_000)],
			['compact', '-1'],
		])

		// Joined.within: JoinWindows.of keeps 24 h (twice the window plus the legacy grace of 24 h minus twice the window).
		const legacy = flow({ applicationId: 'app', client })
		legacy.stream('l', S).join(legacy.stream('r', S), joiner, { within: TimeWindows.of('10s') })
		const [legacyJoin] = internals(legacy).build().subtopologies
		expect(legacyJoin!.stores.map(s => s.retentionMs)).toEqual([86_400_000, 86_400_000])

		const noLog = flow({ applicationId: 'app', client })
		noLog.stream('l', S).outerJoin(noLog.stream('r', S), joiner, windows, { changelog: false })
		const [unlogged] = internals(noLog).build().subtopologies
		expect(unlogged!.stores.map(s => s.logging.enabled)).toEqual([false, false, false])
	})

	it('the sides share one processor clock, committed under the left buffer name; each buffer expires by its own store clock', async () => {
		const driver = windowJoin('join', JoinWindows.ofTimeDifferenceWithNoGrace(1000))
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 1_000)
			await send(ctx, 'r', 'k', 'R', 1_000_000)
			const committed = ctx.consumer.committed.get('r:0')!
			expect(decodeCommitMetadata(committed.metadata).metadata).toEqual(
				new Map([
					['stream-join-left-0', 1_000_000],
					['stream-join-left-0.observed', 1_000],
					['stream-join-right-1.observed', 1_000_000],
				])
			)
			const entries = (store: string) =>
				ctx.getStateStore<SessionStore<string, string[]>>(store).approximateNumEntries()
			// R's expiry ran by the shared clock's cadence, but L is the newest record of its buffer: it stays.
			expect(await entries('stream-join-left-0')).toBe(1)
			// A minute of stream time later, each buffer drops what is past its retention by its own clock as of the last
			// commit: R2's expiry drops L, not R, which is the newest record of the committed right buffer.
			await send(ctx, 'l', 'k', 'L2', 100_000)
			await send(ctx, 'r', 'k', 'R2', 1_100_000)
			expect(await entries('stream-join-left-0')).toBe(1)
			expect(await entries('stream-join-right-1')).toBe(2)
			await send(ctx, 'l', 'k', 'L3', 1_200_000)
			expect(await entries('stream-join-left-0')).toBe(2)
			expect(await entries('stream-join-right-1')).toBe(1)
		})
	})

	for (const eos of [false, true]) {
		const guarantee = eos ? 'exactly-once' : 'at-least-once'
		/** A task of `l.<kind>(r)` with a 1 s window and 10 s of grace. */
		const harness = (kind: 'join' | 'leftJoin') =>
			new TaskHarness({
				eos,
				build: builder => {
					const ctx = new DslContext(builder, { applicationId: APP })
					const left: KStream<string, string> = ctx.stream('l', S)
					const right: KStream<string, string> = ctx.stream('r', S)
					const windows = JoinWindows.ofTimeDifferenceAndGrace(1000, 10_000)
					const joined =
						kind === 'join' ? left.join(right, joiner, windows) : left.leftJoin(right, joiner, windows)
					joined.to('out', S)
				},
			})

		it(`${guarantee}: a task re-created from its commits keeps its buffers and seeds the clocks`, async () => {
			const base = 1_000_000_000
			const h = harness('join')
			const first = await h.start(h.task())
			h.enqueue(first, 'l', [
				{ key: 'a', value: 'A', timestamp: base },
				{ key: 'b', value: 'B', timestamp: base + 50_000 },
			])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			const restarted = await h.restart()
			h.enqueue(restarted, 'r', [
				{ key: 'a', value: 'R', timestamp: base + 500 },
				{ key: 'b', value: 'S', timestamp: base + 50_500 },
			])
			await h.process(restarted)
			await h.commit()
			// B was buffered before the restart; A is past the left buffer's retention (12 s) by its clock seeded from the
			// commits, so it neither joins nor survives the first expiry after the restart.
			expect(h.output('out').map(o => o.value)).toEqual(['B+S'])
			const { stateManager } = restarted as unknown as { stateManager: TaskStateManager }
			const entries = (store: string) =>
				(stateManager.store(store) as SessionStore<string, string[]>).approximateNumEntries()
			expect(await entries('stream-join-left-0')).toBe(1)
		})

		it(`${guarantee}: pending unmatched records survive a restart and close on the next record`, async () => {
			const h = harness('leftJoin')
			const first = await h.start(h.task())
			h.enqueue(first, 'l', [{ key: 'a', value: 'A', timestamp: 100 }])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			const restarted = await h.restart()
			h.enqueue(restarted, 'l', [{ key: 'b', value: 'B', timestamp: 20_000 }])
			await h.process(restarted)
			await h.commit()
			// The scan bound is seeded from the store, so B's record closes A without another unmatched record.
			expect(h.output('out').map(o => [o.value, o.timestamp])).toEqual([['A+null', 100]])
		})
	}

	it('at-least-once: a match forwarded after the last commit is forwarded again by the revived task', async () => {
		const h = new TaskHarness({
			build: builder => {
				const ctx = new DslContext(builder, { applicationId: APP })
				const left: KStream<string, string> = ctx.stream('l', S)
				const right: KStream<string, string> = ctx.stream('r', S)
				left.join(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(10)).to('out', S)
			},
		})
		const first = await h.start(h.task())
		h.enqueue(first, 'l', [{ key: 'a', value: 'A', timestamp: 100 }])
		await h.process(first)
		expect(await h.commit()).toBe('committed')
		const uncommitted = async (task: StreamTask) => {
			h.enqueue(task, 'r', [{ key: 'a', value: 'R', timestamp: 105 }])
			await h.process(task)
			// Its expiry pass runs with the left buffer's clock past the retention of A.
			h.enqueue(task, 'l', [{ key: 'b', value: 'B', timestamp: 70_000 }])
			await h.process(task)
		}
		await uncommitted(first)
		// Revived as after a failed send: the replay finds A again.
		await uncommitted(await h.revive(first))
		expect(h.output('out').map(o => o.value)).toEqual(['A+R', 'A+R'])
	})

	it('at-least-once: a null result forwarded after the last commit is forwarded again by the revived task', async () => {
		const h = new TaskHarness({
			build: builder => {
				const ctx = new DslContext(builder, { applicationId: APP })
				const left: KStream<string, string> = ctx.stream('l', S)
				const right: KStream<string, string> = ctx.stream('r', S)
				left.leftJoin(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(10)).to('out', S)
			},
		})
		const first = await h.start(h.task())
		h.enqueue(first, 'l', [{ key: 'k', value: 'L', timestamp: 100 }])
		await h.process(first)
		expect(await h.commit()).toBe('committed')
		const closing = async (task: StreamTask) => {
			h.enqueue(task, 'l', [{ key: 'other', value: 'O', timestamp: 200 }])
			await h.process(task)
		}
		await closing(first)
		expect(h.output('out').map(o => [o.key, o.value, o.timestamp])).toEqual([['k', 'L+null', 100]])
		// Revived as after a failed send of that result: the deletion of L's entry waited for the commit, so the replay
		// finds it again.
		const revived = await h.revive(first)
		await closing(revived)
		expect(h.output('out').map(o => [o.key, o.value, o.timestamp])).toEqual([
			['k', 'L+null', 100],
			['k', 'L+null', 100],
		])
		// Once committed, L's entry is gone: a restarted task forwards it no more (O, added by both incarnations, closes
		// twice: at-least-once, as in Kafka Streams' list store).
		expect(await h.commit('revocation')).toBe('committed')
		await revived.suspend()
		expect(await revived.close('clean')).toBe('clean')
		const restarted = await h.restart()
		h.enqueue(restarted, 'l', [{ key: 'last', value: 'Z', timestamp: 300 }])
		await h.process(restarted)
		expect(h.output('out').map(o => o.value)).toEqual(['L+null', 'L+null', 'O+null', 'O+null'])
	})
})
