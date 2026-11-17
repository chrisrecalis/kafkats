import { afterEach, describe, expect, it, vi } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import {
	durationMs,
	JoinWindows,
	joinWindowsWithin,
	SessionWindows,
	SlidingWindows,
	TimeWindows,
} from '@/dsl/windows.js'

type WindowsModule = typeof import('@/dsl/windows.js')

const DAY = 86_400_000

afterEach(() => {
	vi.restoreAllMocks()
})

/** A fresh windows module (nothing warned yet in it) and the warnings it emits. */
async function freshWindows(): Promise<{ windows: WindowsModule; warnings: () => unknown[][] }> {
	vi.resetModules()
	const windows = await import('@/dsl/windows.js')
	const emitWarning = vi.spyOn(process, 'emitWarning').mockImplementation(() => undefined)
	return { windows, warnings: () => emitWarning.mock.calls }
}

describe('durationMs', () => {
	it.each([
		[250, 250],
		[0, 0],
		['250ms', 250],
		['2s', 2000],
		['1.5s', 1500],
		['1.1s', 1100],
		['3m', 180_000],
		['2h', 7_200_000],
		['1d', DAY],
	] as const)('%s is %d ms', (duration, ms) => {
		expect(durationMs(duration)).toBe(ms)
	})

	it.each([-1, '-1s', 1.5, '0.0001s', Number.NaN, Number.POSITIVE_INFINITY, 2 ** 53, '5 s', '5w', '1e3ms'])(
		'B9: rejects %s',
		duration => {
			expect(() => durationMs(duration as never)).toThrow(TopologyError)
		}
	)

	it('names the duration in its errors', () => {
		expect(() => durationMs(-5, 'TimeWindows grace')).toThrow('TimeWindows grace must not be negative, got -5')
		expect(() => durationMs(0.5, 'SessionWindows gap')).toThrow(
			'SessionWindows gap must be a whole number of milliseconds, got 0.5'
		)
		expect(() => durationMs('5w' as never, 'JoinWindows grace')).toThrow("JoinWindows grace '5w' is invalid")
	})
})

describe('windows', () => {
	it('time windows: tumbling, or hopping with advanceBy, with no grace or an explicit one', () => {
		expect(TimeWindows.ofSizeWithNoGrace('10s')).toMatchObject({ sizeMs: 10_000, advanceMs: 10_000, graceMs: 0 })
		expect(TimeWindows.ofSizeAndGrace(10, 5).advanceBy(3)).toMatchObject({ sizeMs: 10, advanceMs: 3, graceMs: 5 })
		expect(TimeWindows.ofSizeWithNoGrace(10).advanceBy(10)).toMatchObject({ advanceMs: 10 })
	})

	it('session windows: an inactivity gap with no grace or an explicit one', () => {
		expect(SessionWindows.ofInactivityGapWithNoGrace(100)).toMatchObject({ gapMs: 100, graceMs: 0 })
		expect(SessionWindows.ofInactivityGapAndGrace('1m', '5s')).toMatchObject({ gapMs: 60_000, graceMs: 5000 })
	})

	it('sliding windows: a time difference with no grace or an explicit one', () => {
		expect(SlidingWindows.ofTimeDifferenceWithNoGrace(10)).toMatchObject({ timeDifferenceMs: 10, graceMs: 0 })
		expect(SlidingWindows.ofTimeDifferenceAndGrace(10, 4)).toMatchObject({ timeDifferenceMs: 10, graceMs: 4 })
	})

	it('join windows: symmetric, or bounded per side with before/after; left/outer results wait for the window to close', () => {
		expect(JoinWindows.ofTimeDifferenceWithNoGrace(5)).toMatchObject({
			beforeMs: 5,
			afterMs: 5,
			graceMs: 0,
			spuriousResultFix: true,
		})
		const windows = JoinWindows.ofTimeDifferenceAndGrace(100, 20).before(10).after('30ms')
		expect(windows).toMatchObject({ beforeMs: 10, afterMs: 30, graceMs: 20, spuriousResultFix: true })
	})
})

describe('validation (Kafka Streams)', () => {
	it('B9: TimeWindows need a positive size, an advance in (0, size] and a non-negative grace', () => {
		expect(() => TimeWindows.ofSizeWithNoGrace(0)).toThrow('TimeWindows size must be positive, got 0 ms')
		expect(() => TimeWindows.ofSizeWithNoGrace(-10)).toThrow(TopologyError)
		expect(() => TimeWindows.of(0)).toThrow('TimeWindows size must be positive, got 0 ms')
		expect(() => TimeWindows.ofSizeAndGrace(10, -1)).toThrow('TimeWindows grace must not be negative, got -1')
		for (const advance of [0, 11]) {
			expect(() => TimeWindows.ofSizeWithNoGrace(10).advanceBy(advance)).toThrow(
				`TimeWindows advance must be positive and at most the window size (10 ms), got ${advance} ms`
			)
		}
		expect(() => TimeWindows.ofSizeWithNoGrace(10).advanceBy(-1)).toThrow(
			'TimeWindows advance must not be negative'
		)
	})

	it('B9: SessionWindows need a non-negative gap and grace; a gap of 0 is valid', () => {
		expect(SessionWindows.ofInactivityGapWithNoGrace(0)).toMatchObject({ gapMs: 0, graceMs: 0 })
		expect(() => SessionWindows.ofInactivityGapWithNoGrace(-1)).toThrow(
			'SessionWindows gap must not be negative, got -1'
		)
		expect(() => SessionWindows.ofInactivityGapAndGrace(10, -1)).toThrow(TopologyError)
		expect(() => SessionWindows.withInactivityGap(-1)).toThrow(TopologyError)
	})

	it('B9: SlidingWindows need a non-negative time difference and grace; a difference of 0 is valid', () => {
		expect(SlidingWindows.ofTimeDifferenceWithNoGrace(0)).toMatchObject({ timeDifferenceMs: 0 })
		expect(() => SlidingWindows.ofTimeDifferenceWithNoGrace(-1)).toThrow(
			'SlidingWindows time difference must not be negative, got -1'
		)
		expect(() => SlidingWindows.ofTimeDifferenceAndGrace(10, -1)).toThrow(TopologyError)
		expect(() => SlidingWindows.ofTimeDifferenceWithNoGrace(10).gracePeriod(-1)).toThrow(TopologyError)
	})

	it('B9: JoinWindows need a non-negative difference and grace; before or after may be negative, their sum not', () => {
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(10)
		// A right record joins a left one at t if it is in [t + 10, t + 10]: exactly 10 later.
		expect(windows.before(-10)).toMatchObject({ beforeMs: -10, afterMs: 10 })
		expect(windows.after('-5ms')).toMatchObject({ beforeMs: 10, afterMs: -5 })
		expect(() => windows.before(-11)).toThrow(
			'JoinWindows before + after must not be negative, got before -11 ms and after 10 ms'
		)
		expect(() => windows.after('-11ms')).toThrow(TopologyError)
		expect(() => windows.before(1.5)).toThrow('JoinWindows before must be a whole number of milliseconds, got 1.5')
		expect(() => JoinWindows.ofTimeDifferenceWithNoGrace(-1)).toThrow(
			'JoinWindows time difference must not be negative, got -1'
		)
		expect(() => JoinWindows.ofTimeDifferenceAndGrace(10, -1)).toThrow(TopologyError)
		expect(() => JoinWindows.of(-1)).toThrow(TopologyError)
	})
})

describe('deprecated factories', () => {
	it("keep Kafka Streams' legacy grace: what is left of a day after the window, at least 0", () => {
		expect(TimeWindows.of('1h')).toMatchObject({
			sizeMs: 3_600_000,
			advanceMs: 3_600_000,
			graceMs: DAY - 3_600_000,
		})
		expect(TimeWindows.of(1000).advanceBy(500)).toMatchObject({ advanceMs: 500, graceMs: DAY - 1000 })
		expect(TimeWindows.of('2d').graceMs).toBe(0)
		expect(SessionWindows.withInactivityGap(100)).toMatchObject({ gapMs: 100, graceMs: DAY - 100 })
		expect(SessionWindows.withInactivityGap('2d').graceMs).toBe(0)
		expect(SlidingWindows.of(10)).toEqual(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
		expect(SlidingWindows.of(10).gracePeriod(4)).toEqual(SlidingWindows.ofTimeDifferenceAndGrace(10, 4))
	})

	it('JoinWindows.of is the legacy one of Kafka Streams: a grace of 24 h minus both sides, eager results', () => {
		expect(JoinWindows.of(1000)).toMatchObject({
			beforeMs: 1000,
			afterMs: 1000,
			graceMs: DAY - 2000,
			spuriousResultFix: false,
		})
		expect(JoinWindows.of(DAY / 2 - 1).graceMs).toBe(2)
		expect(JoinWindows.of(DAY / 2).graceMs).toBe(0)
		expect(JoinWindows.of(1000).before(0).after(500)).toMatchObject({
			beforeMs: 0,
			afterMs: 500,
			spuriousResultFix: false,
		})
	})

	it.each([
		['TimeWindows.of', (w: WindowsModule) => w.TimeWindows.of(10), 'ofSizeAndGrace(size, grace)', 'about a day'],
		[
			'SessionWindows.withInactivityGap',
			(w: WindowsModule) => w.SessionWindows.withInactivityGap(10),
			'ofInactivityGapAndGrace(gap, grace)',
			'about a day',
		],
		[
			'JoinWindows.of (and Joined.within)',
			(w: WindowsModule) => w.JoinWindows.of(10),
			'JoinWindows.ofTimeDifferenceAndGrace(timeDifference, grace)',
			'about a day',
		],
		[
			'SlidingWindows.of',
			(w: WindowsModule) => w.SlidingWindows.of(10),
			'ofTimeDifferenceWithNoGrace(timeDifference)',
			'same windows and retention',
		],
		[
			'SlidingWindows.gracePeriod',
			(w: WindowsModule) => w.SlidingWindows.ofTimeDifferenceWithNoGrace(10).gracePeriod(4),
			'ofTimeDifferenceAndGrace(timeDifference, grace)',
			'same windows and retention',
		],
	])('%s warns once per process, naming its replacement and retention', async (api, use, replacement, retention) => {
		const { windows, warnings } = await freshWindows()
		use(windows)
		use(windows)
		expect(warnings()).toHaveLength(1)
		const [message, type] = warnings()[0]!
		expect(type).toBe('DeprecationWarning')
		expect(message).toContain(`${api} is deprecated: `)
		expect(message).toContain(replacement)
		expect(message).toContain(retention)
	})

	it('the current factories do not warn', async () => {
		const { windows, warnings } = await freshWindows()
		windows.TimeWindows.ofSizeAndGrace(10, 1).advanceBy(5)
		windows.SessionWindows.ofInactivityGapAndGrace(10, 1)
		windows.SlidingWindows.ofTimeDifferenceAndGrace(10, 1)
		windows.JoinWindows.ofTimeDifferenceAndGrace(10, 1).before(0).after(5)
		expect(warnings()).toEqual([])
	})
})

describe('Joined.within', () => {
	it('maps the size of TimeWindows and the gap of SessionWindows to JoinWindows.of; JoinWindows stay as they are', () => {
		const legacy = JoinWindows.of(1000)
		expect(joinWindowsWithin(TimeWindows.ofSizeAndGrace(1000, 5).advanceBy(500), 'join()')).toEqual(legacy)
		expect(joinWindowsWithin(SessionWindows.ofInactivityGapAndGrace(1000, 5), 'join()')).toEqual(legacy)
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(10)
		expect(joinWindowsWithin(windows, 'join()')).toBe(windows)
	})

	it('warns as JoinWindows.of when it maps other windows', async () => {
		const { windows, warnings } = await freshWindows()
		windows.joinWindowsWithin(windows.JoinWindows.ofTimeDifferenceWithNoGrace(10), 'join()')
		expect(warnings()).toEqual([])
		windows.joinWindowsWithin(windows.TimeWindows.ofSizeWithNoGrace(10), 'join()')
		windows.joinWindowsWithin(windows.SessionWindows.ofInactivityGapWithNoGrace(10), 'join()')
		expect(warnings()).toHaveLength(1)
		expect(warnings()[0]![0]).toContain('JoinWindows.of (and Joined.within) is deprecated: ')
	})

	it('rejects SlidingWindows and a missing window, naming the operator', () => {
		expect(() => joinWindowsWithin(SlidingWindows.ofTimeDifferenceWithNoGrace(10), 'leftJoin()')).toThrow(
			'leftJoin() with a stream cannot use SlidingWindows; pass JoinWindows'
		)
		expect(() => joinWindowsWithin(undefined, 'join()')).toThrow(
			'join() with a stream needs JoinWindows as its third argument'
		)
	})
})
