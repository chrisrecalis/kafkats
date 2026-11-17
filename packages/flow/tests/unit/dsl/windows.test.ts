import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { durationMs, JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'

const DAY = 86_400_000

describe('durationMs', () => {
	it.each([
		[250, 250],
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

	it.each([-1, 1.5, '0.0001s', '5 s', '5w'])('rejects %s', duration => {
		expect(() => durationMs(duration as never)).toThrow(TopologyError)
	})
})

describe('windows', () => {
	it('time windows: tumbling or hopping, with an explicit grace or the deprecated 24 h one', () => {
		expect(TimeWindows.ofSizeWithNoGrace('10s')).toMatchObject({ sizeMs: 10_000, advanceMs: 10_000, graceMs: 0 })
		expect(TimeWindows.ofSizeAndGrace(10, 5).advanceBy(3)).toMatchObject({ sizeMs: 10, advanceMs: 3, graceMs: 5 })
		expect(TimeWindows.of('1h')).toMatchObject({ sizeMs: 3_600_000, graceMs: DAY - 3_600_000 })
		expect(TimeWindows.of('2d').graceMs).toBe(0)
	})

	it('session windows: gap with an explicit grace or the deprecated 24 h one', () => {
		expect(SessionWindows.ofInactivityGapWithNoGrace(100)).toMatchObject({ gapMs: 100, graceMs: 0 })
		expect(SessionWindows.ofInactivityGapAndGrace(100, 50)).toMatchObject({ gapMs: 100, graceMs: 50 })
		expect(SessionWindows.withInactivityGap(100)).toMatchObject({ gapMs: 100, graceMs: DAY - 100 })
	})

	it('sliding windows: time difference and grace, the deprecated forms included', () => {
		expect(SlidingWindows.ofTimeDifferenceWithNoGrace(10)).toMatchObject({ timeDifferenceMs: 10, graceMs: 0 })
		expect(SlidingWindows.ofTimeDifferenceAndGrace(10, 4)).toMatchObject({ timeDifferenceMs: 10, graceMs: 4 })
		expect(SlidingWindows.of(10).gracePeriod(4)).toMatchObject({ timeDifferenceMs: 10, graceMs: 4 })
	})

	it('join windows: symmetric or before/after; only the deprecated factory emits eagerly', () => {
		const windows = JoinWindows.ofTimeDifferenceAndGrace(100, 20).before(10).after(30)
		expect(windows).toMatchObject({ beforeMs: 10, afterMs: 30, graceMs: 20, spuriousResultFix: true })
		expect(JoinWindows.ofTimeDifferenceWithNoGrace(5)).toMatchObject({ beforeMs: 5, afterMs: 5, graceMs: 0 })
		expect(JoinWindows.of(1000)).toMatchObject({ beforeMs: 1000, graceMs: DAY - 2000, spuriousResultFix: false })
	})
})
