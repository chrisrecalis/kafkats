import { describe, expect, it, vi } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { aggregationRetentionMs, joinRetentionMs } from '@/dsl/retention.js'
import { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'

const DAY_MS = 86_400_000

// The legacy windows below warn that they are deprecated.
vi.spyOn(process, 'emitWarning').mockImplementation(() => undefined)

describe('retention model (6.2)', () => {
	it.each([
		['time windows: size + grace', TimeWindows.ofSizeAndGrace(1000, 200), 1200],
		[
			'hopping windows: size + grace, whatever the advance',
			TimeWindows.ofSizeWithNoGrace(1000).advanceBy(100),
			1000,
		],
		['sessions: gap + grace', SessionWindows.ofInactivityGapAndGrace(300, 50), 350],
		['sliding windows: twice the time difference + grace', SlidingWindows.ofTimeDifferenceAndGrace(100, 7), 207],
		['legacy time windows: a day', TimeWindows.of(1000), DAY_MS],
		['legacy sessions: a day', SessionWindows.withInactivityGap('1m'), DAY_MS],
		['sessions of gap and grace 0: 0', SessionWindows.ofInactivityGapWithNoGrace(0), 0],
	])('default retention of an aggregation store, %s', (_, windows, expected) => {
		expect(aggregationRetentionMs('store', windows, undefined)).toBe(expected)
	})

	it('Materialized.retention keeps windows longer, in any duration unit', () => {
		const windows = TimeWindows.ofSizeAndGrace('1s', '1s')
		expect(aggregationRetentionMs('store', windows, 2000)).toBe(2000)
		expect(aggregationRetentionMs('store', windows, '1h')).toBe(3_600_000)
		expect(aggregationRetentionMs('store', SlidingWindows.ofTimeDifferenceWithNoGrace(10), '20ms')).toBe(20)
	})

	it.each([
		[
			TimeWindows.ofSizeAndGrace(1000, 200),
			1199,
			"The retention of state store 'counts' must be no smaller than the window size (1000 ms) plus the grace " +
				'period (200 ms), got 1199 ms',
		],
		[
			SessionWindows.ofInactivityGapAndGrace(300, 50),
			'0.3s',
			"The retention of state store 'counts' must be no smaller than the inactivity gap (300 ms) plus the grace " +
				'period (50 ms), got 300 ms',
		],
		[
			SlidingWindows.ofTimeDifferenceAndGrace(100, 7),
			206,
			"The retention of state store 'counts' must be no smaller than twice the time difference (100 ms) plus the " +
				'grace period (7 ms), got 206 ms',
		],
	] as const)('a retention below the minimum fails the topology (%#)', (windows, retention, message) => {
		expect(() => aggregationRetentionMs('counts', windows, retention)).toThrow(new TopologyError(message))
	})

	it('a negative or malformed retention fails the topology, naming the store', () => {
		const windows = TimeWindows.ofSizeWithNoGrace(10)
		expect(() => aggregationRetentionMs('counts', windows, -1)).toThrow(
			"The retention of state store 'counts' must not be negative, got -1"
		)
		expect(() => aggregationRetentionMs('counts', windows, '10 days' as never)).toThrow(TopologyError)
	})

	it.each([
		['before + after + grace', JoinWindows.ofTimeDifferenceAndGrace(100, 20), 220],
		['asymmetric windows', JoinWindows.ofTimeDifferenceWithNoGrace(100).before(30).after(-10), 20],
		['legacy windows: a day', JoinWindows.of(1000), DAY_MS],
	])('join buffers keep %s', (_, windows, expected) => {
		expect(joinRetentionMs(windows)).toBe(expected)
	})
})
