import { describe, expect, it } from 'vitest'
import { RateTracker } from '../../src/rate.js'

describe('rate-based lag estimate', () => {
	it('needs two samples before estimating', () => {
		const rates = new RateTracker()
		rates.observe('t 0', 100n, 0)
		expect(rates.estimate('t 0', 50n, 0)).toBeNull()
		rates.observe('t 0', 400n, 30_000)
		// 300 records in 30 s = 10/s; 350 behind = 35 s
		expect(rates.estimate('t 0', 50n, 30_000)).toBe(35)
	})

	it('uses the whole window, not just the last two samples', () => {
		const rates = new RateTracker()
		rates.observe('t 0', 0n, 0)
		rates.observe('t 0', 100n, 30_000)
		rates.observe('t 0', 100n, 60_000) // one quiet interval
		rates.observe('t 0', 300n, 90_000)
		// 300 records over 90 s = 3.33/s; 300 behind = 90 s
		expect(rates.estimate('t 0', 0n, 90_000)).toBeCloseTo(90)
	})

	it('reports zero when caught up and null when the partition is idle', () => {
		const rates = new RateTracker({ minMessagesPerSecond: 1 })
		rates.observe('t 0', 100n, 0)
		rates.observe('t 0', 100n, 30_000)
		expect(rates.estimate('t 0', 100n, 30_000)).toBeNull()
		rates.observe('t 0', 200n, 60_000)
		expect(rates.estimate('t 0', 200n, 60_000)).toBe(0)
	})

	it('ignores samples older than the window and restarts after a truncated log', () => {
		const rates = new RateTracker({ maxAgeMs: 100_000 })
		rates.observe('t 0', 0n, 0)
		rates.observe('t 0', 1_000n, 30_000)
		rates.observe('t 0', 1_100n, 120_000)
		// The sample at 0 s is older than the window; 30 s → 120 s is 100 records in 90 s
		expect(rates.estimate('t 0', 1_000n, 120_000)).toBeCloseTo(90)

		rates.observe('t 0', 5n, 150_000) // high watermark went backwards: topic recreated
		expect(rates.estimate('t 0', 0n, 150_000)).toBeNull()
	})

	it('forgets partitions no group points at', () => {
		const rates = new RateTracker()
		rates.observe('gone 0', 0n, 0)
		rates.observe('gone 0', 10n, 1_000)
		rates.retain(new Set(['kept 0']))
		expect(rates.estimate('gone 0', 0n, 1_000)).toBeNull()
	})
})
