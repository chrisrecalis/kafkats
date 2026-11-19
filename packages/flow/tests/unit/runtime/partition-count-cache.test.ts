import { describe, expect, it, vi } from 'vitest'
import { SinkPartitionCounts } from '@/runtime/partition-count-cache.js'
import { ManualClock } from '../support/manual-clock.js'
import { RecordingLogger } from '../support/task-harness.js'

function cache(partitionCount: (topic: string) => Promise<number>) {
	const clock = new ManualClock(0)
	const logger = new RecordingLogger()
	const lookups = vi.fn(partitionCount)
	const counts = new SinkPartitionCounts({
		producer: () => ({ partitionCount: lookups }),
		clock,
		signal: new AbortController().signal,
		logger,
	})
	return { clock, logger, lookups, counts }
}

describe('SinkPartitionCounts', () => {
	it('answers synchronously once loaded; unknown topics are undefined', async () => {
		const { counts } = cache(async () => 3)
		expect(counts.get('out')).toBeUndefined()
		await expect(counts.load('out', AbortSignal.timeout(1000))).resolves.toBe(3)
		expect(counts.get('out')).toBe(3)
	})

	it('refreshes an entry older than 60 s in the background, one refresh at a time (B13)', async () => {
		let count = 3
		const { clock, lookups, counts } = cache(async () => count)
		await counts.load('out', AbortSignal.timeout(1000))
		count = 6
		clock.advance(60_000)
		expect(counts.get('out')).toBe(3)
		expect(counts.get('out')).toBe(3)
		await vi.waitFor(() => expect(counts.get('out')).toBe(6))
		expect(lookups).toHaveBeenCalledTimes(2)
	})

	it('keeps the last count when a refresh fails', async () => {
		let fail = false
		const { clock, logger, counts } = cache(async () => {
			if (fail) throw new Error('metadata unavailable')
			return 3
		})
		await counts.load('out', AbortSignal.timeout(1000))
		fail = true
		clock.advance(60_000)
		expect(counts.get('out')).toBe(3)
		await vi.waitFor(() => expect(logger.warnings).toHaveLength(1))
		expect(counts.get('out')).toBe(3)
	})
})
