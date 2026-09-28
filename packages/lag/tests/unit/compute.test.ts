import { describe, expect, it } from 'vitest'
import { computePartitionLag, fetchOffsetFor } from '../../src/compute.js'

describe('lag calculation', () => {
	it('uses the committed record timestamp for time lag', () => {
		expect(
			computePartitionLag({
				committedOffset: 4n,
				latestOffset: 10n,
				record: { offset: 4n, timestamp: 100_000n },
				now: 400_000,
			})
		).toBe(300)
	})

	it('reports zero time lag without fetching when caught up', () => {
		expect(fetchOffsetFor(10n, 0n, 10n)).toBeNull()
		expect(computePartitionLag({ committedOffset: 10n, latestOffset: 10n, record: undefined, now: 400_000 })).toBe(
			0
		)
	})

	it('reports unknown lag, not zero, when the committed offset is beyond the log end', () => {
		expect(fetchOffsetFor(12n, 0n, 10n)).toBeNull()
		expect(
			computePartitionLag({ committedOffset: 12n, latestOffset: 10n, record: undefined, now: 400_000 })
		).toBeNull()
	})

	it('reports zero lag when nothing readable remains before the log end', () => {
		// Only transaction markers / aborted records between the commit and the log end
		expect(computePartitionLag({ committedOffset: 4n, latestOffset: 10n, record: null, now: 400_000 })).toBe(0)
	})

	it('reports unknown lag when the fetch failed or the record has no timestamp', () => {
		expect(
			computePartitionLag({ committedOffset: 4n, latestOffset: 10n, record: undefined, now: 400_000 })
		).toBeNull()
		expect(
			computePartitionLag({
				committedOffset: 4n,
				latestOffset: 10n,
				record: { offset: 4n, timestamp: -1n },
				now: 400_000,
			})
		).toBeNull()
	})

	it('measures from the oldest surviving record when the committed one was deleted by retention', () => {
		expect(fetchOffsetFor(2n, 5n, 10n)).toBe(5n)
		expect(
			computePartitionLag({
				committedOffset: 2n,
				latestOffset: 10n,
				record: { offset: 5n, timestamp: 100_000n },
				now: 400_000,
			})
		).toBe(300)
	})
})
