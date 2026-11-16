import { describe, expect, it } from 'vitest'

import type { PartitionBatch } from '@/consumer/types.js'
import { controlBatch, fetchManagerHarness, fetchResponse, offsetsOf, recordBatch, TOPIC, tp } from './fetch-harness.js'

describe('FetchManager next delivery offset and lag', () => {
	it('is the first buffered record, then the fetch position once the records are handed out', async () => {
		const { fm, fetch } = await fetchManagerHarness()
		fm.addPartitions([{ ...tp(0), offset: 5n }])
		expect(fm.nextOffset(TOPIC, 0)).toBe(5n)

		await fetch(fetchResponse({ partition: 0, records: recordBatch(5n, 3), highWatermark: 10n }), 0)
		expect(fm.getFetchPosition(TOPIC, 0)).toBe(8n)
		expect(fm.nextOffset(TOPIC, 0)).toBe(5n)
		expect(fm.currentLag(TOPIC, 0)).toBe(5n)

		// Handed out with no undelivered batches reported: delivered.
		expect(offsetsOf((await fm.poll())[0]!)).toEqual([5n, 6n, 7n])
		expect(fm.nextOffset(TOPIC, 0)).toBe(8n)
		expect(fm.currentLag(TOPIC, 0)).toBe(2n)
	})

	it('counts a drained batch that is not delivered yet until the caller hands it to a handler', async () => {
		const pending: PartitionBatch[] = []
		const { fm, fetch } = await fetchManagerHarness({ config: { undelivered: () => pending } })
		fm.addPartitions([{ ...tp(0), offset: 0n }])

		await fetch(fetchResponse({ partition: 0, records: recordBatch(0n, 4), highWatermark: 4n }), 0)
		// Waiting for a slot (another partition's handler runs) or for a pending rebalance.
		pending.push(...(await fm.poll()))
		expect(fm.nextOffset(TOPIC, 0)).toBe(0n)
		expect(fm.currentLag(TOPIC, 0)).toBe(4n)

		pending.length = 0 // its handler is invoked
		expect(fm.nextOffset(TOPIC, 0)).toBe(4n)
		expect(fm.currentLag(TOPIC, 0)).toBe(0n)
	})

	it('moves past a control batch that delivers nothing', async () => {
		const { fm, fetch } = await fetchManagerHarness()
		fm.addPartitions([{ ...tp(0), offset: 10n }])

		await fetch(fetchResponse({ partition: 0, records: controlBatch(10n), highWatermark: 11n }), 0)

		expect(fm.nextOffset(TOPIC, 0)).toBe(11n)
		expect(fm.currentLag(TOPIC, 0)).toBe(0n)
	})

	it('measures the lag against the last stable offset under read_committed, and never below 0', async () => {
		const { fm, fetch } = await fetchManagerHarness({ config: { isolationLevel: 'read_committed' } })
		fm.addPartitions([{ ...tp(0), offset: 0n }])

		await fetch(
			fetchResponse({ partition: 0, records: recordBatch(0n, 2), highWatermark: 20n, lastStableOffset: 12n }),
			0
		)
		expect(fm.currentLag(TOPIC, 0)).toBe(12n)

		await fm.poll()
		fm.seekPartition(TOPIC, 0, 15n)
		expect(fm.currentLag(TOPIC, 0)).toBe(0n)
	})

	it('has no lag before the first response, while paused, and after a resume until the next response', async () => {
		const { fm, fetch } = await fetchManagerHarness()
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		expect(fm.currentLag(TOPIC, 0)).toBeUndefined()

		await fetch(fetchResponse({ partition: 0, highWatermark: 3n }), 0)
		expect(fm.currentLag(TOPIC, 0)).toBe(3n)

		fm.pausePartitions([tp(0)])
		expect(fm.currentLag(TOPIC, 0)).toBeUndefined()
		fm.resumePartitions([tp(0)])
		expect(fm.currentLag(TOPIC, 0)).toBeUndefined()

		await fetch(fetchResponse({ partition: 0, highWatermark: 3n }), 0)
		expect(fm.currentLag(TOPIC, 0)).toBe(3n)
	})

	it('is undefined for a partition that is not assigned', async () => {
		const { fm } = await fetchManagerHarness()
		expect(fm.nextOffset(TOPIC, 0)).toBeUndefined()
		expect(fm.currentLag(TOPIC, 0)).toBeUndefined()
	})

	it('restarts at the position of a partition removed and re-added, ignoring batches drained before', async () => {
		const pending: PartitionBatch[] = []
		const { fm, fetch } = await fetchManagerHarness({ config: { undelivered: () => pending } })
		fm.addPartitions([{ ...tp(0), offset: 5n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(5n, 3), highWatermark: 8n }), 0)
		pending.push(...(await fm.poll()))

		// Eager rebalance: the partition comes back at its committed offset; the drained batch is stale.
		fm.removePartitions([tp(0)])
		fm.addPartitions([{ ...tp(0), offset: 6n }])

		expect(fm.isBatchAssigned(pending[0]!)).toBe(false)
		expect(fm.nextOffset(TOPIC, 0)).toBe(6n)
	})

	it('reports the next delivery offset and buffered records in the partition metrics', async () => {
		const pending: PartitionBatch[] = []
		const { fm, fetch } = await fetchManagerHarness({
			config: { undelivered: () => pending, maxBytesPerPartition: 150 },
		})
		fm.addPartitions([
			{ ...tp(0), offset: 0n },
			{ ...tp(1), offset: 7n },
		])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(0n, 4), highWatermark: 4n }), 0)

		// A batch takes about 150 bytes: two records, two stay buffered.
		pending.push(...(await fm.poll()))
		expect(offsetsOf(pending[0]!)).toEqual([0n, 1n])

		// Records are estimated at 66 bytes each (2-byte value plus 64).
		expect(fm.partitionMetrics()).toMatchObject([
			{ partition: 0, nextOffset: 0n, position: 4n, bufferedRecords: 2, bufferedBytes: 132 },
			{ partition: 1, nextOffset: 7n, position: 7n, bufferedRecords: 0, bufferedBytes: 0 },
		])

		pending.length = 0
		expect(fm.partitionMetrics()[0]!.nextOffset).toBe(2n)
	})
})
