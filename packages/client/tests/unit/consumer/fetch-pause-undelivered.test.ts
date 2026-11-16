import { describe, expect, it } from 'vitest'

import type { PartitionBatch } from '@/consumer/types.js'
import { fetchManagerHarness, fetchResponse, offsetsOf, recordBatch, TOPIC, tp } from './fetch-harness.js'

/**
 * A run loop's queue of drained batches, reported to the FetchManager as undelivered. `deliver` hands the
 * oldest batch to its "handler" the way the consumer's run loop does: dropped unless still deliverable.
 */
function runLoopQueue() {
	const pending: PartitionBatch[] = []
	const delivered: string[] = []
	return {
		pending,
		delivered,
		undelivered: () => pending,
		deliver(isBatchAssigned: (batch: PartitionBatch) => boolean): void {
			const batch = pending.shift()!
			if (!isBatchAssigned(batch)) return
			delivered.push(...offsetsOf(batch).map(offset => `${batch.partition}:${offset}`))
		},
	}
}

describe('FetchManager pause and seek with drained, undelivered batches', () => {
	it('withholds the batch of a partition paused while it waits, and delivers it once after resume', async () => {
		const queue = runLoopQueue()
		const { fm, fetch, requestedOffset } = await fetchManagerHarness({ config: { undelivered: queue.undelivered } })
		fm.addPartitions([
			{ ...tp(0), offset: 0n },
			{ ...tp(1), offset: 0n },
		])
		await fetch(
			fetchResponse(
				{ partition: 0, records: recordBatch(0n, 2), highWatermark: 2n },
				{ partition: 1, records: recordBatch(0n, 2), highWatermark: 4n }
			),
			0,
			1
		)
		queue.pending.push(...(await fm.poll()))
		expect(queue.pending.map(batch => batch.partition)).toEqual([0, 1])

		// The handler of partition 0 runs first and pauses partition 1, whose batch still waits for a slot.
		const isBatchAssigned = (batch: PartitionBatch) => fm.isBatchAssigned(batch)
		queue.deliver(isBatchAssigned)
		fm.pausePartitions([tp(1)])
		expect(fm.getFetchPosition(TOPIC, 1)).toBe(0n)
		expect(fm.nextOffset(TOPIC, 1)).toBe(0n)
		queue.deliver(isBatchAssigned)
		expect(queue.delivered).toEqual(['0:0', '0:1'])

		// After resume, the records are fetched again from the first one that was withheld.
		fm.resumePartitions([tp(1)])
		await fetch(fetchResponse({ partition: 1, records: recordBatch(0n, 4), highWatermark: 4n }), 1)
		expect(requestedOffset(1)).toBe(0n)
		queue.pending.push(...(await fm.poll()))
		queue.deliver(isBatchAssigned)
		expect(queue.delivered).toEqual(['0:0', '0:1', '1:0', '1:1', '1:2', '1:3'])
	})

	it('rewinds to the waiting batch rather than to the records buffered after it', async () => {
		const queue = runLoopQueue()
		const { fm, fetch } = await fetchManagerHarness({
			config: { undelivered: queue.undelivered, maxBytesPerPartition: 150 },
		})
		fm.addPartitions([{ ...tp(0), offset: 10n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(10n, 4), highWatermark: 14n }), 0)
		queue.pending.push(...(await fm.poll()))
		expect(offsetsOf(queue.pending[0]!)).toEqual([10n, 11n])

		fm.pausePartitions([tp(0)])

		expect(fm.getFetchPosition(TOPIC, 0)).toBe(10n)
		expect(fm.isBatchAssigned(queue.pending[0]!)).toBe(false)
		expect(fm.partitionMetrics()[0]!.bufferedRecords).toBe(0)
	})

	it('does not deliver the batch when the partition is paused and resumed before the batch gets a slot', async () => {
		const queue = runLoopQueue()
		const { fm, fetch } = await fetchManagerHarness({ config: { undelivered: queue.undelivered } })
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(0n, 2), highWatermark: 2n }), 0)
		queue.pending.push(...(await fm.poll()))

		fm.pausePartitions([tp(0)])
		fm.resumePartitions([tp(0)])
		queue.deliver(batch => fm.isBatchAssigned(batch))

		// Its records are fetched again from 0, so delivering the batch too would duplicate them.
		expect(queue.delivered).toEqual([])
		expect(fm.getFetchPosition(TOPIC, 0)).toBe(0n)
	})

	it('keeps the batches of partitions that are not paused', async () => {
		const queue = runLoopQueue()
		const { fm, fetch } = await fetchManagerHarness({ config: { undelivered: queue.undelivered } })
		fm.addPartitions([
			{ ...tp(0), offset: 0n },
			{ ...tp(1), offset: 0n },
		])
		await fetch(
			fetchResponse(
				{ partition: 0, records: recordBatch(0n, 1), highWatermark: 1n },
				{ partition: 1, records: recordBatch(0n, 1), highWatermark: 1n }
			),
			0,
			1
		)
		queue.pending.push(...(await fm.poll()))

		fm.pausePartitions([tp(1)])

		expect(fm.isBatchAssigned(queue.pending[0]!)).toBe(true)
		expect(fm.isBatchAssigned(queue.pending[1]!)).toBe(false)
		expect(fm.getFetchPosition(TOPIC, 0)).toBe(1n)
	})

	it('drops a waiting batch on seek and restarts delivery at the seek offset', async () => {
		const queue = runLoopQueue()
		const { fm, fetch, requestedOffset } = await fetchManagerHarness({ config: { undelivered: queue.undelivered } })
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(0n, 3), highWatermark: 20n }), 0)
		queue.pending.push(...(await fm.poll()))

		fm.seekPartition(TOPIC, 0, 10n)

		expect(fm.isBatchAssigned(queue.pending[0]!)).toBe(false)
		expect(fm.nextOffset(TOPIC, 0)).toBe(10n)
		queue.deliver(batch => fm.isBatchAssigned(batch))
		await fetch(fetchResponse({ partition: 0, records: recordBatch(10n, 2), highWatermark: 20n }), 0)
		expect(requestedOffset(0)).toBe(10n)
		queue.pending.push(...(await fm.poll()))
		queue.deliver(batch => fm.isBatchAssigned(batch))
		expect(queue.delivered).toEqual(['0:10', '0:11'])
	})

	it('leaves a drained batch deliverable when the caller reports no undelivered batches', async () => {
		const { fm, fetch } = await fetchManagerHarness()
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(0n, 2), highWatermark: 2n }), 0)
		const [batch] = await fm.poll()

		fm.pausePartitions([tp(0)])

		// Nothing tells the manager it is undelivered, so it neither rewinds over it nor voids it.
		expect(fm.isBatchAssigned(batch!)).toBe(true)
		expect(fm.getFetchPosition(TOPIC, 0)).toBe(2n)
	})
})
