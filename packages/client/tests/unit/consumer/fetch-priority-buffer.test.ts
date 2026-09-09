import { describe, expect, it, vi } from 'vitest'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { strict, type PriorityStrategy } from '@/consumer/priority.js'

function makeFetchManager(
	maxRecords = 3,
	priority: PriorityStrategy = strict({ order: ['live', 'bulk'] })
): FetchManager {
	const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g')
	return new FetchManager(cluster as never, offsetManager, 'earliest', {
		maxBytesPerPartition: 1024,
		maxRecords,
		minBytes: 1,
		maxWaitMs: 1,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
		maxBufferedBytes: 100,
		priority,
	})
}

function completedFetch(topic: string, partition: number, offsets: bigint[], byteSize: number) {
	return {
		topic,
		partition,
		records: offsets.map(offset => ({ offset })),
		byteSize,
		assignmentEpoch: 0,
	}
}

describe('FetchManager priority buffer', () => {
	it('drains priority order, rotates partial partitions, and retains excluded records', async () => {
		const manager = makeFetchManager()
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'live', partition: 1, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer
		buffer.add(completedFetch('bulk', 0, [10n, 11n], 20))
		buffer.add(completedFetch('live', 0, [0n, 1n, 2n, 3n], 60))
		buffer.add(completedFetch('live', 1, [5n, 6n], 20))

		const first = await manager.poll()
		expect(first[0]).toMatchObject({ topic: 'live', partition: 0 })
		expect(first[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])
		expect(buffer.remainingCapacity()).toBe(45)

		const second = await manager.poll(new Set(['live:0']))
		expect(second[0]).toMatchObject({ topic: 'live', partition: 1 })
		expect(second.flatMap(batch => batch.records.map(record => record.offset))).toEqual([5n, 6n, 10n])

		const third = await manager.poll()
		expect(third.flatMap(batch => batch.records.map(record => record.offset))).toEqual([3n, 11n])
		manager.stop()
	})

	it('delivers partitions the plan omitted FIFO within the remaining budget', async () => {
		// Plan lists live:0 but caps it at zero; live:0 must stay withheld, not fall through to FIFO.
		const manager = makeFetchManager(4, {
			schedule: () => ({ drain: [{ topic: 'live', partition: 0, maxRecords: 0 }], fetch: [] }),
		})
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer
		buffer.add(completedFetch('bulk', 0, [10n, 11n], 20))
		buffer.add(completedFetch('live', 0, [0n, 1n], 20))
		buffer.add(completedFetch('bulk', 0, [12n, 13n, 14n], 30))

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const drained = (manager as any).drainBuffer()
		expect(drained).not.toBeInstanceOf(Promise)
		const offsets = (batches: Array<{ records: Array<{ offset: bigint }> }>) =>
			batches.flatMap(batch => batch.records.map(record => record.offset))
		// All queue entries for the omitted partition are walked, not just the first.
		expect(offsets(drained)).toEqual([10n, 11n, 12n, 13n])
		expect(offsets(await manager.poll())).toEqual([14n])
		manager.stop()
	})
})
