import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'

function makeFetchManager(maxBufferedBytes = 100, maxRecords = 500): FetchManager {
	const cluster = {
		getLeaderForPartition: vi.fn(),
		getLogger: () => null,
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g')
	return new FetchManager(
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster as any,
		offsetManager,
		'earliest',
		{
			maxBytesPerPartition: 1024,
			maxRecords,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			maxBufferedBytes,
		}
	)
}

function completedFetch(topic: string, partition: number, offsets: bigint[], byteSize: number, assignmentEpoch = 0) {
	return {
		topic,
		partition,
		// Only offsets are observed by these buffer-level tests.
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		records: offsets.map(offset => ({ offset })) as any,
		byteSize,
		assignmentEpoch,
	}
}

describe('FetchManager bounded prefetch', () => {
	it('rotates a partially drained partition behind other buffered partitions', async () => {
		const manager = makeFetchManager(100, 3)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n, 1n, 2n, 3n], 60))
		buffer.add(completedFetch('topic-b', 0, [5n, 6n], 40))

		const first = await manager.poll()
		expect(first.flatMap(batch => batch.records.map(record => record.offset))).toEqual([0n, 1n, 2n])
		expect(buffer.remainingCapacity()).toBe(45)

		const second = await manager.poll()
		expect(second.flatMap(batch => batch.records.map(record => record.offset))).toEqual([5n, 6n, 3n])
		expect(buffer.remainingCapacity()).toBe(100)

		manager.stop()
	})

	it('coalesces prefetched responses for the same partition and assignment epoch', async () => {
		const manager = makeFetchManager()
		// Lazily create the real FetchBuffer without assigning partitions (so the
		// background loop cannot race the records injected below).
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n], 10))
		buffer.add(completedFetch('topic-b', 0, [5n], 10))
		buffer.add(completedFetch('topic-a', 0, [1n, 2n], 20))

		const batches = await manager.poll()
		expect(batches).toHaveLength(2)
		expect(batches[0]).toMatchObject({ topic: 'topic-a', partition: 0 })
		expect(batches[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])
		expect(batches[1]).toMatchObject({ topic: 'topic-b', partition: 0 })

		manager.stop()
	})

	it('does not coalesce batches across assignment epochs', async () => {
		const manager = makeFetchManager()
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n], 10, 0))
		buffer.add(completedFetch('topic-a', 0, [1n], 10, 1))

		const batches = await manager.poll()
		expect(batches).toHaveLength(2)
		expect(batches.map(batch => batch.assignmentEpoch)).toEqual([0, 1])

		manager.stop()
	})

	it('does not issue another fetch until a full buffer is drained', async () => {
		const manager = makeFetchManager(100)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const managerAny = manager as any
		managerAny.fetchBuffer.add(completedFetch('topic-a', 0, [0n], 100))

		const broker = { nodeId: 1 }
		let releaseFetch: (() => void) | undefined
		const fetchFromBroker = vi.fn(
			() =>
				new Promise<void>(resolve => {
					releaseFetch = resolve
				})
		)
		managerAny.fetchFromBrokerToBuffer = fetchFromBroker
		managerAny.groupPartitionsByBroker = vi
			.fn()
			.mockImplementation(async (partitions: unknown[]) => new Map([[1, { broker, partitions }]]))
		manager.addPartitions([{ topic: 'topic-a', partition: 0, offset: 1n }])

		// The background loop may still be in its no-partitions backoff from the
		// initialization poll. Once it wakes, the full buffer must park it.
		await new Promise(resolve => setTimeout(resolve, 120))
		expect(fetchFromBroker).not.toHaveBeenCalled()

		await manager.poll()
		await vi.waitFor(() => expect(fetchFromBroker).toHaveBeenCalled(), { timeout: 500 })

		manager.stop()
		releaseFetch?.()
	})

	it('shares the remaining buffer budget across concurrent broker fetches', async () => {
		const manager = makeFetchManager(100)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const managerAny = manager as any
		const releaseFetches: Array<() => void> = []
		const requestBudgets: number[] = []
		managerAny.fetchFromBrokerToBuffer = vi.fn(
			(_broker: unknown, _partitions: unknown[], maxBytes: number) =>
				new Promise<void>(resolve => {
					requestBudgets.push(maxBytes)
					releaseFetches.push(resolve)
				})
		)
		managerAny.groupPartitionsByBroker = vi.fn().mockImplementation(
			async (partitions: unknown[]) =>
				new Map([
					[1, { broker: { nodeId: 1 }, partitions }],
					[2, { broker: { nodeId: 2 }, partitions }],
				])
		)
		manager.addPartitions([{ topic: 'topic-a', partition: 0, offset: 0n }])

		await vi.waitFor(() => expect(requestBudgets).toHaveLength(2), { timeout: 500 })
		expect(requestBudgets).toEqual([50, 50])
		expect(requestBudgets.reduce((sum, bytes) => sum + bytes, 0)).toBe(100)

		manager.stop()
		for (const release of releaseFetches) release()
	})
})
