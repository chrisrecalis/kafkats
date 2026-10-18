import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'

function makeFetchManager(maxBufferedBytes = 1000, maxBytesPerPartition = 1000): FetchManager {
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
			maxBytesPerPartition,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			maxBufferedBytes,
		}
	)
}

// A 36-byte value plus the 64-byte per-record overhead: each record is estimated at 100 bytes.
function record(offset: bigint, valueBytes = 36) {
	return { offset, key: null, value: Buffer.alloc(valueBytes) }
}

function completedFetch(topic: string, partition: number, offsets: bigint[], assignmentEpoch = 0) {
	return { topic, partition, records: offsets.map(offset => record(offset)), assignmentEpoch }
}

describe('FetchManager bounded prefetch', () => {
	it('rotates a partially drained partition behind other buffered partitions', async () => {
		const manager = makeFetchManager(1000, 300)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n, 1n, 2n, 3n]))
		buffer.add(completedFetch('topic-b', 0, [5n, 6n]))

		const first = await manager.poll()
		expect(first.flatMap(batch => batch.records.map(record => record.offset))).toEqual([0n, 1n, 2n])
		expect(buffer.remainingCapacity()).toBe(700)

		const second = await manager.poll()
		expect(second.flatMap(batch => batch.records.map(record => record.offset))).toEqual([5n, 6n, 3n])
		expect(buffer.remainingCapacity()).toBe(1000)

		manager.stop()
	})

	it('caps each partition batch at maxBytesPerPartition across coalesced prefetches', async () => {
		const manager = makeFetchManager(1000, 300)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		// Two prefetched responses for one partition hold 400 bytes; one batch may carry only 300.
		buffer.add(completedFetch('topic-a', 0, [0n, 1n]))
		buffer.add(completedFetch('topic-a', 0, [2n, 3n]))

		const first = await manager.poll()
		expect(first).toHaveLength(1)
		expect(first[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])

		const second = await manager.poll()
		expect(second[0]!.records.map(record => record.offset)).toEqual([3n])
		expect(buffer.remainingCapacity()).toBe(1000)

		manager.stop()
	})

	it('keeps partition order when a batch ends inside one of several buffered responses', async () => {
		const manager = makeFetchManager(1000, 300)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n, 1n]))
		buffer.add(completedFetch('topic-a', 0, [2n, 3n]))
		buffer.add(completedFetch('topic-a', 0, [4n, 5n]))

		// The first batch ends inside the second response; its remainder must precede the third.
		expect((await manager.poll())[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])
		expect((await manager.poll())[0]!.records.map(record => record.offset)).toEqual([3n, 4n, 5n])

		manager.stop()
	})

	it('delivers a record larger than maxBytesPerPartition on its own', async () => {
		const manager = makeFetchManager(2000, 300)
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add({ topic: 'topic-a', partition: 0, records: [record(0n, 500), record(1n)], assignmentEpoch: 0 })

		expect((await manager.poll())[0]!.records.map(record => record.offset)).toEqual([0n])
		expect((await manager.poll())[0]!.records.map(record => record.offset)).toEqual([1n])

		manager.stop()
	})

	it('leaves active partitions buffered while draining records for idle partitions', async () => {
		const manager = makeFetchManager()
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n, 1n, 2n]))
		buffer.add(completedFetch('topic-a', 1, [3n, 4n]))

		const idlePartition = await manager.poll(new Set(['topic-a:0']))
		expect(idlePartition).toHaveLength(1)
		expect(idlePartition[0]).toMatchObject({ topic: 'topic-a', partition: 1 })
		expect(idlePartition[0]!.records.map(record => record.offset)).toEqual([3n, 4n])

		const previouslyActivePartition = await manager.poll()
		expect(previouslyActivePartition).toHaveLength(1)
		expect(previouslyActivePartition[0]).toMatchObject({ topic: 'topic-a', partition: 0 })
		expect(previouslyActivePartition[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])

		manager.stop()
	})

	it('coalesces prefetched responses for the same partition and assignment epoch', async () => {
		const manager = makeFetchManager()
		// Lazily create the real FetchBuffer without assigning partitions (so the
		// background loop cannot race the records injected below).
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer

		buffer.add(completedFetch('topic-a', 0, [0n]))
		buffer.add(completedFetch('topic-b', 0, [5n]))
		buffer.add(completedFetch('topic-a', 0, [1n, 2n]))

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

		buffer.add(completedFetch('topic-a', 0, [0n], 0))
		buffer.add(completedFetch('topic-a', 0, [1n], 1))

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
		managerAny.fetchBuffer.add(completedFetch('topic-a', 0, [0n]))

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
