import { describe, expect, it, vi } from 'vitest'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { strict } from '@/consumer/priority.js'

describe('FetchManager priority fetch loop', () => {
	it('withholds lower tiers until higher tiers cannot fill a poll', async () => {
		const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g')
		const manager = new FetchManager(cluster as never, offsetManager, 'earliest', {
			maxBytesPerPartition: 1024,
			maxRecords: 2,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			maxBufferedBytes: 100,
			priority: strict({ order: ['live', 'bulk'] }),
		})
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const internal = manager as any
		internal.fetchBuffer.add({
			topic: 'live',
			partition: 0,
			records: [{ offset: 0n }, { offset: 1n }],
			byteSize: 20,
			assignmentEpoch: 0,
		})
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 2n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])

		const grouped: string[][] = []
		internal.groupPartitionsByBroker = vi.fn().mockImplementation(async (partitions: Array<{ topic: string }>) => {
			grouped.push(partitions.map(partition => partition.topic))
			return new Map([[1, { broker: { nodeId: 1 }, partitions }]])
		})
		const releases: Array<() => void> = []
		internal.fetchFromBrokerToBuffer = vi.fn(
			() =>
				new Promise<void>(resolve => {
					releases.push(resolve)
				})
		)

		await vi.waitFor(() => expect(grouped.some(topics => topics.length === 1 && topics[0] === 'live')).toBe(true), {
			timeout: 500,
		})
		await manager.poll()
		releases.shift()?.()
		await vi.waitFor(() => expect(grouped.some(topics => topics.includes('bulk'))).toBe(true), { timeout: 500 })
		manager.stop()
		for (const release of releases) release()
	})
})
