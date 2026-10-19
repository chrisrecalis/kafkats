import { describe, expect, it, vi } from 'vitest'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ConnectionClosedError } from '@/network/errors.js'

function createManager(cluster: object) {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g')
	return new FetchManager(cluster as never, offsetManager, 'earliest', {
		maxBytesPerPartition: 1024,
		maxRecords: 10,
		minBytes: 1,
		maxWaitMs: 1,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
		maxBufferedBytes: 10_240,
	})
}

const pending = () => new Promise<never>(() => {})

describe('FetchManager broker reconnect', () => {
	it('re-resolves the leader after a fetch to a disconnected broker fails', async () => {
		const closedBroker = {
			nodeId: 1,
			fetch: vi.fn().mockRejectedValue(new ConnectionClosedError('Cannot send on disconnected connection')),
		}
		const reconnectedBroker = { nodeId: 1, fetch: vi.fn(pending) }
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValueOnce(closedBroker).mockResolvedValue(reconnectedBroker),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
			getLogger: () => null,
		}
		const manager = createManager(cluster)
		manager.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

		await manager.poll()

		await vi.waitFor(() => expect(reconnectedBroker.fetch).toHaveBeenCalled(), { timeout: 1000 })
		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['t'])
		manager.stop()
	})

	it('waits before retrying a leader lookup that failed', async () => {
		const healthyBroker = { nodeId: 2, fetch: vi.fn(pending) }
		const cluster = {
			getLeaderForPartition: vi.fn(async (topic: string, partition: number) => {
				if (partition === 0) throw new ConnectionClosedError('connect ECONNREFUSED')
				return healthyBroker
			}),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
			getLogger: () => null,
		}
		const manager = createManager(cluster)
		manager.addPartitions([
			{ topic: 't', partition: 0, offset: 0n },
			{ topic: 't', partition: 1, offset: 0n },
		])

		await manager.poll()
		await new Promise(resolve => setTimeout(resolve, 300))
		manager.stop()

		const failedLookups = cluster.getLeaderForPartition.mock.calls.filter(([, partition]) => partition === 0)
		expect(failedLookups.length).toBeGreaterThan(0)
		expect(failedLookups.length).toBeLessThanOrEqual(5)
	})
})
