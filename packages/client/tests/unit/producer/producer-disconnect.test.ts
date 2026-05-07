import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('Producer.disconnect()', () => {
	function createMockCluster() {
		const c = {
			getLeaderForPartition: vi.fn(),
			refreshMetadata: vi.fn().mockResolvedValue({
				clusterId: 'test',
				controllerId: 1,
				brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
				topics: new Map([
					['test-topic', { name: 'test-topic', partitions: new Map([[0, { partitionIndex: 0, leaderId: 1 }]]) }],
				]),
				updatedAt: Date.now(),
			}),
			getCoordinator: vi.fn(),
			invalidateCoordinator: vi.fn(),
			getAnyBroker: vi.fn(),
			getLogger: vi.fn().mockReturnValue({
				child: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
				debug: vi.fn(),
				info: vi.fn(),
				warn: vi.fn(),
				error: vi.fn(),
			}),
		}
		return c as unknown as Cluster & {
			getLeaderForPartition: ReturnType<typeof vi.fn>
			refreshMetadata: ReturnType<typeof vi.fn>
		}
	}
	function createMockBroker() {
		return {
			nodeId: 1,
			host: 'broker.test',
			port: 9092,
			isConnected: true,
			produce: vi.fn(),
			getApiVersion: vi.fn().mockReturnValue(9),
		} as unknown as Broker & { produce: ReturnType<typeof vi.fn> }
	}

	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker()
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('rejects orphaned message promises if flush() throws during disconnect', async () => {
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 0,
		})

		// Simulate broker.produce never resolving (broker hung).
		// flush() will eventually time out via the producer's request strategy,
		// or the test will timeout. To avoid that, simulate a throw from
		// broker.produce immediately.
		mockBroker.produce.mockRejectedValue(new Error('broker exploded'))

		// Send (ignore the promise)
		const sendPromise = producer.send('test-topic', { value: Buffer.from('m1') })
		// Suppress unhandled rejection
		sendPromise.catch(() => {})

		// Disconnect should not hang the caller's send() promise.
		await producer.disconnect()

		// The send() promise must be settled (rejected) — not left hanging.
		await expect(sendPromise).rejects.toThrow()
	})

})
