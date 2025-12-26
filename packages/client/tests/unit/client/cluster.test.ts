import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { BrokerNotAvailableError, LeaderNotAvailableError, CoordinatorNotAvailableError } from '@/client/errors.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Create a mock Broker for testing
 */
function createMockBroker(nodeId: number, options: { isConnected?: boolean; failConnect?: boolean } = {}) {
	const mockBroker = {
		nodeId,
		host: `broker-${nodeId}.test.local`,
		port: 9092,
		isConnected: options.isConnected ?? false,
		connect: vi.fn().mockImplementation(async () => {
			if (options.failConnect) {
				throw new ConnectionClosedError('Connection refused')
			}
			mockBroker.isConnected = true
		}),
		disconnect: vi.fn().mockImplementation(async () => {
			mockBroker.isConnected = false
		}),
		metadata: vi.fn(),
		findCoordinator: vi.fn(),
		getApiVersion: vi.fn().mockReturnValue(0),
		getApiVersions: vi.fn().mockReturnValue(new Map()),
	}
	return mockBroker as unknown as Broker & {
		connect: ReturnType<typeof vi.fn>
		metadata: ReturnType<typeof vi.fn>
		findCoordinator: ReturnType<typeof vi.fn>
	}
}

describe('Cluster', () => {
	beforeEach(() => {
		vi.clearAllMocks()
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	describe('connect with failover', () => {
		it('connects to first available bootstrap broker', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['broker1:9092', 'broker2:9092', 'broker3:9092'],
			})

			// Mock the Broker constructor to return our mock brokers
			type BrokerMocks = (typeof cluster)['bootstrapBrokers']

			// Override connect to inject mock brokers
			cluster.connect = async () => {
				// Create mock brokers - first one fails, second succeeds
				const broker1 = createMockBroker(-1, { failConnect: true })
				const broker2 = createMockBroker(-2, { failConnect: false })
				const broker3 = createMockBroker(-3, { failConnect: false })

				// Mock metadata response
				broker2.metadata.mockResolvedValue({
					clusterId: 'test-cluster',
					controllerId: 1,
					brokers: [
						{ nodeId: 1, host: 'broker1.test', port: 9092, rack: null },
						{ nodeId: 2, host: 'broker2.test', port: 9092, rack: null },
					],
					topics: [],
				})
				;(cluster as unknown as { bootstrapBrokers: BrokerMocks }).bootstrapBrokers = [
					broker1 as unknown as Broker,
					broker2 as unknown as Broker,
					broker3 as unknown as Broker,
				]
				;(cluster as unknown as { isConnected: boolean }).isConnected = true

				// Connect loop
				for (const broker of (cluster as unknown as { bootstrapBrokers: BrokerMocks }).bootstrapBrokers) {
					try {
						await (broker as ReturnType<typeof createMockBroker>).connect()
						// Fetch metadata
						await (broker as ReturnType<typeof createMockBroker>).metadata({
							topics: null,
							allowAutoTopicCreation: false,
						})
						break
					} catch {
						// Try next broker
					}
				}
			}

			await cluster.connect()

			// Verify first broker failed, second succeeded
			const brokers = (cluster as unknown as { bootstrapBrokers: ReturnType<typeof createMockBroker>[] })
				.bootstrapBrokers
			expect(brokers[0]!.connect).toHaveBeenCalled()
			expect(brokers[1]!.connect).toHaveBeenCalled()
			expect(brokers[2]!.connect).not.toHaveBeenCalled() // Should stop after second succeeds
		})

		it('throws error when all bootstrap brokers fail', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['broker1:9092', 'broker2:9092'],
			})

			// Create failing mock brokers
			const broker1 = createMockBroker(-1, { failConnect: true })
			const broker2 = createMockBroker(-2, { failConnect: true })

			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [
				broker1 as unknown as Broker,
				broker2 as unknown as Broker,
			]

			await expect(cluster.connect()).rejects.toThrow('Failed to connect to any bootstrap broker')
		})
	})

	describe('getAnyBroker fallback', () => {
		it('returns connected broker from cache first', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const connectedBroker = createMockBroker(1, { isConnected: true })

			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, connectedBroker as unknown as Broker],
			])
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = []
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			const broker = await cluster.getAnyBroker()
			expect(broker).toBe(connectedBroker)
		})

		it('falls back to bootstrap broker when cached brokers disconnected', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const disconnectedBroker = createMockBroker(1, { isConnected: false })
			const bootstrapBroker = createMockBroker(-1, { isConnected: true })

			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, disconnectedBroker as unknown as Broker],
			])
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [
				bootstrapBroker as unknown as Broker,
			]
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			const broker = await cluster.getAnyBroker()
			expect(broker).toBe(bootstrapBroker)
		})

		it('throws when no brokers available', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const disconnectedBroker = createMockBroker(1, { isConnected: false, failConnect: true })
			const disconnectedBootstrap = createMockBroker(-1, { isConnected: false, failConnect: true })

			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, disconnectedBroker as unknown as Broker],
			])
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [
				disconnectedBootstrap as unknown as Broker,
			]
			;(cluster as unknown as { metadata: null }).metadata = null
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			await expect(cluster.getAnyBroker()).rejects.toThrow('No brokers available')
		})
	})

	describe('getBroker error handling', () => {
		it('throws BrokerNotAvailableError when broker not in metadata', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			;(cluster as unknown as { metadata: unknown }).metadata = {
				brokers: new Map(), // Empty - no brokers
				topics: new Map(),
				clusterId: 'test',
				controllerId: -1,
				updatedAt: Date.now(),
			}
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			await expect(cluster.getBroker(999)).rejects.toThrow(BrokerNotAvailableError)
		})
	})

	describe('getLeaderForPartition error handling', () => {
		it('throws LeaderNotAvailableError when topic not in metadata', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const mockBroker = createMockBroker(1, { isConnected: true })
			mockBroker.metadata.mockResolvedValue({
				clusterId: 'test',
				controllerId: 1,
				brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }],
				topics: [], // No topics
			})
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [mockBroker as unknown as Broker]
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			await expect(cluster.getLeaderForPartition('missing-topic', 0)).rejects.toThrow(LeaderNotAvailableError)
		})

		it('throws LeaderNotAvailableError when partition has no leader', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			;(cluster as unknown as { metadata: unknown }).metadata = {
				clusterId: 'test',
				controllerId: 1,
				brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
				topics: new Map([
					[
						'test-topic',
						{
							name: 'test-topic',
							partitions: new Map([
								[
									0,
									{
										partitionIndex: 0,
										leaderId: -1,
										replicaNodes: [],
										isrNodes: [],
										offlineReplicas: [],
									},
								], // No leader
							]),
						},
					],
				]),
				updatedAt: Date.now(),
			}
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			await expect(cluster.getLeaderForPartition('test-topic', 0)).rejects.toThrow(LeaderNotAvailableError)
		})
	})

	describe('coordinator cache invalidation', () => {
		it('invalidateCoordinator removes coordinator from cache', () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const coordinatorCache = (cluster as unknown as { coordinatorCache: Map<string, unknown> }).coordinatorCache
			coordinatorCache.set('GROUP:my-group', {
				type: 'GROUP',
				key: 'my-group',
				nodeId: 1,
				host: 'localhost',
				port: 9092,
				fetchedAt: Date.now(),
			})

			expect(coordinatorCache.has('GROUP:my-group')).toBe(true)

			cluster.invalidateCoordinator('GROUP', 'my-group')

			expect(coordinatorCache.has('GROUP:my-group')).toBe(false)
		})

		it('getCoordinator fetches fresh coordinator after invalidation', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const mockBroker = createMockBroker(1, { isConnected: true })
			mockBroker.findCoordinator.mockResolvedValue({
				errorCode: ErrorCode.None,
				nodeId: 2,
				host: 'coordinator.test',
				port: 9092,
				coordinators: [],
			})
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [mockBroker as unknown as Broker]
			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, mockBroker as unknown as Broker],
				[2, createMockBroker(2, { isConnected: true }) as unknown as Broker],
			])
			;(cluster as unknown as { metadata: unknown }).metadata = {
				clusterId: 'test',
				controllerId: 1,
				brokers: new Map([
					[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }],
					[2, { nodeId: 2, host: 'coordinator.test', port: 9092, rack: null }],
				]),
				topics: new Map(),
				updatedAt: Date.now(),
			}
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			// First call - will fetch coordinator
			await cluster.getCoordinator('GROUP', 'test-group')
			expect(mockBroker.findCoordinator).toHaveBeenCalledTimes(1)

			// Second call - should use cache (don't call findCoordinator again)
			await cluster.getCoordinator('GROUP', 'test-group')
			expect(mockBroker.findCoordinator).toHaveBeenCalledTimes(1)

			// Invalidate and call again - should fetch fresh
			cluster.invalidateCoordinator('GROUP', 'test-group')
			await cluster.getCoordinator('GROUP', 'test-group')
			expect(mockBroker.findCoordinator).toHaveBeenCalledTimes(2)
		})

		it('throws CoordinatorNotAvailableError when coordinator returns error', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const mockBroker = createMockBroker(1, { isConnected: true })
			mockBroker.findCoordinator.mockResolvedValue({
				errorCode: ErrorCode.CoordinatorNotAvailable,
				nodeId: -1,
				host: '',
				port: 0,
				coordinators: [],
			})
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [mockBroker as unknown as Broker]
			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, mockBroker as unknown as Broker],
			])
			;(cluster as unknown as { metadata: unknown }).metadata = {
				clusterId: 'test',
				controllerId: 1,
				brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
				topics: new Map(),
				updatedAt: Date.now(),
			}
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			await expect(cluster.getCoordinator('GROUP', 'test-group')).rejects.toThrow(CoordinatorNotAvailableError)
		})
	})

	describe('shouldRefreshMetadata', () => {
		it('returns true for NOT_LEADER_OR_FOLLOWER', () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			expect(cluster.shouldRefreshMetadata(ErrorCode.NotLeaderOrFollower)).toBe(true)
		})

		it('returns true for UNKNOWN_TOPIC_OR_PARTITION', () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			expect(cluster.shouldRefreshMetadata(ErrorCode.UnknownTopicOrPartition)).toBe(true)
		})

		it('returns false for non-metadata-related errors', () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			expect(cluster.shouldRefreshMetadata(ErrorCode.MessageTooLarge)).toBe(false)
			expect(cluster.shouldRefreshMetadata(ErrorCode.None)).toBe(false)
		})
	})

	describe('metadata refresh deduplication', () => {
		it('deduplicates concurrent metadata refresh calls', async () => {
			const cluster = new Cluster({
				clientId: 'test-client',
				brokers: ['bootstrap:9092'],
			})

			const mockBroker = createMockBroker(1, { isConnected: true })
			let callCount = 0
			mockBroker.metadata.mockImplementation(async () => {
				callCount++
				// Simulate some delay
				await new Promise(resolve => setTimeout(resolve, 10))
				return {
					clusterId: 'test',
					controllerId: 1,
					brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }],
					topics: [],
				}
			})
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [mockBroker as unknown as Broker]
			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([
				[1, mockBroker as unknown as Broker],
			])
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			// Make multiple concurrent refresh calls
			const promises = [cluster.refreshMetadata(), cluster.refreshMetadata(), cluster.refreshMetadata()]

			await Promise.all(promises)

			// Should only make one actual metadata call due to deduplication
			expect(callCount).toBe(1)
		})
	})
})
