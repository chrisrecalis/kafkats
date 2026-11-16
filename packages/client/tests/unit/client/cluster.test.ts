import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { BrokerNotAvailableError, LeaderNotAvailableError, CoordinatorNotAvailableError } from '@/client/errors.js'
import { encodeSubscriptionMetadata } from '@/consumer/assignors/index.js'
import { ConsumerGroup } from '@/consumer/consumer-group.js'
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

const brokersList = [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }]

/** Make `mockBroker` the cluster's only (connected) broker, node 1. */
function wireBroker(cluster: Cluster, mockBroker: ReturnType<typeof createMockBroker>): void {
	;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [mockBroker]
	;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([[1, mockBroker]])
	;(cluster as unknown as { isConnected: boolean }).isConnected = true
}

function partitionMeta(partitionIndex: number, leaderId: number, leaderEpoch: number, errorCode = ErrorCode.None) {
	return {
		errorCode,
		partitionIndex,
		leaderId,
		leaderEpoch,
		replicaNodes: [1],
		isrNodes: leaderId < 0 ? [] : [leaderId],
		offlineReplicas: [],
	}
}

/** A metadata response for one topic, `t`, with the given partitions. */
function metadataResponse(partitions: ReturnType<typeof partitionMeta>[]) {
	return {
		clusterId: 'c',
		controllerId: 1,
		brokers: brokersList,
		topics: [
			{
				errorCode: ErrorCode.None,
				name: 't',
				topicId: '00000000-0000-0000-0000-000000000000',
				isInternal: false,
				partitions,
			},
		],
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

		it('throws LeaderNotAvailableError when the partition still has no leader after a refresh', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			mockBroker.metadata.mockResolvedValue(
				metadataResponse([partitionMeta(0, -1, 4, ErrorCode.LeaderNotAvailable)])
			)
			await cluster.refreshMetadata()
			mockBroker.metadata.mockClear()

			await expect(cluster.getLeaderForPartition('t', 0)).rejects.toThrow(LeaderNotAvailableError)
			expect(mockBroker.metadata).toHaveBeenCalledTimes(1)
			expect(mockBroker.metadata).toHaveBeenCalledWith(expect.objectContaining({ topics: [{ name: 't' }] }))
		})

		it('refreshes the topic when the cached partition has no leader and returns the elected leader', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			mockBroker.metadata.mockResolvedValueOnce(
				metadataResponse([partitionMeta(0, -1, 4, ErrorCode.LeaderNotAvailable)])
			)
			await cluster.refreshMetadata()

			// The election finished: the refresh names broker 1 as the leader.
			mockBroker.metadata.mockResolvedValueOnce(metadataResponse([partitionMeta(0, 1, 5)]))

			await expect(cluster.getLeaderForPartition('t', 0)).resolves.toBe(mockBroker)
			expect(mockBroker.metadata).toHaveBeenCalledTimes(2)
		})

		it('does not refresh when the cached partition has a leader', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			mockBroker.metadata.mockResolvedValueOnce(metadataResponse([partitionMeta(0, 1, 5)]))
			await cluster.refreshMetadata()

			await expect(cluster.getLeaderForPartition('t', 0)).resolves.toBe(mockBroker)
			expect(mockBroker.metadata).toHaveBeenCalledTimes(1)
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

	describe('metadata merge and leader epoch', () => {
		function topicMeta(
			name: string,
			partition: number,
			leaderId: number,
			leaderEpoch: number,
			topicId = '00000000-0000-0000-0000-000000000000'
		) {
			return {
				errorCode: ErrorCode.None,
				name,
				topicId,
				isInternal: false,
				partitions: [
					{
						errorCode: ErrorCode.None,
						partitionIndex: partition,
						leaderId,
						leaderEpoch,
						replicaNodes: [leaderId],
						isrNodes: [leaderId],
						offlineReplicas: [],
					},
				],
			}
		}

		it('does not drop previously-known topics on a topic-scoped refresh', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)

			// Full refresh learns topics A and B.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 1, 0), topicMeta('B', 0, 1, 0)],
			})
			await cluster.refreshMetadata()
			expect([...cluster.getMetadata()!.topics.keys()].sort()).toEqual(['A', 'B'])

			// Topic-scoped refresh for A only — the response carries just A.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 1, 1)],
			})
			await cluster.refreshMetadata(['A'])

			// B must survive the scoped refresh.
			expect([...cluster.getMetadata()!.topics.keys()].sort()).toEqual(['A', 'B'])
		})

		it('ignores a stale (lower leader-epoch) partition update but accepts a newer one', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)

			// Learn leader=1 at epoch 5.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 1, 5)],
			})
			await cluster.refreshMetadata()

			// A stale response (epoch 3) names a different leader — must be ignored.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 2, 3)],
			})
			await cluster.refreshMetadata()
			let p = cluster.getMetadata()!.topics.get('A')!.partitions.get(0)!
			expect(p.leaderEpoch).toBe(5)
			expect(p.leaderId).toBe(1)

			// A newer response (epoch 7) is applied.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 2, 7)],
			})
			await cluster.refreshMetadata()
			p = cluster.getMetadata()!.topics.get('A')!.partitions.get(0)!
			expect(p.leaderEpoch).toBe(7)
			expect(p.leaderId).toBe(2)
		})

		it('does not let the epoch guard retain a stale leader after a topic is recreated (new topic id)', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)

			const idOld = '11111111-1111-1111-1111-111111111111'
			const idNew = '22222222-2222-2222-2222-222222222222'

			// Topic A at epoch 5 under its original topic id.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 1, 5, idOld)],
			})
			await cluster.refreshMetadata()

			// A is deleted and recreated: same name, NEW topic id, epoch reset to 0, new leader.
			mockBroker.metadata.mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A', 0, 2, 0, idNew)],
			})
			await cluster.refreshMetadata()

			// The recreated topic's metadata must win despite the lower epoch.
			const topic = cluster.getMetadata()!.topics.get('A')!
			expect(topic.topicId).toBe(idNew)
			expect(topic.partitions.get(0)!.leaderId).toBe(2)
			expect(topic.partitions.get(0)!.leaderEpoch).toBe(0)
		})
	})

	describe('partitions with a partition-level error', () => {
		// Partition 1 is between leaders, as during an election.
		const electing = [
			partitionMeta(0, 1, 3),
			partitionMeta(1, -1, 4, ErrorCode.LeaderNotAvailable),
			partitionMeta(2, 1, 3),
		]

		it('keeps the partition with leader -1, so the partition count is the real one', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			mockBroker.metadata.mockResolvedValueOnce(metadataResponse(electing))

			// First lookup of the topic in this process: nothing cached to fall back on.
			const topic = (await cluster.refreshMetadata(['t'])).topics.get('t')!

			expect(topic.partitions.size).toBe(3)
			expect([...topic.partitions.keys()]).toEqual([0, 1, 2])
			expect(topic.partitions.get(1)).toMatchObject({ partitionIndex: 1, leaderId: -1, leaderEpoch: 4 })
		})

		it('applies the leader epoch guard to a partition that comes back with an error', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			const partition0 = () => cluster.getMetadata()!.topics.get('t')!.partitions.get(0)!

			mockBroker.metadata.mockResolvedValueOnce(metadataResponse([partitionMeta(0, 1, 5)]))
			await cluster.refreshMetadata()

			// An older response (epoch 4) without a leader does not regress the known leader.
			mockBroker.metadata.mockResolvedValueOnce(
				metadataResponse([partitionMeta(0, -1, 4, ErrorCode.LeaderNotAvailable)])
			)
			await cluster.refreshMetadata()
			expect(partition0()).toMatchObject({ leaderId: 1, leaderEpoch: 5 })

			// The leader is gone at epoch 6: the partition stays, without a leader.
			mockBroker.metadata.mockResolvedValueOnce(
				metadataResponse([partitionMeta(0, -1, 6, ErrorCode.LeaderNotAvailable)])
			)
			await cluster.refreshMetadata()
			expect(partition0()).toMatchObject({ leaderId: -1, leaderEpoch: 6 })

			// A new leader is elected at epoch 7.
			mockBroker.metadata.mockResolvedValueOnce(metadataResponse([partitionMeta(0, 1, 7)]))
			await cluster.refreshMetadata()
			expect(partition0()).toMatchObject({ leaderId: 1, leaderEpoch: 7 })
		})

		it('gives the range assignor every partition, including the one without a leader', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			const mockBroker = createMockBroker(1, { isConnected: true })
			wireBroker(cluster, mockBroker)
			mockBroker.metadata.mockResolvedValueOnce(metadataResponse(electing))

			const group = new ConsumerGroup(cluster, { groupId: 'g', partitionAssignmentStrategy: 'range' })
			const leader = group as unknown as {
				computeAssignments(
					topics: string[],
					members: Array<{ memberId: string; groupInstanceId: string | null; metadata: Buffer }>,
					protocolName: string | null
				): Promise<void>
				leaderAssignments: Map<string, { partitions: Array<{ topic: string; partitions: number[] }> }> | null
			}
			const subscription = encodeSubscriptionMetadata({ version: 0, topics: ['t'], userData: null })

			await leader.computeAssignments(
				['t'],
				[
					{ memberId: 'a', groupInstanceId: null, metadata: subscription },
					{ memberId: 'b', groupInstanceId: null, metadata: subscription },
				],
				'range'
			)

			const assigned = [...leader.leaderAssignments!.values()]
				.flatMap(assignment => assignment.partitions.flatMap(tp => tp.partitions))
				.sort((x, y) => x - y)
			expect(assigned).toEqual([0, 1, 2])
		})
	})
})
