import { describe, expect, it, vi, beforeEach } from 'vitest'

import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Mock the Broker module so Cluster.getBroker creates inspectable, network-free
 * brokers. Instances are recorded for assertions on how many were created and
 * with which host/port.
 */
vi.mock('@/client/broker.js', () => {
	interface MockBrokerConfig {
		host: string
		port: number
		nodeId: number
	}

	class MockBroker {
		readonly host: string
		readonly port: number
		readonly nodeId: number
		isConnected = false
		connect = vi.fn(async () => {
			await Promise.resolve()
			this.isConnected = true
		})
		disconnect = vi.fn(async () => {
			this.isConnected = false
		})
		metadata = vi.fn()
		findCoordinator = vi.fn()

		constructor(config: MockBrokerConfig) {
			this.host = config.host
			this.port = config.port
			this.nodeId = config.nodeId
			instances.push(this)
		}
	}

	const instances: MockBroker[] = []

	return {
		Broker: MockBroker,
		__instances: instances,
	}
})

type MockedBroker = Broker & {
	connect: ReturnType<typeof vi.fn>
	disconnect: ReturnType<typeof vi.fn>
}

async function getInstances(): Promise<MockedBroker[]> {
	const module = (await import('@/client/broker.js')) as unknown as { __instances: MockedBroker[] }
	return module.__instances
}

/**
 * Plain connected broker stub used as bootstrap/metadata source.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function createBrokerStub(nodeId: number, metadataImpl: (...args: any[]) => Promise<unknown>) {
	return {
		nodeId,
		host: `stub-${nodeId}`,
		port: 9092,
		isConnected: true,
		connect: vi.fn(),
		disconnect: vi.fn(),
		metadata: vi.fn(metadataImpl),
		findCoordinator: vi.fn(),
	} as unknown as Broker
}

const brokersList = [{ nodeId: 1, host: 'host-a', port: 9092, rack: null }]

function topicMeta(name: string, errorCode: ErrorCode = ErrorCode.None) {
	return {
		errorCode,
		name,
		topicId: '00000000-0000-0000-0000-000000000000',
		isInternal: false,
		partitions:
			errorCode === ErrorCode.None
				? [
						{
							errorCode: ErrorCode.None,
							partitionIndex: 0,
							leaderId: 1,
							leaderEpoch: 0,
							replicaNodes: [1],
							isrNodes: [1],
							offlineReplicas: [],
						},
					]
				: [],
	}
}

describe('Cluster topology handling', () => {
	beforeEach(async () => {
		const instances = await getInstances()
		instances.length = 0
	})

	describe('stale broker address (node moved)', () => {
		it('reconnects to the new address from fresh metadata instead of the cached one', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			// Cached (disconnected) broker for node 1 at the OLD address.
			const stale = new Broker({ host: 'host-a', port: 9092, nodeId: 1, clientId: 'c' }) as MockedBroker
			;(cluster as unknown as { brokers: Map<number, Broker> }).brokers = new Map([[1, stale]])

			// Fresh metadata already says node 1 moved to host-b:9093.
			;(cluster as unknown as { metadata: unknown }).metadata = {
				clusterId: 'c',
				controllerId: 1,
				brokers: new Map([[1, { nodeId: 1, host: 'host-b', port: 9093, rack: null }]]),
				topics: new Map(),
				updatedAt: Date.now(),
			}

			const broker = await cluster.getBroker(1)

			expect(broker.host).toBe('host-b')
			expect(broker.port).toBe(9093)
			expect(broker.isConnected).toBe(true)
			// The stale broker must be disposed, not reconnected.
			expect(stale.connect).not.toHaveBeenCalled()
			expect(stale.disconnect).toHaveBeenCalled()
		})
	})

	describe('scoped metadata refresh topic eviction', () => {
		it('evicts a topic that comes back with UNKNOWN_TOPIC_OR_PARTITION on a scoped refresh', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			const stub = createBrokerStub(1, async () => ({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A'), topicMeta('B')],
			}))
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [stub]

			await cluster.refreshMetadata()
			expect([...cluster.getMetadata()!.topics.keys()].sort()).toEqual(['A', 'B'])

			// B is deleted: a scoped refresh for B returns a topic-level fatal error.
			;(stub.metadata as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('B', ErrorCode.UnknownTopicOrPartition)],
			})
			await cluster.refreshMetadata(['B'])

			const topics = cluster.getMetadata()!.topics
			expect(topics.has('B')).toBe(false)
			expect(topics.has('A')).toBe(true)
		})

		it('evicts a topic that comes back with INVALID_TOPIC_EXCEPTION on a scoped refresh', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			const stub = createBrokerStub(1, async () => ({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('A'), topicMeta('B')],
			}))
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [stub]

			await cluster.refreshMetadata()
			;(stub.metadata as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
				clusterId: 'c',
				controllerId: 1,
				brokers: brokersList,
				topics: [topicMeta('B', ErrorCode.InvalidTopicException)],
			})
			await cluster.refreshMetadata(['B'])

			const topics = cluster.getMetadata()!.topics
			expect(topics.has('B')).toBe(false)
			expect(topics.has('A')).toBe(true)
		})
	})

	describe('concurrent getBroker', () => {
		it('creates and connects a single Broker when two callers race a metadata refresh', async () => {
			const cluster = new Cluster({ clientId: 'c', brokers: ['bootstrap:9092'] })
			;(cluster as unknown as { isConnected: boolean }).isConnected = true

			// metadata starts null so both callers go through the awaited refresh.
			const stub = createBrokerStub(-1, async () => {
				await new Promise(resolve => setTimeout(resolve, 10))
				return {
					clusterId: 'c',
					controllerId: 1,
					brokers: brokersList,
					topics: [],
				}
			})
			;(cluster as unknown as { bootstrapBrokers: Broker[] }).bootstrapBrokers = [stub]

			const [a, b] = await Promise.all([cluster.getBroker(1), cluster.getBroker(1)])

			expect(a).toBe(b)

			const instances = await getInstances()
			const node1Instances = instances.filter(instance => instance.nodeId === 1)
			expect(node1Instances).toHaveLength(1)
		})
	})
})
