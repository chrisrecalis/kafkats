/**
 * KafkaClient - main entry point for low-level Kafka operations
 */

import { Cluster } from './cluster.js'
import type { KafkaClientConfig, ClusterMetadata, ClusterConfig } from './types.js'
import type { ProduceRequest, ProduceRequestPartition } from '@/protocol/messages/requests/produce.js'
import type { ProducePartitionResponse } from '@/protocol/messages/responses/produce.js'
import type { FetchRequest } from '@/protocol/messages/requests/fetch.js'
import type { FetchPartitionResponse } from '@/protocol/messages/responses/fetch.js'
import { Consumer } from '@/consumer/consumer.js'
import type { ConsumerConfig } from '@/consumer/types.js'
import { Producer } from '@/producer/producer.js'
import type { ProducerConfig } from '@/producer/types.js'
import { Admin } from '@/admin/admin.js'
import type { AdminConfig } from '@/admin/types.js'
import { createLogger, noopLogger, type Logger } from '@/logger.js'
import { createCreateTopicsRequest } from '@/protocol/messages/requests/create-topics.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError } from './errors.js'

/**
 * Options for produce operations
 */
export interface ProduceOptions {
	/** Required acknowledgments: 0=none, 1=leader, -1=all */
	acks?: number
	/** Timeout in milliseconds */
	timeoutMs?: number
	/** Transactional ID */
	transactionalId?: string | null
}

/**
 * Options for fetch operations
 */
export interface FetchOptions {
	/** Maximum wait time in milliseconds */
	maxWaitMs?: number
	/** Minimum bytes to return */
	minBytes?: number
	/** Maximum bytes to return */
	maxBytes?: number
	/** Isolation level: 0=read_uncommitted, 1=read_committed */
	isolationLevel?: number
}

const DEFAULT_PRODUCE_OPTIONS: Required<Omit<ProduceOptions, 'transactionalId'>> = {
	acks: -1,
	timeoutMs: 30000,
}

const DEFAULT_FETCH_OPTIONS: Required<Omit<FetchOptions, 'isolationLevel'>> = {
	maxWaitMs: 5000,
	minBytes: 1,
	maxBytes: 1048576, // 1MB
}

/**
 * KafkaClient provides low-level Kafka operations
 *
 * This is the main entry point for interacting with Kafka.
 * It manages cluster connections and provides convenience methods
 * for common operations like produce and fetch.
 */
export class KafkaClient {
	readonly cluster: Cluster
	private readonly config: KafkaClientConfig
	private readonly logger: Logger

	constructor(config: KafkaClientConfig) {
		this.config = config

		// Create or use provided logger
		if (config.logger) {
			this.logger = config.logger.child({ component: 'kafka-client', clientId: config.clientId })
		} else if (config.logLevel && config.logLevel !== 'silent') {
			this.logger = createLogger(config.logLevel, { component: 'kafka-client', clientId: config.clientId })
		} else {
			this.logger = noopLogger
		}

		this.logger.info('initializing', { brokers: config.brokers })

		const clusterConfig: ClusterConfig = {
			brokers: config.brokers,
			clientId: config.clientId,
			connectionTimeoutMs: config.connectionTimeoutMs,
			requestTimeoutMs: config.requestTimeoutMs,
			maxInFlightRequests: config.maxInFlightRequests,
			tls: config.tls,
			sasl: config.sasl,
			metadataRefreshIntervalMs: config.metadataRefreshIntervalMs,
			logger: this.logger,
		}

		this.cluster = new Cluster(clusterConfig)
	}

	get isConnected(): boolean {
		return this.cluster.getMetadata() !== null
	}

	async connect(): Promise<void> {
		await this.cluster.connect()
	}

	async disconnect(): Promise<void> {
		await this.cluster.disconnect()
	}

	async getMetadata(topics?: string[]): Promise<ClusterMetadata> {
		return this.cluster.refreshMetadata(topics)
	}

	async produce(
		topic: string,
		partition: number,
		records: Buffer,
		options?: ProduceOptions
	): Promise<ProducePartitionResponse> {
		const leader = await this.cluster.getLeaderForPartition(topic, partition)

		const opts = { ...DEFAULT_PRODUCE_OPTIONS, ...options }

		const request: ProduceRequest = {
			transactionalId: options?.transactionalId ?? null,
			acks: opts.acks,
			timeoutMs: opts.timeoutMs,
			topics: [
				{
					name: topic,
					partitions: [
						{
							partitionIndex: partition,
							records,
						},
					],
				},
			],
		}

		const response = await leader.produce(request)

		// Find the partition response
		const topicResponse = response.topics.find(t => t.name === topic)
		const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === partition)

		if (!partitionResponse) {
			throw new Error(`No response for partition ${topic}-${partition}`)
		}

		return partitionResponse
	}

	async produceToMultiplePartitions(
		topic: string,
		partitionRecords: Map<number, Buffer>,
		options?: ProduceOptions
	): Promise<Map<number, ProducePartitionResponse>> {
		// Group partitions by leader
		const partitionsByLeader = new Map<number, ProduceRequestPartition[]>()

		for (const [partition, records] of partitionRecords) {
			const leader = await this.cluster.getLeaderForPartition(topic, partition)
			const leaderId = leader.nodeId

			if (!partitionsByLeader.has(leaderId)) {
				partitionsByLeader.set(leaderId, [])
			}

			partitionsByLeader.get(leaderId)!.push({
				partitionIndex: partition,
				records,
			})
		}

		const opts = { ...DEFAULT_PRODUCE_OPTIONS, ...options }
		const results = new Map<number, ProducePartitionResponse>()

		// Send requests to each leader in parallel
		const promises = Array.from(partitionsByLeader.entries()).map(async ([leaderId, partitions]) => {
			const leader = await this.cluster.getBroker(leaderId)

			const request: ProduceRequest = {
				transactionalId: options?.transactionalId ?? null,
				acks: opts.acks,
				timeoutMs: opts.timeoutMs,
				topics: [
					{
						name: topic,
						partitions,
					},
				],
			}

			const response = await leader.produce(request)

			// Collect responses
			const topicResponse = response.topics.find(t => t.name === topic)
			if (topicResponse) {
				for (const partitionResponse of topicResponse.partitions) {
					results.set(partitionResponse.partitionIndex, partitionResponse)
				}
			}
		})

		await Promise.all(promises)

		return results
	}

	async fetch(
		topic: string,
		partition: number,
		offset: bigint,
		options?: FetchOptions
	): Promise<FetchPartitionResponse> {
		const leader = await this.cluster.getLeaderForPartition(topic, partition)

		const opts = { ...DEFAULT_FETCH_OPTIONS, ...options }

		const request: FetchRequest = {
			replicaId: -1, // Regular consumer
			maxWaitMs: opts.maxWaitMs,
			minBytes: opts.minBytes,
			maxBytes: opts.maxBytes,
			isolationLevel: options?.isolationLevel ?? 0,
			topics: [
				{
					topic,
					topicId: '00000000-0000-0000-0000-000000000000',
					partitions: [
						{
							partitionIndex: partition,
							currentLeaderEpoch: -1,
							fetchOffset: offset,
							lastFetchedEpoch: -1,
							logStartOffset: -1n,
							partitionMaxBytes: opts.maxBytes,
						},
					],
				},
			],
		}

		const response = await leader.fetch(request)

		// Find the partition response
		const topicResponse = response.topics.find(t => t.topic === topic)
		const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === partition)

		if (!partitionResponse) {
			throw new Error(`No response for partition ${topic}-${partition}`)
		}

		return partitionResponse
	}

	/**
	 * Create topics on the cluster
	 *
	 * Routes the request to the controller broker. Ignores TopicAlreadyExists
	 * errors by default, making this method idempotent.
	 *
	 * @param topics - Topics to create with optional configuration
	 * @param options - Optional settings
	 * @returns Promise that resolves when topics are created
	 * @throws KafkaProtocolError if topic creation fails (except TopicAlreadyExists)
	 */
	async createTopics(
		topics: Array<{
			name: string
			numPartitions?: number
			replicationFactor?: number
			configs?: Record<string, string>
		}>,
		options?: { timeoutMs?: number; validateOnly?: boolean }
	): Promise<void> {
		const controller = await this.cluster.getControllerBroker()

		const request = createCreateTopicsRequest(topics)
		if (options?.timeoutMs) {
			request.timeoutMs = options.timeoutMs
		}
		if (options?.validateOnly) {
			request.validateOnly = options.validateOnly
		}

		const response = await controller.createTopics(request)

		// Check for errors (except TopicAlreadyExists which we ignore)
		for (const topic of response.topics) {
			if (topic.errorCode !== ErrorCode.None && topic.errorCode !== ErrorCode.TopicAlreadyExists) {
				throw new KafkaProtocolError(
					topic.errorCode,
					`Failed to create topic ${topic.name}: ${topic.errorMessage ?? 'Unknown error'}`
				)
			}
		}
	}

	getConfig(): Readonly<KafkaClientConfig> {
		return this.config
	}

	consumer(config: ConsumerConfig): Consumer {
		return new Consumer(this.cluster, config)
	}

	/**
	 * Create a producer for this client
	 *
	 * The producer uses implicit batching - messages are queued locally
	 * and batched before sending based on lingerMs and maxBatchBytes.
	 *
	 * @param config - Producer configuration
	 * @returns A new Producer instance
	 *
	 * @example
	 * ```typescript
	 * const producer = client.producer({
	 *   lingerMs: 5,
	 *   maxBatchBytes: 16384,
	 *   acks: 'all',
	 *   compression: 'snappy',
	 * })
	 *
	 * const result = await producer.send(orderTopic, { key: order.id, value: order })
	 * await producer.disconnect()
	 * ```
	 */
	producer(config?: ProducerConfig): Producer {
		return new Producer(this.cluster, config)
	}

	/**
	 * Create an admin client for cluster management operations
	 *
	 * The admin client provides methods for managing topics, consumer groups,
	 * and querying cluster metadata.
	 *
	 * @param config - Admin configuration
	 * @returns A new Admin instance
	 *
	 * @example
	 * ```typescript
	 * const admin = client.admin()
	 *
	 * // List topics
	 * const topics = await admin.listTopics()
	 *
	 * // Delete topics
	 * await admin.deleteTopics(['old-topic'])
	 *
	 * // List consumer groups
	 * const groups = await admin.listGroups()
	 *
	 * // Delete consumer groups
	 * await admin.deleteGroups(['old-group'])
	 * ```
	 */
	admin(config?: AdminConfig): Admin {
		return new Admin(this.cluster, config)
	}
}
