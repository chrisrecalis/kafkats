/**
 * Cluster class - manages broker connections and cluster topology
 */

import { EventEmitter } from 'node:events'
import { Broker } from './broker.js'
import { KafkaError } from './errors.js'
import {
	LeaderNotAvailableError,
	CoordinatorNotAvailableError,
	NotCoordinatorError,
	BrokerNotAvailableError,
	shouldRefreshMetadata,
	throwIfError,
} from './errors.js'
import type {
	ClusterConfig,
	ClusterEvents,
	ClusterMetadata,
	BrokerInfo,
	TopicMetadata,
	PartitionMetadata,
	CoordinatorEntry,
	CoordinatorType,
	BrokerConfig,
} from './types.js'
import type { MetadataResponse } from '@/protocol/messages/responses/metadata.js'
import { COORDINATOR_TYPE } from '@/protocol/messages/requests/find-coordinator.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { noopLogger, type Logger } from '@/logger.js'

const DEFAULT_METADATA_REFRESH_INTERVAL_MS = 300000
const COORDINATOR_CACHE_TTL_MS = 300000

function parseBrokerAddress(address: string): { host: string; port: number } {
	const lastColonIndex = address.lastIndexOf(':')
	if (lastColonIndex === -1) {
		throw new Error(`Invalid broker address format: ${address}. Expected "host:port"`)
	}

	const host = address.slice(0, lastColonIndex)
	const portStr = address.slice(lastColonIndex + 1)
	const port = parseInt(portStr, 10)

	if (isNaN(port) || port < 1 || port > 65535) {
		throw new Error(`Invalid port in broker address: ${address}`)
	}

	return { host, port }
}

/**
 * Cluster manages broker connections and cluster topology
 */
export class Cluster extends EventEmitter<ClusterEvents> {
	private readonly config: ClusterConfig
	private readonly brokers: Map<number, Broker> = new Map()
	private readonly bootstrapBrokers: Broker[] = []
	private metadata: ClusterMetadata | null = null
	private readonly coordinatorCache: Map<string, CoordinatorEntry> = new Map()
	private metadataRefreshPromise: Promise<ClusterMetadata> | null = null
	private metadataRefreshInterval: ReturnType<typeof setInterval> | null = null
	private isConnected = false
	private readonly logger: Logger

	constructor(config: ClusterConfig) {
		super()
		this.config = {
			...config,
			metadataRefreshIntervalMs: config.metadataRefreshIntervalMs ?? DEFAULT_METADATA_REFRESH_INTERVAL_MS,
		}
		this.logger = config.logger?.child({ component: 'cluster' }) ?? noopLogger
	}

	getLogger(): Logger {
		return this.logger
	}

	async connect(): Promise<void> {
		if (this.isConnected) {
			return
		}

		this.logger.debug('connecting', { brokers: this.config.brokers })

		// Create bootstrap broker connections
		for (let i = 0; i < this.config.brokers.length; i++) {
			const { host, port } = parseBrokerAddress(this.config.brokers[i]!)

			const brokerConfig: BrokerConfig = {
				host,
				port,
				nodeId: -(i + 1), // Negative IDs for bootstrap brokers until we know real IDs
				clientId: this.config.clientId,
				connectionTimeoutMs: this.config.connectionTimeoutMs,
				requestTimeoutMs: this.config.requestTimeoutMs,
				maxInFlightRequests: this.config.maxInFlightRequests,
				tls: this.config.tls,
				sasl: this.config.sasl,
				logger: this.logger,
			}

			this.bootstrapBrokers.push(new Broker(brokerConfig))
		}

		let connectedBroker: Broker | null = null
		let lastError: Error | null = null

		for (const broker of this.bootstrapBrokers) {
			try {
				this.logger.debug('trying bootstrap broker', { host: broker.host, port: broker.port })
				await broker.connect()
				connectedBroker = broker
				this.logger.debug('connected to bootstrap broker', { host: broker.host, port: broker.port })
				this.emit('brokerConnect', broker.nodeId, broker.host, broker.port)
				break
			} catch (error) {
				this.logger.debug('bootstrap broker connection failed', {
					host: broker.host,
					port: broker.port,
					error: (error as Error).message,
				})
				lastError = error as Error
			}
		}

		if (!connectedBroker) {
			this.logger.error('failed to connect to any bootstrap broker', { error: lastError?.message })
			if (lastError instanceof KafkaError) {
				throw lastError
			}
			throw new Error(`Failed to connect to any bootstrap broker: ${lastError?.message}`, { cause: lastError })
		}

		this.isConnected = true

		// Fetch initial metadata
		await this.refreshMetadata()

		// Start periodic metadata refresh
		this.startMetadataRefreshInterval()
		this.logger.info('connected', { brokerCount: this.metadata?.brokers.size })
	}

	async disconnect(): Promise<void> {
		this.logger.debug('disconnecting')
		this.isConnected = false
		this.stopMetadataRefreshInterval()

		const disconnectPromises: Promise<void>[] = []

		for (const broker of this.brokers.values()) {
			disconnectPromises.push(
				broker.disconnect().catch((error: Error) => {
					this.emit('error', error)
				})
			)
		}

		for (const broker of this.bootstrapBrokers) {
			disconnectPromises.push(
				broker.disconnect().catch((error: Error) => {
					this.emit('error', error)
				})
			)
		}

		await Promise.all(disconnectPromises)

		this.brokers.clear()
		this.bootstrapBrokers.length = 0
		this.metadata = null
		this.coordinatorCache.clear()
		this.logger.info('disconnected')
	}

	async getBroker(nodeId: number): Promise<Broker> {
		// Check if we already have a connection
		let broker = this.brokers.get(nodeId)
		if (broker?.isConnected) {
			this.logger.debug('broker cache hit', { nodeId })
			return broker
		}

		this.logger.debug('broker cache miss', { nodeId })

		// Find broker info in metadata
		if (!this.metadata) {
			await this.refreshMetadata()
		}

		const brokerInfo = this.metadata?.brokers.get(nodeId)
		if (!brokerInfo) {
			this.logger.error('broker not found in metadata', { nodeId })
			throw new BrokerNotAvailableError(nodeId)
		}

		// Create new broker connection if needed
		if (!broker) {
			const brokerConfig: BrokerConfig = {
				host: brokerInfo.host,
				port: brokerInfo.port,
				nodeId: brokerInfo.nodeId,
				clientId: this.config.clientId,
				connectionTimeoutMs: this.config.connectionTimeoutMs,
				requestTimeoutMs: this.config.requestTimeoutMs,
				maxInFlightRequests: this.config.maxInFlightRequests,
				tls: this.config.tls,
				sasl: this.config.sasl,
				logger: this.logger,
			}
			broker = new Broker(brokerConfig)
			this.brokers.set(nodeId, broker)
		}

		// Connect to broker
		this.logger.debug('connecting to broker', { nodeId, host: brokerInfo.host, port: brokerInfo.port })
		await broker.connect()
		this.logger.debug('connected to broker', { nodeId, host: brokerInfo.host, port: brokerInfo.port })
		this.emit('brokerConnect', nodeId, brokerInfo.host, brokerInfo.port)

		return broker
	}

	async getLeaderForPartition(topic: string, partition: number): Promise<Broker> {
		this.logger.debug('getting leader for partition', { topic, partition })

		if (!this.metadata) {
			await this.refreshMetadata([topic])
		}

		const topicMetadata = this.metadata?.topics.get(topic)
		if (!topicMetadata) {
			// Refresh metadata for this topic
			this.logger.debug('topic not in metadata, refreshing', { topic })
			await this.refreshMetadata([topic])
			const refreshedTopic = this.metadata?.topics.get(topic)
			if (!refreshedTopic) {
				this.logger.error('leader not available', { topic, partition })
				throw new LeaderNotAvailableError(topic, partition)
			}
		}

		const partitionMetadata = this.metadata?.topics.get(topic)?.partitions.get(partition)
		if (!partitionMetadata) {
			this.logger.error('partition not found', { topic, partition })
			throw new LeaderNotAvailableError(topic, partition)
		}

		if (partitionMetadata.leaderId < 0) {
			this.logger.error('no leader for partition', { topic, partition })
			throw new LeaderNotAvailableError(topic, partition)
		}

		this.logger.debug('found leader for partition', { topic, partition, leaderId: partitionMetadata.leaderId })
		return this.getBroker(partitionMetadata.leaderId)
	}

	async getCoordinator(type: CoordinatorType, key: string): Promise<Broker> {
		const cacheKey = `${type}:${key}`

		// Check cache first
		const cached = this.coordinatorCache.get(cacheKey)
		if (cached && Date.now() - cached.fetchedAt < COORDINATOR_CACHE_TTL_MS) {
			this.logger.debug('coordinator cache hit', { type, key, nodeId: cached.nodeId })
			try {
				return await this.getBroker(cached.nodeId)
			} catch {
				// Cache entry is stale, remove it
				this.logger.debug('coordinator cache entry stale', { type, key })
				this.coordinatorCache.delete(cacheKey)
			}
		}

		this.logger.debug('finding coordinator', { type, key })

		// Find coordinator
		const broker = await this.getAnyBroker()
		const response = await broker.findCoordinator({
			key,
			keyType: COORDINATOR_TYPE[type],
			coordinatorKeys: [key],
		})

		// Get coordinator info - v4+ uses coordinators array, v3 uses top-level fields
		let nodeId: number
		let host: string
		let port: number
		let coordinatorErrorCode: ErrorCode

		if (response.coordinators && response.coordinators.length > 0) {
			// v4+: coordinator info is in the coordinators array
			const coordinator = response.coordinators[0]!
			nodeId = coordinator.nodeId
			host = coordinator.host
			port = coordinator.port
			coordinatorErrorCode = coordinator.errorCode
		} else {
			// v3: coordinator info is at the top level
			nodeId = response.nodeId
			host = response.host
			port = response.port
			coordinatorErrorCode = response.errorCode
		}

		// Check for errors
		if (coordinatorErrorCode !== ErrorCode.None) {
			if (coordinatorErrorCode === ErrorCode.CoordinatorNotAvailable) {
				this.logger.error('coordinator not available', { type, key, errorCode: coordinatorErrorCode })
				throw new CoordinatorNotAvailableError(type, key)
			}
			if (coordinatorErrorCode === ErrorCode.NotCoordinator) {
				this.logger.error('not coordinator', { type, key, errorCode: coordinatorErrorCode })
				throw new NotCoordinatorError(type, key)
			}
			throwIfError(coordinatorErrorCode, `FindCoordinator for ${type} ${key}`)
		}

		// Validate we got valid coordinator info
		if (nodeId < 0 || !host) {
			this.logger.error('invalid coordinator info', { type, key, nodeId, host })
			throw new CoordinatorNotAvailableError(type, key)
		}

		this.logger.debug('coordinator found', { type, key, nodeId, host, port })

		// Cache the result
		const entry: CoordinatorEntry = {
			type,
			key,
			nodeId,
			host,
			port,
			fetchedAt: Date.now(),
		}
		this.coordinatorCache.set(cacheKey, entry)

		// Ensure we have broker info for this coordinator
		if (!this.metadata?.brokers.has(nodeId)) {
			// Add broker info from coordinator response
			const brokerInfo: BrokerInfo = {
				nodeId,
				host,
				port,
				rack: null,
			}
			this.metadata?.brokers.set(nodeId, brokerInfo)
		}

		return this.getBroker(nodeId)
	}

	async getControllerBroker(): Promise<Broker> {
		if (!this.metadata) {
			await this.refreshMetadata()
		}

		if (!this.metadata || this.metadata.controllerId < 0) {
			throw new BrokerNotAvailableError(-1)
		}

		return this.getBroker(this.metadata.controllerId)
	}

	async getAnyBroker(): Promise<Broker> {
		// Try connected brokers first
		for (const broker of this.brokers.values()) {
			if (broker.isConnected) {
				return broker
			}
		}

		// Try bootstrap brokers
		for (const broker of this.bootstrapBrokers) {
			if (broker.isConnected) {
				return broker
			}
		}

		// Try to connect to any known broker
		if (this.metadata) {
			for (const [nodeId] of this.metadata.brokers) {
				try {
					return await this.getBroker(nodeId)
				} catch {
					// Try next broker
				}
			}
		}

		// Try bootstrap brokers
		for (const broker of this.bootstrapBrokers) {
			try {
				await broker.connect()
				return broker
			} catch {
				// Try next broker
			}
		}

		throw new Error('No brokers available')
	}

	async refreshMetadata(topics?: string[]): Promise<ClusterMetadata> {
		// Deduplicate concurrent refresh requests
		if (this.metadataRefreshPromise) {
			return this.metadataRefreshPromise
		}

		this.metadataRefreshPromise = this.doRefreshMetadata(topics)
		try {
			return await this.metadataRefreshPromise
		} finally {
			this.metadataRefreshPromise = null
		}
	}

	private async doRefreshMetadata(topics?: string[]): Promise<ClusterMetadata> {
		const startTime = Date.now()
		this.logger.debug('refreshing metadata', { topics })

		const broker = await this.getAnyBroker()

		const response = await broker.metadata({
			topics: topics ? topics.map(name => ({ name })) : null,
			allowAutoTopicCreation: false,
		})

		// Update metadata
		this.metadata = this.parseMetadataResponse(response)
		this.emit('metadataUpdate', this.metadata)

		const durationMs = Date.now() - startTime
		this.logger.debug('metadata refreshed', {
			durationMs,
			brokerCount: this.metadata.brokers.size,
			topicCount: this.metadata.topics.size,
		})

		return this.metadata
	}

	private parseMetadataResponse(response: MetadataResponse): ClusterMetadata {
		const brokers = new Map<number, BrokerInfo>()
		for (const broker of response.brokers) {
			brokers.set(broker.nodeId, {
				nodeId: broker.nodeId,
				host: broker.host,
				port: broker.port,
				rack: broker.rack,
			})
		}

		const topics = new Map<string, TopicMetadata>()
		for (const topic of response.topics) {
			if (topic.errorCode === ErrorCode.None && topic.name) {
				const partitions = new Map<number, PartitionMetadata>()
				for (const partition of topic.partitions) {
					if (partition.errorCode === ErrorCode.None) {
						partitions.set(partition.partitionIndex, {
							partitionIndex: partition.partitionIndex,
							leaderId: partition.leaderId,
							leaderEpoch: partition.leaderEpoch,
							replicaNodes: partition.replicaNodes,
							isrNodes: partition.isrNodes,
							offlineReplicas: partition.offlineReplicas,
						})
					}
				}

				topics.set(topic.name, {
					name: topic.name,
					topicId: topic.topicId,
					isInternal: topic.isInternal,
					partitions,
				})
			}
		}

		return {
			clusterId: response.clusterId,
			controllerId: response.controllerId,
			brokers,
			topics,
			updatedAt: Date.now(),
		}
	}

	getMetadata(): ClusterMetadata | null {
		return this.metadata
	}

	shouldRefreshMetadata(errorCode: ErrorCode): boolean {
		return shouldRefreshMetadata(errorCode)
	}

	invalidateCoordinator(type: CoordinatorType, key: string): void {
		const cacheKey = `${type}:${key}`
		this.coordinatorCache.delete(cacheKey)
	}

	private startMetadataRefreshInterval(): void {
		if (this.metadataRefreshInterval) {
			return
		}

		const intervalMs = this.config.metadataRefreshIntervalMs ?? DEFAULT_METADATA_REFRESH_INTERVAL_MS
		this.metadataRefreshInterval = setInterval(() => {
			this.refreshMetadata().catch((error: Error) => {
				this.emit('error', error)
			})
		}, intervalMs)

		// Don't prevent process from exiting
		if (this.metadataRefreshInterval.unref) {
			this.metadataRefreshInterval.unref()
		}
	}

	private stopMetadataRefreshInterval(): void {
		if (this.metadataRefreshInterval) {
			clearInterval(this.metadataRefreshInterval)
			this.metadataRefreshInterval = null
		}
	}
}
