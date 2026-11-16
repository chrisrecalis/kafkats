/**
 * Admin client for Kafka cluster management operations
 */

import type { Cluster } from '@/client/cluster.js'
import type {
	AdminConfig,
	ResolvedAdminConfig,
	TopicDescription,
	PartitionInfo,
	ConsumerGroupListing,
	ConsumerGroupDescription,
	MemberDescription,
	TopicPartition,
	ClusterDescription,
	BrokerDescription,
	CreateTopicsResult,
	DeleteTopicsResult,
	DeleteGroupsResult,
	AclBinding,
	AclBindingFilter,
	DescribeAclsResult,
	CreateAclResult,
	DeleteAclsFilterResult,
	RecordsToDelete,
	DeleteRecordsOptions,
	DeleteRecordsResult,
} from './types.js'
import type { ClusterMetadata } from '@/client/types.js'
import { createCreateTopicsRequest } from '@/protocol/messages/requests/create-topics.js'
import { createDeleteTopicsRequest } from '@/protocol/messages/requests/delete-topics.js'
import type {
	DeleteRecordsRequest,
	DeleteRecordsRequestPartition,
} from '@/protocol/messages/requests/delete-records.js'
import type { DeleteRecordsPartitionResponse } from '@/protocol/messages/responses/delete-records.js'
import { createListGroupsRequest } from '@/protocol/messages/requests/list-groups.js'
import { createDescribeGroupsRequest } from '@/protocol/messages/requests/describe-groups.js'
import { createDeleteGroupsRequest } from '@/protocol/messages/requests/delete-groups.js'
import { OFFSET_TIMESTAMP } from '@/protocol/messages/requests/list-offsets.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	KafkaError,
	KafkaProtocolError,
	LeaderNotAvailableError,
	TimeoutError,
	UnknownTopicOrPartitionError,
	isConnectionFailure,
	isKafkaError,
	isRetriable,
	shouldRefreshMetadata,
} from '@/client/errors.js'
import type { Logger } from '@/logger.js'
import { noopLogger } from '@/logger.js'
import { sleep } from '@/utils/sleep.js'
import { tpKey } from '@/utils/topic-partition.js'
import { DEFAULT_REQUEST_TIMEOUT_MS } from '@/network/types.js'

/** Backoff between attempts of deleteRecords: 100 ms, doubling up to 1 s. */
const DELETE_RECORDS_RETRY_INITIAL_DELAY_MS = 100
const DELETE_RECORDS_RETRY_MAX_DELAY_MS = 1_000

/** Outcome of one deleteRecords attempt for one partition: its new low watermark, or why it failed. */
type DeletionAttempt = { lowWatermark: bigint } | { error: unknown }

/**
 * Map a coordinator lookup/request failure to an error code for per-group results
 */
function errorCodeFor(error: unknown): ErrorCode {
	return isKafkaError(error) ? error.errorCode : ErrorCode.UnknownServerError
}

/** Whether a later attempt may succeed where a deletion failed with `error`. */
function isRetriableDeletion(error: unknown): boolean {
	return isConnectionFailure(error) || isRetriable(error)
}

/** Whether the partition's leader must be looked up again before the next attempt. */
function leaderMayHaveMoved(error: unknown): boolean {
	return isConnectionFailure(error) || (isKafkaError(error) && shouldRefreshMetadata(error.errorCode))
}

/** `error` as a KafkaError; an error of another kind becomes the cause of one. */
function asKafkaError(error: unknown): KafkaError {
	if (isKafkaError(error)) {
		return error
	}
	const wrapped = new KafkaError(error instanceof Error ? error.message : String(error))
	wrapped.cause = error
	return wrapped
}

/** One DeleteRecords request for `records` (all led by the same broker). */
function deleteRecordsRequest(records: readonly RecordsToDelete[], timeoutMs: number): DeleteRecordsRequest {
	const topics = new Map<string, DeleteRecordsRequestPartition[]>()
	for (const { topic, partition, offset } of records) {
		const partitions = topics.get(topic) ?? []
		partitions.push({ partitionIndex: partition, offset })
		topics.set(topic, partitions)
	}
	return { topics: [...topics].map(([name, partitions]) => ({ name, partitions })), timeoutMs }
}

/** The outcome of `record` given the broker's `answer` for it. */
function deletionOutcome(record: RecordsToDelete, answer: DeleteRecordsPartitionResponse | undefined): DeletionAttempt {
	const context = `DeleteRecords for ${record.topic}-${record.partition}`
	if (!answer) {
		return { error: new KafkaProtocolError(ErrorCode.UnknownServerError, `${context}: missing from the response`) }
	}
	if (answer.errorCode !== ErrorCode.None) {
		return { error: new KafkaProtocolError(answer.errorCode, context) }
	}
	return { lowWatermark: answer.lowWatermark }
}

/** Settle as `promise` does, or reject with the reason of `signal` once it aborts; the promise then runs on unobserved. */
function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		const onAbort = () => reject(signal.reason)
		if (signal.aborted) onAbort()
		else signal.addEventListener('abort', onAbort, { once: true })
		void promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort))
	})
}

/**
 * Build a ConsumerGroupDescription entry for a group whose coordinator step failed,
 * so the failure is surfaced in results instead of the group being silently omitted
 */
function failedGroupDescription(groupId: string, error: unknown): ConsumerGroupDescription {
	return {
		groupId,
		state: '',
		protocolType: '',
		protocol: '',
		members: [],
		errorCode: errorCodeFor(error),
	}
}

/**
 * Parse consumer protocol assignment from binary format
 */
function parseConsumerAssignment(buffer: Buffer): TopicPartition[] {
	if (buffer.length === 0) {
		return []
	}

	try {
		// Consumer protocol assignment format:
		// version: INT16
		// topic_count: INT32
		// topics[]: topic_name (STRING), partitions (INT32[])
		// user_data: BYTES

		let offset = 0

		// Skip version
		offset += 2

		// Read topic count
		const topicCount = buffer.readInt32BE(offset)
		offset += 4

		const assignments: TopicPartition[] = []

		for (let i = 0; i < topicCount; i++) {
			// Read topic name length
			const topicNameLength = buffer.readInt16BE(offset)
			offset += 2

			// Read topic name
			const topicName = buffer.toString('utf8', offset, offset + topicNameLength)
			offset += topicNameLength

			// Read partition count
			const partitionCount = buffer.readInt32BE(offset)
			offset += 4

			// Read partitions
			for (let j = 0; j < partitionCount; j++) {
				const partition = buffer.readInt32BE(offset)
				offset += 4
				assignments.push({ topic: topicName, partition })
			}
		}

		return assignments
	} catch {
		// Return empty if parsing fails
		return []
	}
}

/**
 * Admin client for cluster management operations
 *
 * Provides methods for managing topics, consumer groups, and cluster metadata.
 *
 * @example
 * ```typescript
 * const admin = client.admin()
 *
 * // List topics
 * const topics = await admin.listTopics()
 *
 * // Describe topics
 * const descriptions = await admin.describeTopics(['my-topic'])
 *
 * // Delete topics
 * await admin.deleteTopics(['old-topic'])
 *
 * // List consumer groups
 * const groups = await admin.listGroups()
 *
 * // Describe consumer groups
 * const groupDetails = await admin.describeGroups(['my-group'])
 *
 * // Delete consumer groups
 * await admin.deleteGroups(['old-group'])
 * ```
 */
export class Admin {
	private readonly cluster: Cluster
	private readonly config: ResolvedAdminConfig
	private readonly logger: Logger

	constructor(cluster: Cluster, config?: AdminConfig) {
		this.cluster = cluster
		this.config = {
			requestTimeoutMs: config?.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
		}
		this.logger = cluster.getLogger()?.child({ component: 'admin' }) ?? noopLogger
	}

	// ==================== Topic Operations ====================

	/**
	 * Fetch the earliest or latest offsets for a topic's partitions.
	 *
	 * Uses the ListOffsets API against each partition leader. When `isolationLevel` is `read_committed`,
	 * the returned "latest" offsets represent the last stable offset (LSO) rather than the log end offset (LEO),
	 * matching what a `read_committed` consumer can actually read.
	 */
	async fetchTopicOffsets(
		topic: string,
		partitions: number[],
		which: 'earliest' | 'latest',
		options?: { isolationLevel?: 'read_uncommitted' | 'read_committed' }
	): Promise<Map<number, bigint>> {
		if (partitions.length === 0) {
			return new Map()
		}

		const uniquePartitions = [...new Set(partitions)]

		const timestamp = which === 'earliest' ? OFFSET_TIMESTAMP.EARLIEST : OFFSET_TIMESTAMP.LATEST
		const isolationLevel = options?.isolationLevel === 'read_committed' ? 1 : 0

		const offsets = await Promise.all(
			uniquePartitions.map(async partition => {
				const offset = await this.listOffset(topic, partition, timestamp, isolationLevel)
				return [partition, offset] as const
			})
		)

		return new Map(offsets)
	}

	/**
	 * List all topic names in the cluster
	 *
	 * Uses the Metadata API to fetch topic list.
	 *
	 * @returns Array of topic names
	 */
	async listTopics(): Promise<string[]> {
		this.logger.debug('listing topics')

		// Refresh metadata with all topics
		const metadata = await this.cluster.refreshMetadata()

		const topics = Array.from(metadata.topics.keys())
		this.logger.debug('listed topics', { count: topics.length })

		return topics
	}

	/**
	 * Describe topics with detailed metadata
	 *
	 * @param topics - Topic names to describe (if empty, describes all topics)
	 * @returns Array of topic descriptions
	 */
	async describeTopics(topics?: string[]): Promise<TopicDescription[]> {
		this.logger.debug('describing topics', { topics })

		// Refresh metadata for specified topics or all
		const metadata = await this.cluster.refreshMetadata(topics)

		const descriptions: TopicDescription[] = []

		const topicsToDescribe = topics ?? Array.from(metadata.topics.keys())

		for (const topicName of topicsToDescribe) {
			const topicMetadata = metadata.topics.get(topicName)
			if (topicMetadata) {
				const partitions: PartitionInfo[] = []

				for (const [, partitionMetadata] of topicMetadata.partitions) {
					partitions.push({
						partitionIndex: partitionMetadata.partitionIndex,
						leaderId: partitionMetadata.leaderId,
						leaderEpoch: partitionMetadata.leaderEpoch,
						replicas: partitionMetadata.replicaNodes,
						isr: partitionMetadata.isrNodes,
						offlineReplicas: partitionMetadata.offlineReplicas,
					})
				}

				// Sort partitions by index
				partitions.sort((a, b) => a.partitionIndex - b.partitionIndex)

				descriptions.push({
					name: topicMetadata.name,
					topicId: topicMetadata.topicId,
					isInternal: topicMetadata.isInternal,
					partitions,
				})
			}
		}

		this.logger.debug('described topics', { count: descriptions.length })
		return descriptions
	}

	/**
	 * Create topics in the cluster
	 *
	 * Sends the request to the controller broker.
	 *
	 * @param topics - Topics to create
	 * @param options - Optional settings
	 * @returns Results for each topic
	 */
	async createTopics(
		topics: Array<{
			name: string
			numPartitions?: number
			replicationFactor?: number
			configs?: Record<string, string>
		}>,
		options?: { timeoutMs?: number; validateOnly?: boolean }
	): Promise<CreateTopicsResult[]> {
		this.logger.debug('creating topics', { topics: topics.map(t => t.name) })

		const controller = await this.cluster.getControllerBroker()

		const request = createCreateTopicsRequest(topics)
		request.timeoutMs = options?.timeoutMs ?? this.config.requestTimeoutMs
		request.validateOnly = options?.validateOnly ?? false

		const response = await controller.createTopics(request)

		const results: CreateTopicsResult[] = response.topics.map(t => ({
			name: t.name,
			topicId: t.topicId,
			errorCode: t.errorCode,
			errorMessage: t.errorMessage,
			numPartitions: t.numPartitions,
			replicationFactor: t.replicationFactor,
		}))

		for (const result of results) {
			if (result.errorCode !== ErrorCode.None) {
				this.logger.warn('topic creation failed', {
					topic: result.name,
					errorCode: result.errorCode,
					errorMessage: result.errorMessage,
				})
			}
		}

		this.logger.debug('created topics', { count: results.length })
		return results
	}

	/**
	 * Delete topics from the cluster
	 *
	 * Sends the request to the controller broker.
	 *
	 * Per-topic failures are reported in each result's `errorCode` (not thrown), consistent
	 * with the other admin operations.
	 *
	 * @param topics - Topic names to delete
	 * @param options - Optional timeout settings
	 * @returns Results for each topic, including a per-topic `errorCode`
	 */
	async deleteTopics(topics: string[], options?: { timeoutMs?: number }): Promise<DeleteTopicsResult[]> {
		this.logger.debug('deleting topics', { topics })

		const controller = await this.cluster.getControllerBroker()

		const request = createDeleteTopicsRequest(topics, {
			timeoutMs: options?.timeoutMs ?? this.config.requestTimeoutMs,
		})

		const response = await controller.deleteTopics(request)

		const results: DeleteTopicsResult[] = response.responses.map(r => ({
			name: r.name,
			errorCode: r.errorCode,
			errorMessage: r.errorMessage,
		}))

		// Check for errors
		for (const result of results) {
			if (result.errorCode !== ErrorCode.None) {
				this.logger.warn('topic deletion failed', {
					topic: result.name,
					errorCode: result.errorCode,
					errorMessage: result.errorMessage,
				})
			}
		}

		this.logger.debug('deleted topics', { count: results.length })
		return results
	}

	/**
	 * Delete the records of partitions before the given offsets, moving each partition's log start offset forward.
	 *
	 * Partitions are sent to their leaders, one request per leader. A moved leader, an unknown topic or partition,
	 * other retriable broker answers and connection failures are retried with backoff (100 ms doubling to 1 s), the
	 * leaders being looked up again where they may have moved, until `timeoutMs` is spent. Failures are returned per
	 * partition, not thrown: the broker's answer as a `KafkaProtocolError` (e.g. `OffsetOutOfRange` for an offset
	 * beyond the high watermark), or a `TimeoutError` (cause: the last failure) for a partition still pending at the
	 * deadline. Aborting `options.signal` rejects the call with its reason.
	 *
	 * @param records - Partitions and offsets; a partition listed twice is deleted up to its last offset
	 * @param options - Time budget and abort signal
	 * @returns One result per partition, in the order the partitions were first listed
	 */
	async deleteRecords(
		records: readonly RecordsToDelete[],
		options: DeleteRecordsOptions = {}
	): Promise<DeleteRecordsResult[]> {
		const { signal } = options
		signal?.throwIfAborted()
		const timeoutMs = options.timeoutMs ?? this.config.requestTimeoutMs
		const deadline = Date.now() + timeoutMs

		const pending = new Map(records.map(record => [tpKey(record.topic, record.partition), record] as const))
		const order = [...pending.keys()]
		const results = new Map<string, DeleteRecordsResult>()
		const lastErrors = new Map<string, unknown>()
		this.logger.debug('deleting records', { partitions: pending.size, timeoutMs })

		// Aborted at the deadline or by the caller's signal; the caller's is checked first when telling them apart.
		const stop = new AbortController()
		const timer = setTimeout(() => stop.abort(), timeoutMs)
		const onAbort = () => stop.abort()
		signal?.addEventListener('abort', onAbort, { once: true })
		try {
			let relocate = new Set<string>()
			let delayMs = DELETE_RECORDS_RETRY_INITIAL_DELAY_MS
			while (pending.size > 0) {
				const attempt = this.attemptDeleteRecords(
					[...pending.values()],
					relocate,
					Math.max(0, deadline - Date.now())
				)
				const outcomes = await abortable(attempt, stop.signal)
				relocate = new Set()
				for (const [key, outcome] of outcomes) {
					const { topic, partition } = pending.get(key)!
					if ('lowWatermark' in outcome) {
						results.set(key, { topic, partition, lowWatermark: outcome.lowWatermark })
						pending.delete(key)
					} else if (isRetriableDeletion(outcome.error)) {
						lastErrors.set(key, outcome.error)
						if (leaderMayHaveMoved(outcome.error)) relocate.add(topic)
					} else {
						results.set(key, { topic, partition, error: asKafkaError(outcome.error) })
						pending.delete(key)
					}
				}
				if (pending.size > 0) {
					this.logger.debug('retrying deleteRecords', { partitions: pending.size, delayMs })
					await sleep(delayMs, { signal: stop.signal })
					delayMs = Math.min(2 * delayMs, DELETE_RECORDS_RETRY_MAX_DELAY_MS)
				}
			}
		} catch (error) {
			signal?.throwIfAborted()
			if (!stop.signal.aborted) throw error
		} finally {
			clearTimeout(timer)
			signal?.removeEventListener('abort', onAbort)
		}

		for (const [key, { topic, partition }] of pending) {
			const error = new TimeoutError(`DeleteRecords for ${topic}-${partition} did not complete`, timeoutMs)
			if (lastErrors.has(key)) error.cause = lastErrors.get(key)
			results.set(key, { topic, partition, error })
		}
		return order.map(key => results.get(key)!)
	}

	/**
	 * One attempt of {@link deleteRecords}: one DeleteRecords request per leader, sent concurrently. The leaders of
	 * the topics in `relocate`, and of partitions the metadata cache has none for, are looked up again first.
	 * Never rejects: a failure becomes the outcome of the partitions it concerns.
	 */
	private async attemptDeleteRecords(
		records: readonly RecordsToDelete[],
		relocate: ReadonlySet<string>,
		timeoutMs: number
	): Promise<Map<string, DeletionAttempt>> {
		const outcomes = new Map<string, DeletionAttempt>()
		let metadata: ClusterMetadata | null
		try {
			metadata = await this.metadataWithLeaders(records, relocate)
		} catch (error) {
			for (const record of records) outcomes.set(tpKey(record.topic, record.partition), { error })
			return outcomes
		}

		const byLeader = new Map<number, RecordsToDelete[]>()
		for (const record of records) {
			const { topic, partition } = record
			const partitionMetadata = metadata?.topics.get(topic)?.partitions.get(partition)
			if (!partitionMetadata) {
				outcomes.set(tpKey(topic, partition), { error: new UnknownTopicOrPartitionError(topic, partition) })
			} else if (partitionMetadata.leaderId < 0) {
				outcomes.set(tpKey(topic, partition), { error: new LeaderNotAvailableError(topic, partition) })
			} else {
				const led = byLeader.get(partitionMetadata.leaderId) ?? []
				led.push(record)
				byLeader.set(partitionMetadata.leaderId, led)
			}
		}

		await Promise.all(
			[...byLeader].map(async ([leaderId, led]) => {
				try {
					const leader = await this.cluster.getBroker(leaderId)
					const response = await leader.deleteRecords(deleteRecordsRequest(led, timeoutMs))
					const answers = new Map(
						response.topics.flatMap(t =>
							t.partitions.map(p => [tpKey(t.name, p.partitionIndex), p] as const)
						)
					)
					for (const record of led) {
						const key = tpKey(record.topic, record.partition)
						outcomes.set(key, deletionOutcome(record, answers.get(key)))
					}
				} catch (error) {
					for (const record of led) outcomes.set(tpKey(record.topic, record.partition), { error })
				}
			})
		)
		return outcomes
	}

	/** The cluster metadata, refreshed first for `relocate` and for the topics of partitions it knows no leader of. */
	private async metadataWithLeaders(
		records: readonly RecordsToDelete[],
		relocate: ReadonlySet<string>
	): Promise<ClusterMetadata | null> {
		const cached = this.cluster.getMetadata()
		const topics = new Set(relocate)
		for (const { topic, partition } of records) {
			if ((cached?.topics.get(topic)?.partitions.get(partition)?.leaderId ?? -1) < 0) topics.add(topic)
		}
		return topics.size === 0 ? cached : this.cluster.refreshMetadata([...topics])
	}

	// ==================== Cluster Operations ====================

	/**
	 * Describe the cluster
	 *
	 * Returns cluster ID, controller, and broker information.
	 *
	 * @returns Cluster description
	 */
	async describeCluster(): Promise<ClusterDescription> {
		this.logger.debug('describing cluster')

		const metadata = await this.cluster.refreshMetadata()

		const brokers: BrokerDescription[] = []
		for (const [, brokerInfo] of metadata.brokers) {
			brokers.push({
				nodeId: brokerInfo.nodeId,
				host: brokerInfo.host,
				port: brokerInfo.port,
				rack: brokerInfo.rack,
			})
		}

		// Sort brokers by node ID
		brokers.sort((a, b) => a.nodeId - b.nodeId)

		const description: ClusterDescription = {
			clusterId: metadata.clusterId,
			controllerId: metadata.controllerId,
			brokers,
		}

		this.logger.debug('described cluster', {
			clusterId: description.clusterId,
			controllerId: description.controllerId,
			brokerCount: brokers.length,
		})

		return description
	}

	// ==================== Consumer Group Operations ====================

	/**
	 * List all consumer groups
	 *
	 * Queries all brokers and aggregates results.
	 *
	 * @param options - Filter options
	 * @returns Array of consumer group listings
	 */
	async listGroups(options?: { statesFilter?: string[] }): Promise<ConsumerGroupListing[]> {
		this.logger.debug('listing groups', { statesFilter: options?.statesFilter })

		// Get metadata to find all brokers
		const metadata = await this.cluster.refreshMetadata()

		const allGroups = new Map<string, ConsumerGroupListing>()

		// Query each broker for groups
		for (const [nodeId] of metadata.brokers) {
			try {
				const broker = await this.cluster.getBroker(nodeId)
				const request = createListGroupsRequest(options)
				const response = await broker.listGroups(request)

				if (response.errorCode !== ErrorCode.None) {
					this.logger.warn('list groups failed on broker', {
						nodeId,
						errorCode: response.errorCode,
					})
					continue
				}

				for (const group of response.groups) {
					// Deduplicate by groupId
					if (!allGroups.has(group.groupId)) {
						allGroups.set(group.groupId, {
							groupId: group.groupId,
							protocolType: group.protocolType,
							state: group.groupState,
						})
					}
				}
			} catch (error) {
				this.logger.warn('failed to query broker for groups', {
					nodeId,
					error: (error as Error).message,
				})
			}
		}

		const groups = Array.from(allGroups.values())
		this.logger.debug('listed groups', { count: groups.length })

		return groups
	}

	/**
	 * Describe consumer groups with detailed information
	 *
	 * Routes requests to the appropriate coordinator for each group.
	 *
	 * @param groupIds - Group IDs to describe
	 * @returns Array of consumer group descriptions
	 */
	async describeGroups(groupIds: string[]): Promise<ConsumerGroupDescription[]> {
		this.logger.debug('describing groups', { groupIds })

		const descriptions: ConsumerGroupDescription[] = []

		// Group requests by coordinator
		const groupsByCoordinator = new Map<number, string[]>()

		for (const groupId of groupIds) {
			try {
				const coordinator = await this.cluster.getCoordinator('GROUP', groupId)
				const nodeId = coordinator.nodeId

				const existing = groupsByCoordinator.get(nodeId) ?? []
				existing.push(groupId)
				groupsByCoordinator.set(nodeId, existing)
			} catch (error) {
				this.logger.warn('failed to find coordinator for group', {
					groupId,
					error: (error as Error).message,
				})
				descriptions.push(failedGroupDescription(groupId, error))
			}
		}

		// Send requests to each coordinator
		for (const [nodeId, groups] of groupsByCoordinator) {
			try {
				const broker = await this.cluster.getBroker(nodeId)
				const request = createDescribeGroupsRequest(groups)
				const response = await broker.describeGroups(request)

				for (const group of response.groups) {
					const members: MemberDescription[] = []

					for (const member of group.members) {
						const assignment = parseConsumerAssignment(member.memberAssignment)

						members.push({
							memberId: member.memberId,
							groupInstanceId: member.groupInstanceId,
							clientId: member.clientId,
							clientHost: member.clientHost,
							assignment,
						})
					}

					descriptions.push({
						groupId: group.groupId,
						state: group.groupState,
						protocolType: group.protocolType,
						protocol: group.protocolData,
						members,
						errorCode: group.errorCode,
					})
				}
			} catch (error) {
				this.logger.warn('failed to describe groups from coordinator', {
					nodeId,
					groups,
					error: (error as Error).message,
				})
				for (const groupId of groups) {
					descriptions.push(failedGroupDescription(groupId, error))
				}
			}
		}

		this.logger.debug('described groups', { count: descriptions.length })
		return descriptions
	}

	/**
	 * Delete consumer groups
	 *
	 * Groups must be empty (no active members) to be deleted.
	 * Routes requests to the appropriate coordinator for each group.
	 *
	 * @param groupIds - Group IDs to delete
	 * @returns Results for each group
	 */
	async deleteGroups(groupIds: string[]): Promise<DeleteGroupsResult[]> {
		this.logger.debug('deleting groups', { groupIds })

		const results: DeleteGroupsResult[] = []

		// Group requests by coordinator
		const groupsByCoordinator = new Map<number, string[]>()

		for (const groupId of groupIds) {
			try {
				const coordinator = await this.cluster.getCoordinator('GROUP', groupId)
				const nodeId = coordinator.nodeId

				const existing = groupsByCoordinator.get(nodeId) ?? []
				existing.push(groupId)
				groupsByCoordinator.set(nodeId, existing)
			} catch (error) {
				this.logger.warn('failed to find coordinator for group', {
					groupId,
					error: (error as Error).message,
				})
				results.push({ groupId, errorCode: errorCodeFor(error) })
			}
		}

		// Send requests to each coordinator
		for (const [nodeId, groups] of groupsByCoordinator) {
			try {
				const broker = await this.cluster.getBroker(nodeId)
				const request = createDeleteGroupsRequest(groups)
				const response = await broker.deleteGroups(request)

				for (const result of response.results) {
					results.push({
						groupId: result.groupId,
						errorCode: result.errorCode,
					})

					if (result.errorCode !== ErrorCode.None) {
						this.logger.warn('group deletion failed', {
							groupId: result.groupId,
							errorCode: result.errorCode,
						})
					}
				}
			} catch (error) {
				this.logger.warn('failed to delete groups from coordinator', {
					nodeId,
					groups,
					error: (error as Error).message,
				})
				for (const groupId of groups) {
					results.push({ groupId, errorCode: errorCodeFor(error) })
				}
			}
		}

		this.logger.debug('deleted groups', { count: results.length })
		return results
	}

	// ==================== ACL Operations ====================

	/**
	 * Describe ACLs matching a filter
	 *
	 * Queries the cluster for ACLs that match the specified filter criteria.
	 * Use AclResourceType.ANY, AclOperation.ANY, etc. to match all values.
	 *
	 * @param filter - Filter criteria for ACLs to describe
	 * @returns Resources with their matching ACLs
	 *
	 * @example
	 * ```typescript
	 * import { AclResourceType, AclResourcePatternType, AclOperation, AclPermissionType } from '@kafkats/client'
	 *
	 * // Describe all ACLs for topics
	 * const result = await admin.describeAcls({
	 *   resourceTypeFilter: AclResourceType.TOPIC,
	 *   resourceNameFilter: null,
	 *   patternTypeFilter: AclResourcePatternType.ANY,
	 *   principalFilter: null,
	 *   hostFilter: null,
	 *   operation: AclOperation.ANY,
	 *   permissionType: AclPermissionType.ANY,
	 * })
	 *
	 * // Describe ACLs for a specific topic
	 * const result = await admin.describeAcls({
	 *   resourceTypeFilter: AclResourceType.TOPIC,
	 *   resourceNameFilter: 'my-topic',
	 *   patternTypeFilter: AclResourcePatternType.LITERAL,
	 *   principalFilter: null,
	 *   hostFilter: null,
	 *   operation: AclOperation.ANY,
	 *   permissionType: AclPermissionType.ANY,
	 * })
	 * ```
	 */
	async describeAcls(filter: AclBindingFilter): Promise<DescribeAclsResult> {
		this.logger.debug('describing ACLs', { filter })

		// ACL operations go to any broker (typically controller)
		const controller = await this.cluster.getControllerBroker()

		const response = await controller.describeAcls({
			resourceTypeFilter: filter.resourceTypeFilter,
			resourceNameFilter: filter.resourceNameFilter,
			patternTypeFilter: filter.patternTypeFilter,
			principalFilter: filter.principalFilter,
			hostFilter: filter.hostFilter,
			operation: filter.operation,
			permissionType: filter.permissionType,
		})

		if (response.errorCode !== ErrorCode.None) {
			this.logger.warn('describe ACLs failed', {
				errorCode: response.errorCode,
				errorMessage: response.errorMessage,
			})
		}

		const result: DescribeAclsResult = {
			errorCode: response.errorCode,
			errorMessage: response.errorMessage,
			resources: response.resources.map(r => ({
				resourceType: r.resourceType,
				resourceName: r.resourceName,
				patternType: r.patternType,
				acls: r.acls.map(a => ({
					principal: a.principal,
					host: a.host,
					operation: a.operation,
					permissionType: a.permissionType,
				})),
			})),
		}

		this.logger.debug('described ACLs', { resourceCount: result.resources.length })
		return result
	}

	/**
	 * Create ACL bindings
	 *
	 * Creates one or more ACL bindings in the cluster.
	 *
	 * @param acls - ACL bindings to create
	 * @returns Results for each ACL creation (in the same order as input)
	 *
	 * @example
	 * ```typescript
	 * import { AclResourceType, AclResourcePatternType, AclOperation, AclPermissionType } from '@kafkats/client'
	 *
	 * // Allow User:alice to read from my-topic
	 * const results = await admin.createAcls([{
	 *   resourceType: AclResourceType.TOPIC,
	 *   resourceName: 'my-topic',
	 *   resourcePatternType: AclResourcePatternType.LITERAL,
	 *   principal: 'User:alice',
	 *   host: '*',
	 *   operation: AclOperation.READ,
	 *   permissionType: AclPermissionType.ALLOW,
	 * }])
	 *
	 * // Allow User:bob to write to all topics with prefix "data-"
	 * const results = await admin.createAcls([{
	 *   resourceType: AclResourceType.TOPIC,
	 *   resourceName: 'data-',
	 *   resourcePatternType: AclResourcePatternType.PREFIXED,
	 *   principal: 'User:bob',
	 *   host: '*',
	 *   operation: AclOperation.WRITE,
	 *   permissionType: AclPermissionType.ALLOW,
	 * }])
	 * ```
	 */
	async createAcls(acls: AclBinding[]): Promise<CreateAclResult[]> {
		this.logger.debug('creating ACLs', { count: acls.length })

		const controller = await this.cluster.getControllerBroker()

		const response = await controller.createAcls({
			creations: acls.map(acl => ({
				resourceType: acl.resourceType,
				resourceName: acl.resourceName,
				resourcePatternType: acl.resourcePatternType,
				principal: acl.principal,
				host: acl.host,
				operation: acl.operation,
				permissionType: acl.permissionType,
			})),
		})

		const results: CreateAclResult[] = response.results.map(r => ({
			errorCode: r.errorCode,
			errorMessage: r.errorMessage,
		}))

		for (let i = 0; i < results.length; i++) {
			const result = results[i]!
			if (result.errorCode !== ErrorCode.None) {
				this.logger.warn('ACL creation failed', {
					index: i,
					errorCode: result.errorCode,
					errorMessage: result.errorMessage,
				})
			}
		}

		this.logger.debug('created ACLs', { count: results.length })
		return results
	}

	/**
	 * Delete ACLs matching filters
	 *
	 * Deletes all ACLs that match the specified filter criteria.
	 * Use AclResourceType.ANY, AclOperation.ANY, etc. to match all values.
	 *
	 * @param filters - Filters for ACLs to delete
	 * @returns Results for each filter with the ACLs that were deleted
	 *
	 * @example
	 * ```typescript
	 * import { AclResourceType, AclResourcePatternType, AclOperation, AclPermissionType } from '@kafkats/client'
	 *
	 * // Delete all ACLs for User:alice on my-topic
	 * const results = await admin.deleteAcls([{
	 *   resourceTypeFilter: AclResourceType.TOPIC,
	 *   resourceNameFilter: 'my-topic',
	 *   patternTypeFilter: AclResourcePatternType.LITERAL,
	 *   principalFilter: 'User:alice',
	 *   hostFilter: null,
	 *   operation: AclOperation.ANY,
	 *   permissionType: AclPermissionType.ANY,
	 * }])
	 *
	 * // Each result contains the ACLs that were matched and deleted
	 * for (const result of results) {
	 *   console.log(`Deleted ${result.matchingAcls.length} ACLs`)
	 * }
	 * ```
	 */
	async deleteAcls(filters: AclBindingFilter[]): Promise<DeleteAclsFilterResult[]> {
		this.logger.debug('deleting ACLs', { filterCount: filters.length })

		const controller = await this.cluster.getControllerBroker()

		const response = await controller.deleteAcls({
			filters: filters.map(f => ({
				resourceTypeFilter: f.resourceTypeFilter,
				resourceNameFilter: f.resourceNameFilter,
				patternTypeFilter: f.patternTypeFilter,
				principalFilter: f.principalFilter,
				hostFilter: f.hostFilter,
				operation: f.operation,
				permissionType: f.permissionType,
			})),
		})

		const results: DeleteAclsFilterResult[] = response.filterResults.map(fr => ({
			errorCode: fr.errorCode,
			errorMessage: fr.errorMessage,
			matchingAcls: fr.matchingAcls.map(ma => ({
				resourceType: ma.resourceType,
				resourceName: ma.resourceName,
				resourcePatternType: ma.patternType,
				principal: ma.principal,
				host: ma.host,
				operation: ma.operation,
				permissionType: ma.permissionType,
			})),
		}))

		for (const result of results) {
			if (result.errorCode !== ErrorCode.None) {
				this.logger.warn('ACL deletion failed', {
					errorCode: result.errorCode,
					errorMessage: result.errorMessage,
				})
			}
		}

		this.logger.debug('deleted ACLs', { filterCount: results.length })
		return results
	}

	/**
	 * List a single offset for a topic partition with retries and metadata refresh.
	 */
	private async listOffset(
		topic: string,
		partition: number,
		timestamp: bigint,
		isolationLevel: number
	): Promise<bigint> {
		const maxAttempts = 5
		let lastError: unknown

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			try {
				const leader = await this.cluster.getLeaderForPartition(topic, partition)

				const response = await leader.listOffsets({
					isolationLevel,
					topics: [
						{
							name: topic,
							partitions: [
								{
									partitionIndex: partition,
									timestamp,
								},
							],
						},
					],
				})

				const topicResponse = response.topics.find(t => t.name === topic)
				const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === partition)

				if (!partitionResponse) {
					throw new Error(`No offset response for ${topic}-${partition}`)
				}

				if (partitionResponse.errorCode !== ErrorCode.None) {
					throw new KafkaProtocolError(
						partitionResponse.errorCode,
						`ListOffsets failed for ${topic}-${partition}`
					)
				}

				return partitionResponse.offset
			} catch (error) {
				lastError = error

				const retriable = isKafkaError(error) && error.retriable
				if (!retriable || attempt >= maxAttempts) {
					throw error
				}

				const errorCode = isKafkaError(error) ? error.errorCode : undefined
				if (errorCode !== undefined && shouldRefreshMetadata(errorCode)) {
					this.logger.debug('refreshing metadata due to listOffsets error', {
						topic,
						partition,
						errorCode,
						attempt,
					})
					await this.cluster.refreshMetadata([topic]).catch(() => {})
				}

				const delayMs = Math.min(100 * 2 ** (attempt - 1), 2000)
				this.logger.debug('retrying listOffsets after error', {
					topic,
					partition,
					attempt,
					delayMs,
					error: error instanceof Error ? error.message : String(error),
				})
				await sleep(delayMs)
			}
		}

		throw lastError instanceof Error ? lastError : new Error(`ListOffsets failed for ${topic}-${partition}`)
	}
}
