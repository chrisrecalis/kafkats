/**
 * Admin client for Kafka cluster management operations
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import type {
	AdminConfig,
	ResolvedAdminConfig,
	TopicDescription,
	PartitionInfo,
	ConsumerGroupListing,
	ConsumerGroupDescription,
	ConsumerGroupOffset,
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
} from './types.js'
import { createCreateTopicsRequest } from '@/protocol/messages/requests/create-topics.js'
import { createDeleteTopicsRequest } from '@/protocol/messages/requests/delete-topics.js'
import { createListGroupsRequest } from '@/protocol/messages/requests/list-groups.js'
import { createDescribeGroupsRequest } from '@/protocol/messages/requests/describe-groups.js'
import { createDeleteGroupsRequest } from '@/protocol/messages/requests/delete-groups.js'
import { OFFSET_TIMESTAMP } from '@/protocol/messages/requests/list-offsets.js'
import type { OffsetFetchTopic } from '@/protocol/messages/requests/offset-fetch.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError, isKafkaError, shouldRefreshMetadata } from '@/client/errors.js'
import type { Logger } from '@/logger.js'
import { noopLogger } from '@/logger.js'
import { sleep } from '@/utils/sleep.js'
import { retry } from '@/utils/retry.js'
import { DEFAULT_REQUEST_TIMEOUT_MS } from '@/network/types.js'

/**
 * Map a coordinator lookup/request failure to an error code for per-group results
 */
function errorCodeFor(error: unknown): ErrorCode {
	return isKafkaError(error) ? error.errorCode : ErrorCode.UnknownServerError
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
	 * Partitions are grouped by leader and queried with one ListOffsets request per leader, so the
	 * cost is bounded by broker count rather than partition count. Partitions that fail with a
	 * retriable error (leader moved, not available) are retried with refreshed metadata; any other
	 * error fails the call.
	 *
	 * When `isolationLevel` is `read_committed`, the returned "latest" offsets represent the last
	 * stable offset (LSO) rather than the log end offset (LEO), matching what a `read_committed`
	 * consumer can actually read.
	 */
	async fetchTopicOffsets(
		topic: string,
		partitions: number[],
		which: 'earliest' | 'latest',
		options?: { isolationLevel?: 'read_uncommitted' | 'read_committed' }
	): Promise<Map<number, bigint>> {
		const result = new Map<number, bigint>()
		let pending = [...new Set(partitions)]
		if (pending.length === 0) return result

		const timestamp = which === 'earliest' ? OFFSET_TIMESTAMP.EARLIEST : OFFSET_TIMESTAMP.LATEST
		const isolationLevel = options?.isolationLevel === 'read_committed' ? 1 : 0
		const maxAttempts = 5
		let lastError: unknown

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			const byLeader = new Map<number, { leader: Broker; partitions: number[] }>()
			const retry: number[] = []
			for (const partition of pending) {
				try {
					const leader = await this.cluster.getLeaderForPartition(topic, partition)
					const group = byLeader.get(leader.nodeId) ?? { leader, partitions: [] }
					group.partitions.push(partition)
					byLeader.set(leader.nodeId, group)
				} catch (error) {
					if (!(isKafkaError(error) && error.retriable)) throw error
					lastError = error
					retry.push(partition)
				}
			}

			await Promise.all(
				Array.from(byLeader.values(), async ({ leader, partitions: batch }) => {
					let response
					try {
						response = await leader.listOffsets({
							isolationLevel,
							topics: [
								{
									name: topic,
									partitions: batch.map(partitionIndex => ({ partitionIndex, timestamp })),
								},
							],
						})
					} catch (error) {
						if (!(isKafkaError(error) && error.retriable)) throw error
						lastError = error
						retry.push(...batch)
						return
					}
					const answered = new Map(
						response.topics
							.find(t => t.name === topic)
							?.partitions.map(p => [p.partitionIndex, p] as const) ?? []
					)
					for (const partition of batch) {
						const p = answered.get(partition)
						if (!p) {
							lastError = new Error(`No offset response for ${topic}-${partition}`)
							retry.push(partition)
							continue
						}
						if (p.errorCode === ErrorCode.None) {
							result.set(partition, p.offset)
							continue
						}
						const error = new KafkaProtocolError(
							p.errorCode,
							`ListOffsets failed for ${topic}-${partition}`
						)
						if (!error.retriable) throw error
						lastError = error
						retry.push(partition)
					}
				})
			)

			if (retry.length === 0) return result
			pending = retry
			if (attempt >= maxAttempts) break

			const errorCode = isKafkaError(lastError) ? lastError.errorCode : undefined
			if (errorCode === undefined || shouldRefreshMetadata(errorCode)) {
				this.logger.debug('refreshing metadata due to listOffsets error', {
					topic,
					partitions: pending.length,
					errorCode,
					attempt,
				})
				await this.cluster.refreshMetadata([topic]).catch(() => {})
			}
			const delayMs = Math.min(100 * 2 ** (attempt - 1), 2000)
			this.logger.debug('retrying listOffsets after error', {
				topic,
				partitions: pending.length,
				attempt,
				delayMs,
				error: lastError instanceof Error ? lastError.message : String(lastError),
			})
			await sleep(delayMs)
		}

		throw lastError instanceof Error
			? lastError
			: new Error(`ListOffsets failed for ${topic} partitions ${pending.join(', ')}`)
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
	 * Queries all brokers and aggregates results. By default a broker that cannot be queried is
	 * logged and skipped, so the result may be missing the groups it coordinates.
	 *
	 * @param options.statesFilter - Only return groups in these states
	 * @param options.strict - Throw instead of returning a partial result when any broker fails
	 * @returns Array of consumer group listings
	 */
	async listGroups(options?: { statesFilter?: string[]; strict?: boolean }): Promise<ConsumerGroupListing[]> {
		this.logger.debug('listing groups', { statesFilter: options?.statesFilter })

		// Get metadata to find all brokers
		const metadata = await this.cluster.refreshMetadata()

		const allGroups = new Map<string, ConsumerGroupListing>()
		const failures: Array<{ nodeId: number; error: Error }> = []

		// Query each broker for groups
		for (const [nodeId] of metadata.brokers) {
			try {
				const broker = await this.cluster.getBroker(nodeId)
				const request = createListGroupsRequest({ statesFilter: options?.statesFilter })
				const response = await broker.listGroups(request)

				if (response.errorCode !== ErrorCode.None) {
					throw new KafkaProtocolError(response.errorCode, `ListGroups failed on broker ${nodeId}`)
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
				failures.push({ nodeId, error: error as Error })
				this.logger.warn('failed to query broker for groups', {
					nodeId,
					error: (error as Error).message,
				})
			}
		}

		if (options?.strict && failures.length > 0) {
			const detail = failures.map(f => `broker ${f.nodeId}: ${f.error.message}`).join('; ')
			throw new Error(`ListGroups failed on ${failures.length} of ${metadata.brokers.size} brokers (${detail})`, {
				cause: failures[0]!.error,
			})
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
	 * List the committed offsets of a consumer group
	 *
	 * Uses the OffsetFetch API against the group's coordinator. The group does not need
	 * active members; offsets committed by a group that has since gone empty are returned
	 * until the broker expires them (`offsets.retention.minutes`).
	 *
	 * Partitions without a committed offset are reported with `offset: null`.
	 *
	 * @param groupId - Consumer group ID
	 * @param partitions - Partitions to look up; omit to return every partition the group has committed
	 * @param options.requireStable - Wait for pending transactional commits (KIP-447) instead of returning
	 *   the previous position. Defaults to `false`; monitoring callers rarely need it.
	 * @throws KafkaProtocolError if the coordinator rejects the request (e.g. GroupAuthorizationFailed)
	 */
	async listConsumerGroupOffsets(
		groupId: string,
		partitions?: TopicPartition[],
		options?: { requireStable?: boolean }
	): Promise<ConsumerGroupOffset[]> {
		this.logger.debug('listing consumer group offsets', { groupId, partitions: partitions?.length ?? 'all' })

		let topics: OffsetFetchTopic[] | null = null
		if (partitions) {
			if (partitions.length === 0) {
				return []
			}
			const byTopic = new Map<string, Set<number>>()
			for (const tp of partitions) {
				const set = byTopic.get(tp.topic) ?? new Set<number>()
				set.add(tp.partition)
				byTopic.set(tp.topic, set)
			}
			topics = Array.from(byTopic, ([name, parts]) => ({
				name,
				partitions: Array.from(parts, partitionIndex => ({ partitionIndex })),
			}))
		}

		// Coordinator lookups and OffsetFetch both return transient codes while the coordinator is
		// loading or moving (CoordinatorNotAvailable, CoordinatorLoadInProgress, NotCoordinator), and
		// requireStable adds UnstableOffsetCommit while a transactional commit is pending. Retry those
		// within the request timeout, re-discovering the coordinator when it has moved.
		const response = await retry(
			async () => {
				const coordinator = await this.cluster.getCoordinator('GROUP', groupId)
				const res = await coordinator.offsetFetch({
					groupId,
					topics,
					requireStable: options?.requireStable ?? false,
				})
				if (res.errorCode !== ErrorCode.None) {
					throw new KafkaProtocolError(res.errorCode, `OffsetFetch failed for group ${groupId}`)
				}
				for (const topic of res.topics) {
					for (const partition of topic.partitions) {
						if (partition.errorCode !== ErrorCode.None) {
							throw new KafkaProtocolError(
								partition.errorCode,
								`OffsetFetch failed for group ${groupId} on ${topic.name}-${partition.partitionIndex}`
							)
						}
					}
				}
				return res
			},
			{
				maxAttempts: 1_000,
				maxElapsedMs: this.config.requestTimeoutMs,
				initialDelayMs: 100,
				maxDelayMs: 1_000,
				multiplier: 2,
				jitter: 0,
				shouldRetry: error => isKafkaError(error) && error.retriable,
				onRetry: ({ error }) => {
					const code = isKafkaError(error) ? error.errorCode : undefined
					if (code === ErrorCode.NotCoordinator || code === ErrorCode.CoordinatorNotAvailable) {
						this.cluster.invalidateCoordinator('GROUP', groupId)
					}
				},
			}
		)

		const offsets: ConsumerGroupOffset[] = []
		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				offsets.push({
					topic: topic.name,
					partition: partition.partitionIndex,
					offset: partition.committedOffset < 0n ? null : partition.committedOffset,
					leaderEpoch: partition.committedLeaderEpoch,
					metadata: partition.metadata,
				})
			}
		}

		this.logger.debug('listed consumer group offsets', { groupId, count: offsets.length })
		return offsets
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
}
