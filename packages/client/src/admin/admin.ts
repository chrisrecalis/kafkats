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
	DeleteTopicsResult,
	DeleteGroupsResult,
} from './types.js'
import { createDeleteTopicsRequest } from '@/protocol/messages/requests/delete-topics.js'
import { createListGroupsRequest } from '@/protocol/messages/requests/list-groups.js'
import { createDescribeGroupsRequest } from '@/protocol/messages/requests/describe-groups.js'
import { createDeleteGroupsRequest } from '@/protocol/messages/requests/delete-groups.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { Logger } from '@/logger.js'
import { noopLogger } from '@/logger.js'

const DEFAULT_REQUEST_TIMEOUT_MS = 30000

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
	 * Delete topics from the cluster
	 *
	 * Sends the request to the controller broker.
	 *
	 * @param topics - Topic names to delete
	 * @param options - Optional timeout settings
	 * @returns Results for each topic
	 * @throws KafkaProtocolError if deletion fails
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
			}
		}

		const descriptions: ConsumerGroupDescription[] = []

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
			}
		}

		const results: DeleteGroupsResult[] = []

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
			}
		}

		this.logger.debug('deleted groups', { count: results.length })
		return results
	}
}
