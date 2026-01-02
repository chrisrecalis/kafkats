/**
 * Broker class - wraps Connection with typed protocol methods and API version negotiation
 */

import { Connection, type ConnectionOptions } from '@/network/connection.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { decodeResponseHeader } from '@/protocol/messages/headers.js'
import { encodeApiVersionsRequest, API_VERSIONS_VERSIONS } from '@/protocol/messages/requests/api-versions.js'
import { decodeApiVersionsResponse } from '@/protocol/messages/responses/api-versions.js'
import { noopLogger, type Logger } from '@/logger.js'
import {
	encodeMetadataRequest,
	METADATA_VERSIONS,
	type MetadataRequest,
} from '@/protocol/messages/requests/metadata.js'
import { decodeMetadataResponse, type MetadataResponse } from '@/protocol/messages/responses/metadata.js'
import { encodeProduceRequest, PRODUCE_VERSIONS, type ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { decodeProduceResponse, type ProduceResponse } from '@/protocol/messages/responses/produce.js'
import { encodeFetchRequest, FETCH_VERSIONS, type FetchRequest } from '@/protocol/messages/requests/fetch.js'
import { decodeFetchResponse, type FetchResponse } from '@/protocol/messages/responses/fetch.js'
import {
	encodeListOffsetsRequest,
	LIST_OFFSETS_VERSIONS,
	type ListOffsetsRequest,
} from '@/protocol/messages/requests/list-offsets.js'
import { decodeListOffsetsResponse, type ListOffsetsResponse } from '@/protocol/messages/responses/list-offsets.js'
import {
	encodeFindCoordinatorRequest,
	FIND_COORDINATOR_VERSIONS,
	type FindCoordinatorRequest,
} from '@/protocol/messages/requests/find-coordinator.js'
import {
	decodeFindCoordinatorResponse,
	type FindCoordinatorResponse,
} from '@/protocol/messages/responses/find-coordinator.js'
import {
	encodeJoinGroupRequest,
	JOIN_GROUP_VERSIONS,
	type JoinGroupRequest,
} from '@/protocol/messages/requests/join-group.js'
import { decodeJoinGroupResponse, type JoinGroupResponse } from '@/protocol/messages/responses/join-group.js'
import {
	encodeSyncGroupRequest,
	SYNC_GROUP_VERSIONS,
	type SyncGroupRequest,
} from '@/protocol/messages/requests/sync-group.js'
import { decodeSyncGroupResponse, type SyncGroupResponse } from '@/protocol/messages/responses/sync-group.js'
import {
	encodeHeartbeatRequest,
	HEARTBEAT_VERSIONS,
	type HeartbeatRequest,
} from '@/protocol/messages/requests/heartbeat.js'
import { decodeHeartbeatResponse, type HeartbeatResponse } from '@/protocol/messages/responses/heartbeat.js'
import {
	encodeLeaveGroupRequest,
	LEAVE_GROUP_VERSIONS,
	type LeaveGroupRequest,
} from '@/protocol/messages/requests/leave-group.js'
import { decodeLeaveGroupResponse, type LeaveGroupResponse } from '@/protocol/messages/responses/leave-group.js'
import {
	encodeOffsetCommitRequest,
	OFFSET_COMMIT_VERSIONS,
	type OffsetCommitRequest,
} from '@/protocol/messages/requests/offset-commit.js'
import { decodeOffsetCommitResponse, type OffsetCommitResponse } from '@/protocol/messages/responses/offset-commit.js'
import {
	encodeOffsetFetchRequest,
	OFFSET_FETCH_VERSIONS,
	type OffsetFetchRequest,
} from '@/protocol/messages/requests/offset-fetch.js'
import { decodeOffsetFetchResponse, type OffsetFetchResponse } from '@/protocol/messages/responses/offset-fetch.js'
import {
	encodeCreateTopicsRequest,
	CREATE_TOPICS_VERSIONS,
	type CreateTopicsRequest,
} from '@/protocol/messages/requests/create-topics.js'
import { decodeCreateTopicsResponse, type CreateTopicsResponse } from '@/protocol/messages/responses/create-topics.js'
import {
	encodeInitProducerIdRequest,
	INIT_PRODUCER_ID_VERSIONS,
	type InitProducerIdRequest,
} from '@/protocol/messages/requests/init-producer-id.js'
import {
	decodeInitProducerIdResponse,
	type InitProducerIdResponse,
} from '@/protocol/messages/responses/init-producer-id.js'
import {
	encodeAddPartitionsToTxnRequest,
	ADD_PARTITIONS_TO_TXN_VERSIONS,
	type AddPartitionsToTxnRequest,
} from '@/protocol/messages/requests/add-partitions-to-txn.js'
import {
	decodeAddPartitionsToTxnResponse,
	type AddPartitionsToTxnResponse,
} from '@/protocol/messages/responses/add-partitions-to-txn.js'
import {
	encodeAddOffsetsToTxnRequest,
	ADD_OFFSETS_TO_TXN_VERSIONS,
	type AddOffsetsToTxnRequest,
} from '@/protocol/messages/requests/add-offsets-to-txn.js'
import {
	decodeAddOffsetsToTxnResponse,
	type AddOffsetsToTxnResponse,
} from '@/protocol/messages/responses/add-offsets-to-txn.js'
import { encodeEndTxnRequest, END_TXN_VERSIONS, type EndTxnRequest } from '@/protocol/messages/requests/end-txn.js'
import { decodeEndTxnResponse, type EndTxnResponse } from '@/protocol/messages/responses/end-txn.js'
import {
	encodeTxnOffsetCommitRequest,
	TXN_OFFSET_COMMIT_VERSIONS,
	type TxnOffsetCommitRequest,
} from '@/protocol/messages/requests/txn-offset-commit.js'
import {
	decodeTxnOffsetCommitResponse,
	type TxnOffsetCommitResponse,
} from '@/protocol/messages/responses/txn-offset-commit.js'
import {
	encodeDeleteTopicsRequest,
	DELETE_TOPICS_VERSIONS,
	type DeleteTopicsRequest,
} from '@/protocol/messages/requests/delete-topics.js'
import { decodeDeleteTopicsResponse, type DeleteTopicsResponse } from '@/protocol/messages/responses/delete-topics.js'
import {
	encodeListGroupsRequest,
	LIST_GROUPS_VERSIONS,
	type ListGroupsRequest,
} from '@/protocol/messages/requests/list-groups.js'
import { decodeListGroupsResponse, type ListGroupsResponse } from '@/protocol/messages/responses/list-groups.js'
import {
	encodeDescribeGroupsRequest,
	DESCRIBE_GROUPS_VERSIONS,
	type DescribeGroupsRequest,
} from '@/protocol/messages/requests/describe-groups.js'
import {
	decodeDescribeGroupsResponse,
	type DescribeGroupsResponse,
} from '@/protocol/messages/responses/describe-groups.js'
import {
	encodeDeleteGroupsRequest,
	DELETE_GROUPS_VERSIONS,
	type DeleteGroupsRequest,
} from '@/protocol/messages/requests/delete-groups.js'
import { decodeDeleteGroupsResponse, type DeleteGroupsResponse } from '@/protocol/messages/responses/delete-groups.js'
import {
	encodeShareGroupHeartbeatRequest,
	SHARE_GROUP_HEARTBEAT_VERSIONS,
	type ShareGroupHeartbeatRequest,
} from '@/protocol/messages/requests/share-group-heartbeat.js'
import {
	decodeShareGroupHeartbeatResponse,
	type ShareGroupHeartbeatResponse,
} from '@/protocol/messages/responses/share-group-heartbeat.js'
import {
	encodeShareFetchRequest,
	SHARE_FETCH_VERSIONS,
	type ShareFetchRequest,
} from '@/protocol/messages/requests/share-fetch.js'
import { decodeShareFetchResponse, type ShareFetchResponse } from '@/protocol/messages/responses/share-fetch.js'
import {
	encodeShareAcknowledgeRequest,
	SHARE_ACKNOWLEDGE_VERSIONS,
	type ShareAcknowledgeRequest,
} from '@/protocol/messages/requests/share-acknowledge.js'
import {
	decodeShareAcknowledgeResponse,
	type ShareAcknowledgeResponse,
} from '@/protocol/messages/responses/share-acknowledge.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { UnsupportedVersionError, KafkaProtocolError } from './errors.js'
import type { BrokerConfig, VersionRange } from './types.js'

/**
 * Client-supported API version ranges
 * These are the versions our client implementation can handle
 */
const CLIENT_API_VERSIONS: Partial<Record<ApiKey, { min: number; max: number }>> = {
	[ApiKey.ApiVersions]: API_VERSIONS_VERSIONS,
	[ApiKey.Metadata]: METADATA_VERSIONS,
	[ApiKey.Produce]: PRODUCE_VERSIONS,
	[ApiKey.Fetch]: FETCH_VERSIONS,
	[ApiKey.ListOffsets]: LIST_OFFSETS_VERSIONS,
	[ApiKey.FindCoordinator]: FIND_COORDINATOR_VERSIONS,
	[ApiKey.JoinGroup]: JOIN_GROUP_VERSIONS,
	[ApiKey.SyncGroup]: SYNC_GROUP_VERSIONS,
	[ApiKey.Heartbeat]: HEARTBEAT_VERSIONS,
	[ApiKey.LeaveGroup]: LEAVE_GROUP_VERSIONS,
	[ApiKey.OffsetCommit]: OFFSET_COMMIT_VERSIONS,
	[ApiKey.OffsetFetch]: OFFSET_FETCH_VERSIONS,
	[ApiKey.CreateTopics]: CREATE_TOPICS_VERSIONS,
	[ApiKey.InitProducerId]: INIT_PRODUCER_ID_VERSIONS,
	[ApiKey.AddPartitionsToTxn]: ADD_PARTITIONS_TO_TXN_VERSIONS,
	[ApiKey.AddOffsetsToTxn]: ADD_OFFSETS_TO_TXN_VERSIONS,
	[ApiKey.EndTxn]: END_TXN_VERSIONS,
	[ApiKey.TxnOffsetCommit]: TXN_OFFSET_COMMIT_VERSIONS,
	[ApiKey.DeleteTopics]: DELETE_TOPICS_VERSIONS,
	[ApiKey.ListGroups]: LIST_GROUPS_VERSIONS,
	[ApiKey.DescribeGroups]: DESCRIBE_GROUPS_VERSIONS,
	[ApiKey.DeleteGroups]: DELETE_GROUPS_VERSIONS,
	[ApiKey.ShareGroupHeartbeat]: SHARE_GROUP_HEARTBEAT_VERSIONS,
	[ApiKey.ShareFetch]: SHARE_FETCH_VERSIONS,
	[ApiKey.ShareAcknowledge]: SHARE_ACKNOWLEDGE_VERSIONS,
}

/**
 * Broker wraps a Connection with typed protocol methods and API version negotiation
 */
export class Broker {
	readonly host: string
	readonly port: number
	readonly nodeId: number

	private readonly connection: Connection
	private readonly fetchConnection: Connection
	private apiVersions: Map<ApiKey, VersionRange> = new Map()
	private versionsFetchedAt: number = 0
	private readonly logger: Logger

	constructor(config: BrokerConfig) {
		this.host = config.host
		this.port = config.port
		this.nodeId = config.nodeId
		this.logger = config.logger?.child({ component: 'broker', nodeId: config.nodeId }) ?? noopLogger

		const connectionOptions: ConnectionOptions = {
			host: config.host,
			port: config.port,
			clientId: config.clientId,
			connectionTimeoutMs: config.connectionTimeoutMs,
			requestTimeoutMs: config.requestTimeoutMs,
			maxInFlightRequests: config.maxInFlightRequests,
			tls: config.tls,
			sasl: config.sasl,
			logger: config.logger?.child({ connection: 'control' }) ?? config.logger,
		}

		this.connection = new Connection(connectionOptions)

		this.fetchConnection = new Connection({
			...connectionOptions,
			logger: config.logger?.child({ connection: 'fetch' }) ?? config.logger,
		})
	}

	get isConnected(): boolean {
		return this.connection.isConnected && this.fetchConnection.isConnected
	}

	async connect(): Promise<void> {
		if (this.isConnected) {
			return
		}

		this.logger.debug('connecting', { host: this.host, port: this.port })
		await Promise.all([this.connection.connect(), this.fetchConnection.connect()])
		await this.negotiateApiVersions()
		this.logger.debug('connected', { host: this.host, port: this.port })
	}

	async disconnect(): Promise<void> {
		this.logger.debug('disconnecting', { host: this.host, port: this.port })
		await Promise.all([this.connection.close(), this.fetchConnection.close()])
		this.apiVersions.clear()
		this.versionsFetchedAt = 0
		this.logger.debug('disconnected', { host: this.host, port: this.port })
	}

	/**
	 * Negotiate API versions with broker
	 *
	 * We use v0 initially since it's universally supported.
	 * After getting the response, we know what versions the broker supports.
	 */
	private async negotiateApiVersions(): Promise<void> {
		const startTime = Date.now()
		this.logger.debug('negotiating API versions')

		// Use v0 for initial ApiVersions (universally supported, non-flexible)
		const version = 0

		const responseBuffer = await this.connection.send(ApiKey.ApiVersions, version, encoder => {
			encodeApiVersionsRequest(encoder, version, {})
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ApiVersions, version)
		const response = decodeApiVersionsResponse(decoder, version)

		if (response.errorCode !== ErrorCode.None) {
			this.logger.error('API versions negotiation failed', { errorCode: response.errorCode })
			throw new KafkaProtocolError(response.errorCode, 'ApiVersions request failed')
		}

		this.apiVersions.clear()
		for (const apiVersion of response.apiVersions) {
			this.apiVersions.set(apiVersion.apiKey, {
				min: apiVersion.minVersion,
				max: apiVersion.maxVersion,
			})
		}
		this.versionsFetchedAt = Date.now()

		const durationMs = Date.now() - startTime
		this.logger.debug('API versions negotiated', { durationMs, apiCount: this.apiVersions.size })
	}

	getApiVersion(apiKey: ApiKey, preferredMin?: number, preferredMax?: number): number {
		const brokerRange = this.apiVersions.get(apiKey)
		if (!brokerRange) {
			throw new UnsupportedVersionError(apiKey, preferredMax ?? 0, undefined)
		}

		const clientRange = CLIENT_API_VERSIONS[apiKey]
		if (!clientRange) {
			throw new UnsupportedVersionError(apiKey, preferredMax ?? 0, brokerRange)
		}

		// Negotiate: find intersection of broker, client, and preferred ranges
		const minVersion = Math.max(brokerRange.min, clientRange.min, preferredMin ?? 0)
		const maxVersion = Math.min(brokerRange.max, clientRange.max, preferredMax ?? Infinity)

		if (minVersion > maxVersion) {
			throw new UnsupportedVersionError(apiKey, preferredMax ?? clientRange.max, brokerRange)
		}

		return maxVersion
	}

	getApiVersions(): Map<ApiKey, VersionRange> {
		return new Map(this.apiVersions)
	}

	async metadata(request: MetadataRequest): Promise<MetadataResponse> {
		const version = this.getApiVersion(ApiKey.Metadata)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'Metadata', version })

		const responseBuffer = await this.connection.send(ApiKey.Metadata, version, encoder => {
			encodeMetadataRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.Metadata, version)
		const response = decodeMetadataResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'Metadata', version, durationMs })
		return response
	}

	async produce(request: ProduceRequest): Promise<ProduceResponse> {
		const version = this.getApiVersion(ApiKey.Produce)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'Produce', version, acks: request.acks })

		const responseBuffer = await this.connection.send(ApiKey.Produce, version, encoder => {
			encodeProduceRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.Produce, version)
		const response = decodeProduceResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'Produce', version, durationMs })
		return response
	}

	async fetch(request: FetchRequest): Promise<FetchResponse> {
		const version = this.getApiVersion(ApiKey.Fetch)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'Fetch', version, maxWaitMs: request.maxWaitMs })

		const responseBuffer = await this.fetchConnection.send(ApiKey.Fetch, version, encoder => {
			encodeFetchRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.Fetch, version)
		const response = decodeFetchResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'Fetch', version, durationMs })
		return response
	}

	async listOffsets(request: ListOffsetsRequest): Promise<ListOffsetsResponse> {
		const version = this.getApiVersion(ApiKey.ListOffsets)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'ListOffsets', version })

		const responseBuffer = await this.connection.send(ApiKey.ListOffsets, version, encoder => {
			encodeListOffsetsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ListOffsets, version)
		const response = decodeListOffsetsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'ListOffsets', version, durationMs })
		return response
	}

	async findCoordinator(request: FindCoordinatorRequest): Promise<FindCoordinatorResponse> {
		const version = this.getApiVersion(ApiKey.FindCoordinator)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'FindCoordinator', version, key: request.key })

		const responseBuffer = await this.connection.send(ApiKey.FindCoordinator, version, encoder => {
			encodeFindCoordinatorRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.FindCoordinator, version)
		const response = decodeFindCoordinatorResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'FindCoordinator', version, durationMs })
		return response
	}

	async joinGroup(request: JoinGroupRequest): Promise<JoinGroupResponse> {
		const version = this.getApiVersion(ApiKey.JoinGroup)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'JoinGroup',
			version,
			groupId: request.groupId,
			memberId: request.memberId,
		})

		const responseBuffer = await this.connection.send(ApiKey.JoinGroup, version, encoder => {
			encodeJoinGroupRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.JoinGroup, version)
		const response = decodeJoinGroupResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'JoinGroup', version, durationMs, errorCode: response.errorCode })
		return response
	}

	async syncGroup(request: SyncGroupRequest): Promise<SyncGroupResponse> {
		const version = this.getApiVersion(ApiKey.SyncGroup)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'SyncGroup',
			version,
			groupId: request.groupId,
			generationId: request.generationId,
		})

		const responseBuffer = await this.connection.send(ApiKey.SyncGroup, version, encoder => {
			encodeSyncGroupRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.SyncGroup, version)
		const response = decodeSyncGroupResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'SyncGroup', version, durationMs, errorCode: response.errorCode })
		return response
	}

	async heartbeat(request: HeartbeatRequest): Promise<HeartbeatResponse> {
		const version = this.getApiVersion(ApiKey.Heartbeat)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'Heartbeat',
			version,
			groupId: request.groupId,
			generationId: request.generationId,
		})

		const responseBuffer = await this.connection.send(ApiKey.Heartbeat, version, encoder => {
			encodeHeartbeatRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.Heartbeat, version)
		const response = decodeHeartbeatResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'Heartbeat', version, durationMs, errorCode: response.errorCode })
		return response
	}

	async leaveGroup(request: LeaveGroupRequest): Promise<LeaveGroupResponse> {
		const version = this.getApiVersion(ApiKey.LeaveGroup)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'LeaveGroup', version, groupId: request.groupId })

		const responseBuffer = await this.connection.send(ApiKey.LeaveGroup, version, encoder => {
			encodeLeaveGroupRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.LeaveGroup, version)
		const response = decodeLeaveGroupResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', {
			api: 'LeaveGroup',
			version,
			durationMs,
			errorCode: response.errorCode,
		})
		return response
	}

	async offsetCommit(request: OffsetCommitRequest): Promise<OffsetCommitResponse> {
		const version = this.getApiVersion(ApiKey.OffsetCommit)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'OffsetCommit', version, groupId: request.groupId })

		const responseBuffer = await this.connection.send(ApiKey.OffsetCommit, version, encoder => {
			encodeOffsetCommitRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.OffsetCommit, version)
		const response = decodeOffsetCommitResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'OffsetCommit', version, durationMs })
		return response
	}

	async offsetFetch(request: OffsetFetchRequest): Promise<OffsetFetchResponse> {
		const version = this.getApiVersion(ApiKey.OffsetFetch)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'OffsetFetch', version, groupId: request.groupId })

		const responseBuffer = await this.connection.send(ApiKey.OffsetFetch, version, encoder => {
			encodeOffsetFetchRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.OffsetFetch, version)
		const response = decodeOffsetFetchResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'OffsetFetch', version, durationMs })
		return response
	}

	async createTopics(request: CreateTopicsRequest): Promise<CreateTopicsResponse> {
		const version = this.getApiVersion(ApiKey.CreateTopics)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'CreateTopics',
			version,
			topicCount: request.topics.length,
		})

		const responseBuffer = await this.connection.send(ApiKey.CreateTopics, version, encoder => {
			encodeCreateTopicsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.CreateTopics, version)
		const response = decodeCreateTopicsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'CreateTopics', version, durationMs })
		return response
	}

	async initProducerId(request: InitProducerIdRequest): Promise<InitProducerIdResponse> {
		const version = this.getApiVersion(ApiKey.InitProducerId)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'InitProducerId',
			version,
			transactionalId: request.transactionalId,
		})

		const responseBuffer = await this.connection.send(ApiKey.InitProducerId, version, encoder => {
			encodeInitProducerIdRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.InitProducerId, version)
		const response = decodeInitProducerIdResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', {
			api: 'InitProducerId',
			version,
			durationMs,
			errorCode: response.errorCode,
			producerId: response.producerId.toString(),
			producerEpoch: response.producerEpoch,
		})
		return response
	}

	async addPartitionsToTxn(request: AddPartitionsToTxnRequest): Promise<AddPartitionsToTxnResponse> {
		const version = this.getApiVersion(ApiKey.AddPartitionsToTxn)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'AddPartitionsToTxn',
			version,
			transactionalId: request.transactionalId,
		})

		const responseBuffer = await this.connection.send(ApiKey.AddPartitionsToTxn, version, encoder => {
			encodeAddPartitionsToTxnRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.AddPartitionsToTxn, version)
		const response = decodeAddPartitionsToTxnResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'AddPartitionsToTxn', version, durationMs })
		return response
	}

	async addOffsetsToTxn(request: AddOffsetsToTxnRequest): Promise<AddOffsetsToTxnResponse> {
		const version = this.getApiVersion(ApiKey.AddOffsetsToTxn)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'AddOffsetsToTxn',
			version,
			transactionalId: request.transactionalId,
			groupId: request.groupId,
		})

		const responseBuffer = await this.connection.send(ApiKey.AddOffsetsToTxn, version, encoder => {
			encodeAddOffsetsToTxnRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.AddOffsetsToTxn, version)
		const response = decodeAddOffsetsToTxnResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', {
			api: 'AddOffsetsToTxn',
			version,
			durationMs,
			errorCode: response.errorCode,
		})
		return response
	}

	async endTxn(request: EndTxnRequest): Promise<EndTxnResponse> {
		const version = this.getApiVersion(ApiKey.EndTxn)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'EndTxn',
			version,
			transactionalId: request.transactionalId,
			committed: request.committed,
		})

		const responseBuffer = await this.connection.send(ApiKey.EndTxn, version, encoder => {
			encodeEndTxnRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.EndTxn, version)
		const response = decodeEndTxnResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', {
			api: 'EndTxn',
			version,
			durationMs,
			errorCode: response.errorCode,
		})
		return response
	}

	async txnOffsetCommit(request: TxnOffsetCommitRequest): Promise<TxnOffsetCommitResponse> {
		const version = this.getApiVersion(ApiKey.TxnOffsetCommit)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'TxnOffsetCommit',
			version,
			transactionalId: request.transactionalId,
			groupId: request.groupId,
		})

		const responseBuffer = await this.connection.send(ApiKey.TxnOffsetCommit, version, encoder => {
			encodeTxnOffsetCommitRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.TxnOffsetCommit, version)
		const response = decodeTxnOffsetCommitResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'TxnOffsetCommit', version, durationMs })
		return response
	}

	async deleteTopics(request: DeleteTopicsRequest): Promise<DeleteTopicsResponse> {
		const version = this.getApiVersion(ApiKey.DeleteTopics)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'DeleteTopics',
			version,
			topicCount: request.topics.length,
		})

		const responseBuffer = await this.connection.send(ApiKey.DeleteTopics, version, encoder => {
			encodeDeleteTopicsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.DeleteTopics, version)
		const response = decodeDeleteTopicsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'DeleteTopics', version, durationMs })
		return response
	}

	async listGroups(request: ListGroupsRequest): Promise<ListGroupsResponse> {
		const version = this.getApiVersion(ApiKey.ListGroups)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'ListGroups', version })

		const responseBuffer = await this.connection.send(ApiKey.ListGroups, version, encoder => {
			encodeListGroupsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ListGroups, version)
		const response = decodeListGroupsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', {
			api: 'ListGroups',
			version,
			durationMs,
			groupCount: response.groups.length,
		})
		return response
	}

	async describeGroups(request: DescribeGroupsRequest): Promise<DescribeGroupsResponse> {
		const version = this.getApiVersion(ApiKey.DescribeGroups)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'DescribeGroups',
			version,
			groupCount: request.groups.length,
		})

		const responseBuffer = await this.connection.send(ApiKey.DescribeGroups, version, encoder => {
			encodeDescribeGroupsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.DescribeGroups, version)
		const response = decodeDescribeGroupsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'DescribeGroups', version, durationMs })
		return response
	}

	async deleteGroups(request: DeleteGroupsRequest): Promise<DeleteGroupsResponse> {
		const version = this.getApiVersion(ApiKey.DeleteGroups)
		const startTime = Date.now()
		this.logger.debug('sending request', {
			api: 'DeleteGroups',
			version,
			groupCount: request.groupsNames.length,
		})

		const responseBuffer = await this.connection.send(ApiKey.DeleteGroups, version, encoder => {
			encodeDeleteGroupsRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.DeleteGroups, version)
		const response = decodeDeleteGroupsResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'DeleteGroups', version, durationMs })
		return response
	}

	async shareGroupHeartbeat(request: ShareGroupHeartbeatRequest): Promise<ShareGroupHeartbeatResponse> {
		const version = this.getApiVersion(ApiKey.ShareGroupHeartbeat)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'ShareGroupHeartbeat', version, groupId: request.groupId })

		const responseBuffer = await this.connection.send(ApiKey.ShareGroupHeartbeat, version, encoder => {
			encodeShareGroupHeartbeatRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ShareGroupHeartbeat, version)
		let response: ShareGroupHeartbeatResponse
		try {
			response = decodeShareGroupHeartbeatResponse(decoder, version)
		} catch (error) {
			if (process.env.KAFKA_TS_DEBUG_SHARE_GROUP_HEARTBEAT === '1') {
				const err = error instanceof Error ? error : new Error(String(error))
				const maxHexBytes = 512
				const hex = responseBuffer.subarray(0, maxHexBytes).toString('hex')
				throw new Error(
					`failed to decode ShareGroupHeartbeat response (v${version}, bytes=${responseBuffer.length}, offset=${decoder.offset()}, remaining=${decoder.remaining()}): ${err.message}; hex[0..${maxHexBytes}]=${hex}`,
					{ cause: err }
				)
			}
			throw error
		}

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'ShareGroupHeartbeat', version, durationMs })
		return response
	}

	async shareFetch(request: ShareFetchRequest): Promise<ShareFetchResponse> {
		const version = this.getApiVersion(ApiKey.ShareFetch)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'ShareFetch', version })

		const responseBuffer = await this.fetchConnection.send(ApiKey.ShareFetch, version, encoder => {
			encodeShareFetchRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ShareFetch, version)
		const response = decodeShareFetchResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'ShareFetch', version, durationMs })
		return response
	}

	async shareAcknowledge(request: ShareAcknowledgeRequest): Promise<ShareAcknowledgeResponse> {
		const version = this.getApiVersion(ApiKey.ShareAcknowledge)
		const startTime = Date.now()
		this.logger.debug('sending request', { api: 'ShareAcknowledge', version })

		const responseBuffer = await this.connection.send(ApiKey.ShareAcknowledge, version, encoder => {
			encodeShareAcknowledgeRequest(encoder, version, request)
		})

		const decoder = new Decoder(responseBuffer)
		decodeResponseHeader(decoder, ApiKey.ShareAcknowledge, version)
		const response = decodeShareAcknowledgeResponse(decoder, version)

		const durationMs = Date.now() - startTime
		this.logger.debug('received response', { api: 'ShareAcknowledge', version, durationMs })
		return response
	}

	toString(): string {
		return `Broker(${this.nodeId}@${this.host}:${this.port}, connected=${this.isConnected})`
	}
}
