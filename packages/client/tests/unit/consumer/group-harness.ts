import { onTestFinished } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import { Consumer } from '@/consumer/consumer.js'
import type { ConsumerConfig } from '@/consumer/types.js'
import { encodeAssignment, encodeSubscriptionMetadata } from '@/consumer/assignors/consumer-protocol.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { FetchRequest } from '@/protocol/messages/requests/fetch.js'
import type { JoinGroupRequest } from '@/protocol/messages/requests/join-group.js'
import type { ListOffsetsRequest } from '@/protocol/messages/requests/list-offsets.js'
import type { OffsetCommitRequest } from '@/protocol/messages/requests/offset-commit.js'
import type { OffsetFetchRequest } from '@/protocol/messages/requests/offset-fetch.js'
import type { SyncGroupRequest } from '@/protocol/messages/requests/sync-group.js'
import { fetchResponse, recordBatch, TOPIC, type PartitionResponse } from './fetch-harness.js'

export { TOPIC, tp } from './fetch-harness.js'

/** A promise resolved from outside. */
export function deferred<T = void>() {
	let resolve!: (value: T) => void
	const promise = new Promise<T>(r => {
		resolve = r
	})
	return { promise, resolve }
}

/** The log of one partition of {@link TOPIC}: the records at offsets `[start, end)` exist. */
interface Log {
	start: bigint
	end: bigint
}

/** A commit as the broker received it. */
interface ReceivedCommit {
	generationId: number
	partition: number
	offset: bigint
	metadata: string | null
}

/**
 * One broker that is the group coordinator and the leader of every partition of {@link TOPIC}. It runs the group
 * protocol for one member (which leads and assigns), keeps committed offsets, and serves fetches from in-memory
 * logs, so a real Consumer with its ConsumerGroup, OffsetManager and FetchManager runs against it. `calls`
 * records the group requests in order (and `Fetch <partitions>` for each fetch).
 */
export function fakeBroker(
	options: {
		partitions?: number
		topicVisible?: boolean
		/** Members of the group besides the one under test, with their subscriptions (they never fetch or commit). */
		otherMembers?: readonly { memberId: string; topics: readonly string[] }[]
		/** Topics other than {@link TOPIC} that metadata shows, with their partition counts. */
		topics?: Record<string, number>
		/**
		 * The first JoinGroup answers as to a static leader returning to a stable group (KIP-814, v9): skip the
		 * assignment, SyncGroup then answers with this assignment of {@link TOPIC}.
		 */
		returningLeader?: readonly number[]
	} = {}
) {
	const partitionCount = options.partitions ?? 2
	/** Whether metadata shows {@link TOPIC} yet (a topic created moments ago may not show). */
	let topicVisible = options.topicVisible ?? true
	/** The topic was deleted: commits and fetches of it answer UNKNOWN_TOPIC_OR_PARTITION. */
	let deleted = false
	const otherTopics = new Map(Object.entries(options.topics ?? {}))
	let returningLeader = options.returningLeader
	/** The assignments of every SyncGroup request, by member. */
	const syncs: Map<string, Buffer>[] = []
	let metadataRefreshes = 0
	let heartbeats = 0
	const calls: string[] = []
	const logs = new Map<number, Log>(Array.from({ length: partitionCount }, (_, p) => [p, { start: 0n, end: 0n }]))
	const committed = new Map<number, { offset: bigint; metadata: string | null }>()
	const commits: ReceivedCommit[] = []
	/** Answers of the next heartbeats, then None. */
	const heartbeatErrors: ErrorCode[] = []
	/** Answers of the next JoinGroup requests, then None. */
	const joinErrors: ErrorCode[] = []
	/** Answers of the next OffsetCommit requests (for every partition), then None. */
	const commitErrors: ErrorCode[] = []
	/** While set, SyncGroup (OffsetCommit) waits for it. */
	const gates: { syncGroup?: Promise<void>; offsetCommit?: Promise<void> } = {}
	let generationId = 0
	let members = 0

	const coordinator = {
		nodeId: 1,
		host: 'fake',
		port: 9092,

		async joinGroup(request: JoinGroupRequest) {
			calls.push('JoinGroup')
			const memberId = request.memberId || `member-${++members}`
			const errorCode = joinErrors.shift() ?? ErrorCode.None
			if (errorCode !== ErrorCode.None) {
				return {
					throttleTimeMs: 0,
					errorCode,
					generationId: -1,
					protocolType: null,
					protocolName: null,
					leader: '',
					skipAssignment: false,
					memberId: '',
					members: [],
				}
			}
			const skipAssignment = returningLeader !== undefined
			if (!skipAssignment) generationId++
			const protocol = request.protocols[0]!
			const others = (options.otherMembers ?? []).map(other => ({
				memberId: other.memberId,
				groupInstanceId: null,
				metadata: encodeSubscriptionMetadata({ version: 0, topics: [...other.topics], userData: null }),
			}))
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				generationId: Math.max(generationId, 1),
				protocolType: 'consumer',
				protocolName: protocol.name,
				leader: memberId,
				skipAssignment,
				memberId,
				members: [{ memberId, groupInstanceId: null, metadata: protocol.metadata }, ...others],
			}
		},

		async syncGroup(request: SyncGroupRequest) {
			calls.push('SyncGroup')
			await gates.syncGroup
			syncs.push(new Map(request.assignments.map(entry => [entry.memberId, entry.assignment])))
			if (returningLeader !== undefined) {
				const partitions = [...returningLeader]
				returningLeader = undefined
				generationId = Math.max(generationId, 1)
				return {
					throttleTimeMs: 0,
					errorCode: ErrorCode.None,
					protocolType: 'consumer',
					protocolName: request.protocolName,
					assignment: encodeAssignment({
						version: 0,
						partitions: [{ topic: TOPIC, partitions }],
						userData: null,
					}),
				}
			}
			const own = request.assignments.find(entry => entry.memberId === request.memberId)!.assignment
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				protocolType: 'consumer',
				protocolName: request.protocolName,
				assignment: own,
			}
		},

		async heartbeat() {
			heartbeats++
			return { throttleTimeMs: 0, errorCode: heartbeatErrors.shift() ?? ErrorCode.None }
		},

		async leaveGroup() {
			calls.push('LeaveGroup')
			return { throttleTimeMs: 0, errorCode: ErrorCode.None, members: [] }
		},

		async offsetFetch(request: OffsetFetchRequest) {
			calls.push('OffsetFetch')
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				topics: (request.topics ?? []).map(topic => ({
					name: topic.name,
					partitions: topic.partitions.map(({ partitionIndex }) => {
						const entry = committed.get(partitionIndex)
						return {
							partitionIndex,
							committedOffset: entry?.offset ?? -1n,
							committedLeaderEpoch: -1,
							metadata: entry?.metadata ?? '',
							errorCode: ErrorCode.None,
						}
					}),
				})),
			}
		},

		async offsetCommit(request: OffsetCommitRequest) {
			calls.push('OffsetCommit')
			await gates.offsetCommit
			const injected = commitErrors.shift()
			const stale = request.generationId !== -1 && request.generationId !== generationId
			const answer = injected ?? (stale ? ErrorCode.IllegalGeneration : ErrorCode.None)
			const codeOf = (topic: string) => (deleted && topic === TOPIC ? ErrorCode.UnknownTopicOrPartition : answer)
			for (const topic of request.topics) {
				const errorCode = codeOf(topic.name)
				for (const partition of topic.partitions) {
					commits.push({
						generationId: request.generationId,
						partition: partition.partitionIndex,
						offset: partition.committedOffset,
						metadata: partition.committedMetadata ?? null,
					})
					if (errorCode === ErrorCode.None) {
						committed.set(partition.partitionIndex, {
							offset: partition.committedOffset,
							metadata: partition.committedMetadata ?? null,
						})
					}
				}
			}
			return {
				throttleTimeMs: 0,
				topics: request.topics.map(topic => ({
					name: topic.name,
					partitions: topic.partitions.map(({ partitionIndex }) => ({
						partitionIndex,
						errorCode: codeOf(topic.name),
					})),
				})),
			}
		},

		async listOffsets(request: ListOffsetsRequest) {
			return {
				throttleTimeMs: 0,
				topics: request.topics.map(topic => ({
					name: topic.name,
					partitions: topic.partitions.map(({ partitionIndex, timestamp }) => {
						const log = logs.get(partitionIndex)!
						return {
							partitionIndex,
							errorCode: ErrorCode.None,
							timestamp: -1n,
							offset: timestamp === -2n ? log.start : log.end,
							leaderEpoch: -1,
						}
					}),
				})),
			}
		},

		async fetch(request: FetchRequest) {
			const partitions = request.topics.flatMap(topic => topic.partitions)
			calls.push(`Fetch ${partitions.map(p => p.partitionIndex).join(',')}`)
			const answer = (): PartitionResponse[] =>
				partitions.map(({ partitionIndex, fetchOffset }) => {
					const log = logs.get(partitionIndex)!
					if (deleted) {
						return {
							partition: partitionIndex,
							errorCode: ErrorCode.UnknownTopicOrPartition,
							highWatermark: -1n,
						}
					}
					if (fetchOffset < log.start || fetchOffset > log.end) {
						return {
							partition: partitionIndex,
							errorCode: ErrorCode.OffsetOutOfRange,
							highWatermark: log.end,
						}
					}
					const count = Number(log.end - fetchOffset)
					return {
						partition: partitionIndex,
						records: count > 0 ? recordBatch(fetchOffset, count) : undefined,
						highWatermark: log.end,
					}
				})
			let responses = answer()
			if (responses.every(response => !response.records && !response.errorCode)) {
				// Long poll, shortened: wait a little for records to arrive.
				await new Promise(resolve => setTimeout(resolve, 5))
				responses = answer()
			}
			return fetchResponse(...responses)
		},

		async closeFetchConnection() {},
	}

	const cluster = {
		clientId: 'fake-client',
		getLogger: () => null,
		getDedicatedCoordinator: async () => coordinator,
		releaseDedicatedBroker: async () => {},
		getCoordinator: async () => coordinator,
		invalidateCoordinator: () => {},
		getLeaderForPartition: async () => coordinator,
		/** The metadata of the last refresh, as the real cluster caches it. */
		metadata: null as unknown,
		refreshMetadata: async () => {
			metadataRefreshes++
			const shown = (count: number) => ({
				partitions: new Map(Array.from({ length: count }, (_, p) => [p, { leader: 1 }])),
			})
			cluster.metadata = {
				topics: new Map([
					...(topicVisible ? [[TOPIC, shown(logs.size)] as const] : []),
					...[...otherTopics].map(([topic, count]) => [topic, shown(count)] as const),
				]),
			}
			return cluster.metadata
		},
		getMetadata: () => cluster.metadata,
	}

	return {
		cluster: cluster as unknown as Cluster,
		calls,
		commits,
		committed,
		heartbeatErrors,
		joinErrors,
		commitErrors,
		gates,
		get generationId() {
			return generationId
		},
		get metadataRefreshes() {
			return metadataRefreshes
		},
		get heartbeats() {
			return heartbeats
		},
		/** The assignments of every SyncGroup request, by member. */
		syncs,
		/** Delete {@link TOPIC}: metadata stops showing it, and commits and fetches of it fail. */
		deleteTopic(): void {
			deleted = true
			topicVisible = false
		},
		/** Add partitions to one of the other topics (the next metadata refresh shows them). */
		growTopic(topic: string, count: number): void {
			otherTopics.set(topic, count)
		},
		/** Metadata shows the topic from now on. */
		showTopic(): void {
			topicVisible = true
		},
		/** Add a partition with an empty log to the topic (the next metadata refresh shows it). */
		addPartition(): void {
			logs.set(logs.size, { start: 0n, end: 0n })
		},
		/** Append `count` records to a partition's log. */
		produce(partition: number, count: number): void {
			logs.get(partition)!.end += BigInt(count)
		},
		/** Delete the records of a partition below `offset` (retention). */
		deleteBefore(partition: number, offset: bigint): void {
			logs.get(partition)!.start = offset
		},
	}
}

export type FakeBroker = ReturnType<typeof fakeBroker>

/**
 * A group consumer on a fake broker (eager 'range' protocol, fast heartbeats and fetches), stopped when the test
 * ends.
 */
export function groupConsumer(broker: FakeBroker, config: Partial<ConsumerConfig> = {}): Consumer {
	const consumer = new Consumer(broker.cluster, {
		groupId: 'g',
		partitionAssignmentStrategy: 'range',
		heartbeatIntervalMs: 10,
		maxWaitMs: 10,
		autoOffsetReset: 'earliest',
		...config,
	})
	onTestFinished(() => consumer.stop())
	return consumer
}

export function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/** Wait until `condition` holds (checked every 2 ms, for up to 2 s). */
export async function until(condition: () => boolean, message = 'condition'): Promise<void> {
	const deadline = Date.now() + 2000
	while (!condition()) {
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${message}`)
		await sleep(2)
	}
}
