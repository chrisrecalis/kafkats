import { onTestFinished } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import { Consumer } from '@/consumer/consumer.js'
import type { ConsumerConfig } from '@/consumer/types.js'
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
export function fakeBroker(options: { partitions?: number } = {}) {
	const partitionCount = options.partitions ?? 2
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
	/** While set, SyncGroup waits for it. */
	const gates: { syncGroup?: Promise<void> } = {}
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
			generationId++
			const protocol = request.protocols[0]!
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				generationId,
				protocolType: 'consumer',
				protocolName: protocol.name,
				leader: memberId,
				skipAssignment: false,
				memberId,
				members: [{ memberId, groupInstanceId: null, metadata: protocol.metadata }],
			}
		},

		async syncGroup(request: SyncGroupRequest) {
			calls.push('SyncGroup')
			await gates.syncGroup
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
			const injected = commitErrors.shift()
			const stale = request.generationId !== -1 && request.generationId !== generationId
			const errorCode = injected ?? (stale ? ErrorCode.IllegalGeneration : ErrorCode.None)
			for (const topic of request.topics) {
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
					partitions: topic.partitions.map(({ partitionIndex }) => ({ partitionIndex, errorCode })),
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
		refreshMetadata: async () => ({
			topics: new Map([[TOPIC, { partitions: new Map(Array.from(logs.keys(), p => [p, { leader: 1 }])) }]]),
		}),
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
