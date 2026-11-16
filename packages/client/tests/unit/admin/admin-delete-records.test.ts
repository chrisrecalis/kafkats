import { describe, expect, it } from 'vitest'

import { Admin } from '@/admin/admin.js'
import type { Cluster } from '@/client/cluster.js'
import type { ClusterMetadata, TopicMetadata } from '@/client/types.js'
import { KafkaError, KafkaProtocolError, TimeoutError, UnknownTopicOrPartitionError } from '@/client/errors.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { DeleteRecordsRequest } from '@/protocol/messages/requests/delete-records.js'
import type { DeleteRecordsResponse } from '@/protocol/messages/responses/delete-records.js'

/** How a fake broker answers a DeleteRecords request (it may throw, or never settle). */
type Answer = (nodeId: number, request: DeleteRecordsRequest) => Promise<DeleteRecordsResponse>

/**
 * The parts of a Cluster that deleteRecords uses. `leaders` is what the brokers report ("topic:partition" -> leader,
 * -1 for none); the metadata cache holds the leaders of the last refresh.
 */
class FakeCluster {
	readonly leaders = new Map<string, number>()
	readonly refreshes: string[][] = []
	readonly requests: { nodeId: number; request: DeleteRecordsRequest }[] = []
	private metadata: ClusterMetadata | null = null

	constructor(private readonly answer: Answer = succeed) {}

	getLogger() {
		return undefined
	}

	getMetadata() {
		return this.metadata
	}

	async refreshMetadata(topics: string[]) {
		this.refreshes.push(topics)
		const byTopic = new Map<string, TopicMetadata>()
		for (const [key, leaderId] of this.leaders) {
			const [name, partition] = key.split(':') as [string, string]
			const topic = byTopic.get(name) ?? { name, topicId: '', isInternal: false, partitions: new Map() }
			topic.partitions.set(Number(partition), {
				partitionIndex: Number(partition),
				leaderId,
				leaderEpoch: 0,
				replicaNodes: [],
				isrNodes: [],
				offlineReplicas: [],
			})
			byTopic.set(name, topic)
		}
		this.metadata = { clusterId: null, controllerId: 1, brokers: new Map(), topics: byTopic, updatedAt: 0 }
		return this.metadata
	}

	async getBroker(nodeId: number) {
		return {
			deleteRecords: (request: DeleteRecordsRequest) => {
				this.requests.push({ nodeId, request })
				return this.answer(nodeId, request)
			},
		}
	}

	admin(): Admin {
		return new Admin(this as unknown as Cluster)
	}
}

/** Every partition deleted, its low watermark being the requested offset. */
async function succeed(_nodeId: number, request: DeleteRecordsRequest): Promise<DeleteRecordsResponse> {
	return answerWith(request, () => ErrorCode.None)
}

/** A response with the error code `codeOf` gives each partition (low watermark: the offset on success). */
function answerWith(
	request: DeleteRecordsRequest,
	codeOf: (topic: string, partition: number) => ErrorCode
): DeleteRecordsResponse {
	return {
		throttleTimeMs: 0,
		topics: request.topics.map(topic => ({
			name: topic.name,
			partitions: topic.partitions.map(({ partitionIndex, offset }) => {
				const errorCode = codeOf(topic.name, partitionIndex)
				return { partitionIndex, errorCode, lowWatermark: errorCode === ErrorCode.None ? offset : -1n }
			}),
		})),
	}
}

const never = () => new Promise<DeleteRecordsResponse>(() => {})

describe('Admin.deleteRecords', () => {
	it('sends one request per leader, with the time budget as the broker timeout, and reports in input order', async () => {
		const cluster = new FakeCluster()
		cluster.leaders.set('t:0', 1).set('t:1', 2).set('u:0', 1)

		const results = await cluster.admin().deleteRecords(
			[
				{ topic: 't', partition: 1, offset: 10n },
				{ topic: 't', partition: 0, offset: 20n },
				{ topic: 'u', partition: 0, offset: 30n },
			],
			{ timeoutMs: 5000 }
		)

		expect(results).toEqual([
			{ topic: 't', partition: 1, lowWatermark: 10n },
			{ topic: 't', partition: 0, lowWatermark: 20n },
			{ topic: 'u', partition: 0, lowWatermark: 30n },
		])
		expect(cluster.refreshes).toEqual([['t', 'u']])
		const byNode = new Map(cluster.requests.map(({ nodeId, request }) => [nodeId, request]))
		expect(cluster.requests).toHaveLength(2)
		expect(byNode.get(1)?.topics).toEqual([
			{ name: 't', partitions: [{ partitionIndex: 0, offset: 20n }] },
			{ name: 'u', partitions: [{ partitionIndex: 0, offset: 30n }] },
		])
		expect(byNode.get(2)?.topics).toEqual([{ name: 't', partitions: [{ partitionIndex: 1, offset: 10n }] }])
		for (const { request } of cluster.requests) {
			expect(request.timeoutMs).toBeGreaterThan(4900)
			expect(request.timeoutMs).toBeLessThanOrEqual(5000)
		}
	})

	it('uses the cached leaders when it knows them', async () => {
		const cluster = new FakeCluster()
		cluster.leaders.set('t:0', 1)
		const admin = cluster.admin()

		await admin.deleteRecords([{ topic: 't', partition: 0, offset: 1n }])
		await admin.deleteRecords([{ topic: 't', partition: 0, offset: 2n }])

		expect(cluster.refreshes).toEqual([['t']])
		expect(cluster.requests).toHaveLength(2)
	})

	it('deletes a partition listed twice up to its last offset and reports it once', async () => {
		const cluster = new FakeCluster()
		cluster.leaders.set('t:0', 1)

		const results = await cluster.admin().deleteRecords([
			{ topic: 't', partition: 0, offset: 5n },
			{ topic: 't', partition: 0, offset: 8n },
		])

		expect(results).toEqual([{ topic: 't', partition: 0, lowWatermark: 8n }])
		expect(cluster.requests[0]!.request.topics).toEqual([
			{ name: 't', partitions: [{ partitionIndex: 0, offset: 8n }] },
		])
	})

	it('returns an empty result for no records without a request', async () => {
		const cluster = new FakeCluster()

		expect(await cluster.admin().deleteRecords([])).toEqual([])
		expect(cluster.requests).toHaveLength(0)
		expect(cluster.refreshes).toHaveLength(0)
	})

	it('returns a broker error per partition, without throwing or retrying it', async () => {
		const cluster = new FakeCluster(async (_nodeId, request) =>
			answerWith(request, (_topic, partition) => (partition === 0 ? ErrorCode.OffsetOutOfRange : ErrorCode.None))
		)
		cluster.leaders.set('t:0', 1).set('t:1', 1)

		const [outOfRange, deleted] = await cluster.admin().deleteRecords([
			{ topic: 't', partition: 0, offset: 500n },
			{ topic: 't', partition: 1, offset: 5n },
		])

		expect(outOfRange!.lowWatermark).toBeUndefined()
		expect(outOfRange!.error).toBeInstanceOf(KafkaProtocolError)
		expect(outOfRange!.error!.errorCode).toBe(ErrorCode.OffsetOutOfRange)
		expect(deleted).toEqual({ topic: 't', partition: 1, lowWatermark: 5n })
		expect(cluster.requests).toHaveLength(1)
	})

	it('looks the leader up again after NotLeaderOrFollower and retries on the new leader', async () => {
		const cluster = new FakeCluster(async (nodeId, request) => {
			if (nodeId === 2) return succeed(nodeId, request)
			cluster.leaders.set('t:0', 2) // the leadership moved; the cache still names node 1
			return answerWith(request, () => ErrorCode.NotLeaderOrFollower)
		})
		cluster.leaders.set('t:0', 1)

		const results = await cluster.admin().deleteRecords([{ topic: 't', partition: 0, offset: 3n }])

		expect(results).toEqual([{ topic: 't', partition: 0, lowWatermark: 3n }])
		expect(cluster.requests.map(r => r.nodeId)).toEqual([1, 2])
		expect(cluster.refreshes).toEqual([['t'], ['t']])
	})

	it('retries the partitions of a leader whose connection failed, after looking the leader up again', async () => {
		let failures = 1
		const cluster = new FakeCluster(async (nodeId, request) => {
			if (failures-- > 0) throw new ConnectionClosedError()
			return succeed(nodeId, request)
		})
		cluster.leaders.set('t:0', 1).set('t:1', 1)

		const results = await cluster.admin().deleteRecords([
			{ topic: 't', partition: 0, offset: 1n },
			{ topic: 't', partition: 1, offset: 2n },
		])

		expect(results.map(r => r.lowWatermark)).toEqual([1n, 2n])
		expect(cluster.requests).toHaveLength(2)
		expect(cluster.refreshes).toEqual([['t'], ['t']])
	})

	it('retries a retriable broker error without looking the leader up again', async () => {
		let failures = 1
		const cluster = new FakeCluster(async (_nodeId, request) =>
			answerWith(request, () => (failures-- > 0 ? ErrorCode.RequestTimedOut : ErrorCode.None))
		)
		cluster.leaders.set('t:0', 1)

		const results = await cluster.admin().deleteRecords([{ topic: 't', partition: 0, offset: 4n }])

		expect(results).toEqual([{ topic: 't', partition: 0, lowWatermark: 4n }])
		expect(cluster.requests).toHaveLength(2)
		expect(cluster.refreshes).toEqual([['t']])
	})

	it('reports a TimeoutError, caused by the last failure, for a partition still failing at the deadline', async () => {
		const cluster = new FakeCluster()
		cluster.leaders.set('t:0', 1)
		const started = Date.now()

		const [missing, deleted] = await cluster.admin().deleteRecords(
			[
				{ topic: 'gone', partition: 0, offset: 1n },
				{ topic: 't', partition: 0, offset: 1n },
			],
			{ timeoutMs: 300 }
		)

		expect(Date.now() - started).toBeLessThan(1000)
		expect(missing!.error).toBeInstanceOf(TimeoutError)
		expect(missing!.error!.cause).toBeInstanceOf(UnknownTopicOrPartitionError)
		expect(deleted).toEqual({ topic: 't', partition: 0, lowWatermark: 1n })
		// The unknown topic is looked up again before every attempt (at 0 and 100 ms; the next would be at 300 ms).
		expect(cluster.refreshes.length).toBeGreaterThanOrEqual(2)
		expect(cluster.refreshes.every(topics => topics.includes('gone'))).toBe(true)
	})

	it('stops waiting for a request still in flight at the deadline', async () => {
		const cluster = new FakeCluster(never)
		cluster.leaders.set('t:0', 1)
		const started = Date.now()

		const [result] = await cluster.admin().deleteRecords([{ topic: 't', partition: 0, offset: 1n }], {
			timeoutMs: 100,
		})

		expect(Date.now() - started).toBeLessThan(1000)
		expect(result!.error).toBeInstanceOf(TimeoutError)
		expect(result!.error!.cause).toBeUndefined()
	})

	it('rejects with the reason of the aborted signal', async () => {
		const cluster = new FakeCluster(never)
		cluster.leaders.set('t:0', 1)
		const controller = new AbortController()
		const reason = new Error('shutting down')

		const deletion = cluster.admin().deleteRecords([{ topic: 't', partition: 0, offset: 1n }], {
			signal: controller.signal,
		})
		setTimeout(() => controller.abort(reason), 20)

		await expect(deletion).rejects.toBe(reason)
	})

	it('rejects at once, sending nothing, when the signal is already aborted', async () => {
		const cluster = new FakeCluster()
		cluster.leaders.set('t:0', 1)
		const reason = new Error('already stopped')

		await expect(
			cluster
				.admin()
				.deleteRecords([{ topic: 't', partition: 0, offset: 1n }], { signal: AbortSignal.abort(reason) })
		).rejects.toBe(reason)
		expect(cluster.requests).toHaveLength(0)
	})

	it('returns a failure that is not a Kafka error as a KafkaError caused by it, without retrying it', async () => {
		const failure = new Error('cannot decode')
		const cluster = new FakeCluster(async () => {
			throw failure
		})
		cluster.leaders.set('t:0', 1)

		const [result] = await cluster.admin().deleteRecords([{ topic: 't', partition: 0, offset: 1n }])

		expect(result!.error).toBeInstanceOf(KafkaError)
		expect(result!.error!.message).toBe('cannot decode')
		expect(result!.error!.cause).toBe(failure)
		expect(cluster.requests).toHaveLength(1)
	})
})
