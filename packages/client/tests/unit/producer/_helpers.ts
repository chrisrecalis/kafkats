import { mockDeep, type DeepMockProxy } from 'vitest-mock-extended'

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'

export type MockCluster = DeepMockProxy<Cluster>
export type MockBroker = DeepMockProxy<Broker>

export function createMockCluster(opts: { partitionCount?: number } = {}): MockCluster {
	const partitionCount = opts.partitionCount ?? 1
	const partitions = new Map(
		Array.from({ length: partitionCount }, (_, i) => [
			i,
			{ partitionIndex: i, leaderId: 1, leaderEpoch: 0, replicaNodes: [1], isrNodes: [1], offlineReplicas: [] },
		])
	)

	const cluster = mockDeep<Cluster>()
	cluster.refreshMetadata.mockResolvedValue({
		clusterId: 'test',
		controllerId: 1,
		brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
		topics: new Map([['test-topic', { name: 'test-topic', topicId: 'test-id', isInternal: false, partitions }]]),
		updatedAt: Date.now(),
	})
	return cluster
}

export function createMockBroker(nodeId: number): MockBroker {
	const broker = mockDeep<Broker>()
	Object.assign(broker, {
		nodeId,
		host: `broker-${nodeId}.test`,
		port: 9092,
		isConnected: true,
	})
	broker.getApiVersion.mockReturnValue(9)
	return broker
}

/**
 * A cluster whose transaction coordinator, group coordinator and partition leader answer every transactional request
 * of `topic` partition 0 with success (producer id 1, epoch 0).
 */
export function createTransactionalMocks(topic = 'test-topic', options: { partitionCount?: number } = {}) {
	const cluster = createMockCluster(options)
	const txnCoordinator = createMockBroker(1)
	const groupCoordinator = createMockBroker(2)
	const leader = createMockBroker(3)

	cluster.getCoordinator.mockImplementation(async type =>
		type === 'TRANSACTION' ? txnCoordinator : groupCoordinator
	)
	cluster.getAnyBroker.mockResolvedValue(txnCoordinator)
	cluster.getLeaderForPartition.mockResolvedValue(leader)

	txnCoordinator.initProducerId.mockResolvedValue({
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		producerId: 1n,
		producerEpoch: 0,
	})
	txnCoordinator.addPartitionsToTxn.mockResolvedValue({
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		results: [{ name: topic, resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
	})
	txnCoordinator.addOffsetsToTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	txnCoordinator.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	groupCoordinator.txnOffsetCommit.mockResolvedValue({
		throttleTimeMs: 0,
		topics: [{ name: topic, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
	})
	leader.produce.mockResolvedValue(
		buildProduceResponse([{ name: topic, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }])
	)

	return { cluster, txnCoordinator, groupCoordinator, leader }
}

export function buildProduceResponse(
	topics: Array<{
		name: string
		partitions: Array<{
			partitionIndex: number
			errorCode: ErrorCode
			baseOffset?: bigint
			logAppendTimeMs?: bigint
		}>
	}>
): ProduceResponse {
	return {
		topics: topics.map(t => ({
			name: t.name,
			partitions: t.partitions.map(p => ({
				partitionIndex: p.partitionIndex,
				errorCode: p.errorCode,
				baseOffset: p.baseOffset ?? 0n,
				logAppendTimeMs: p.logAppendTimeMs ?? BigInt(Date.now()),
				logStartOffset: 0n,
				recordErrors: [],
				errorMessage: null,
			})),
		})),
		throttleTimeMs: 0,
	}
}
