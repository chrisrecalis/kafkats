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

export function buildProduceResponse(
	topics: Array<{
		name: string
		partitions: Array<{ partitionIndex: number; errorCode: ErrorCode; baseOffset?: bigint }>
	}>
): ProduceResponse {
	return {
		topics: topics.map(t => ({
			name: t.name,
			partitions: t.partitions.map(p => ({
				partitionIndex: p.partitionIndex,
				errorCode: p.errorCode,
				baseOffset: p.baseOffset ?? 0n,
				logAppendTimeMs: BigInt(Date.now()),
				logStartOffset: 0n,
				recordErrors: [],
				errorMessage: null,
			})),
		})),
		throttleTimeMs: 0,
	}
}
