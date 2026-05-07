import { vi } from 'vitest'

import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'

export function createMockCluster() {
	const mockBrokers = new Map<number, ReturnType<typeof createMockBroker>>()

	const mockCluster = {
		getLeaderForPartition: vi.fn(),
		refreshMetadata: vi.fn().mockResolvedValue({
			clusterId: 'test',
			controllerId: 1,
			brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
			topics: new Map([
				[
					'test-topic',
					{
						name: 'test-topic',
						partitions: new Map([[0, { partitionIndex: 0, leaderId: 1 }]]),
					},
				],
			]),
			updatedAt: Date.now(),
		}),
		getCoordinator: vi.fn(),
		getAnyBroker: vi.fn(),
		getLogger: vi.fn().mockReturnValue({
			child: () => ({
				debug: vi.fn(),
				info: vi.fn(),
				warn: vi.fn(),
				error: vi.fn(),
			}),
			debug: vi.fn(),
			info: vi.fn(),
			warn: vi.fn(),
			error: vi.fn(),
		}),
		getMockBroker: (nodeId: number) => mockBrokers.get(nodeId),
		setMockBroker: (nodeId: number, broker: ReturnType<typeof createMockBroker>) => {
			mockBrokers.set(nodeId, broker)
		},
	}

	return mockCluster as unknown as Cluster & {
		getLeaderForPartition: ReturnType<typeof vi.fn>
		refreshMetadata: ReturnType<typeof vi.fn>
		getCoordinator: ReturnType<typeof vi.fn>
		getAnyBroker: ReturnType<typeof vi.fn>
		getMockBroker: (nodeId: number) => ReturnType<typeof createMockBroker> | undefined
		setMockBroker: (nodeId: number, broker: ReturnType<typeof createMockBroker>) => void
	}
}

export function createMockBroker(nodeId: number) {
	return {
		nodeId,
		host: `broker-${nodeId}.test`,
		port: 9092,
		isConnected: true,
		produce: vi.fn(),
		initProducerId: vi.fn(),
		getApiVersion: vi.fn().mockReturnValue(9),
	} as unknown as Broker & { produce: ReturnType<typeof vi.fn>; initProducerId: ReturnType<typeof vi.fn> }
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
