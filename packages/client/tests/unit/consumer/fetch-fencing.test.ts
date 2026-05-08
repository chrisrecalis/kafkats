import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('FetchManager response fencing', () => {
	it('drops records from a stale fetch when the PartitionState was replaced', async () => {
		let resolveFetch: (resp: unknown) => void = () => {}
		const fetchResponse = new Promise(resolve => {
			resolveFetch = resolve
		})

		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockReturnValue(fetchResponse),
		}

		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		const fm = new FetchManager(
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			cluster as any,
			offsetManager,
			'earliest',
			{
				maxBytesPerPartition: 1024,
				minBytes: 1,
				maxWaitMs: 50,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fmAny = fm as any

		const addedToBuffer: Array<{ topic: string; partition: number }> = []
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number }) => {
				addedToBuffer.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: vi.fn(),
			isFull: () => false,
		}

		// Stale records (offsets 12-13) that would clobber the fresh state's offset of 100n without the fence.
		fmAny.decodeRecords = vi.fn().mockReturnValue([
			{ offset: 12n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
			{ offset: 13n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
		])

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const oldState = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [oldState])

		fm.removePartitions([{ topic: 't', partition: 0 }])
		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const newState = fmAny.partitionStates.get('t:0')
		expect(newState).not.toBe(oldState)
		expect(newState.offset).toBe(100n)

		resolveFetch({
			topics: [
				{
					topic: 't',
					partitions: [
						{
							partitionIndex: 0,
							errorCode: ErrorCode.None,
							highWatermark: 50n,
							lastStableOffset: 50n,
							logStartOffset: 0n,
							abortedTransactions: null,
							preferredReadReplica: -1,
							recordsData: Buffer.from('non-empty'),
						},
					],
				},
			],
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			sessionId: 0,
		})

		await inflight

		expect(newState.offset).toBe(100n)
		expect(addedToBuffer).toEqual([])
	})
})
