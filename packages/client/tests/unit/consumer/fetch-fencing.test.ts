import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { Cluster } from '@/client/cluster.js'

describe('FetchManager response fencing', () => {
	it('drops records from a stale fetch when the PartitionState was replaced', async () => {
		// Reproduce the bug: in-flight fetch was issued against PartitionState A
		// (offset=10). During the await, the partition is revoked and re-added,
		// replacing the entry with PartitionState B (offset=100). Without the
		// fence, the in-flight response — carrying records around offset 12-15
		// — would clobber B.offset back to 16, causing data loss/replay.

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

		// Initialize a fake fetchBuffer (fetchFromBrokerToBuffer early-returns
		// without one, which would mask the bug behind a no-op). We only need
		// add() and removePartitions() for this code path.
		const addedToBuffer: Array<{ topic: string; partition: number }> = []
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number }) => {
				addedToBuffer.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: vi.fn(),
			isFull: () => false,
		}

		// Stub decodeRecords so we don't have to encode a real record batch.
		// Return records with offsets 12, 13 — these are STALE (from before
		// the rebalance) and would, without the fence, advance state.offset
		// to 14 and clobber the fresh PartitionState's offset of 100.
		fmAny.decodeRecords = vi.fn().mockReturnValue([
			{ offset: 12n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
			{ offset: 13n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
		])

		// Add partition with offset=10
		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const oldState = fmAny.partitionStates.get('t:0')

		// Manually call the private fetch function
		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [oldState])

		// Simulate a rebalance: remove and re-add the partition. New state
		// has a fresh offset (e.g. resumed from a different commit).
		fm.removePartitions([{ topic: 't', partition: 0 }])
		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const newState = fmAny.partitionStates.get('t:0')
		expect(newState).not.toBe(oldState)
		expect(newState.offset).toBe(100n)

		// Resolve the in-flight fetch with NON-EMPTY records (decodeRecords
		// stub above returns 2 records). The fetch loop will:
		// 1. Look up partitionStates.get('t:0') → newState
		// 2. With the fence: detect newState !== oldState → skip
		// 3. Without the fence: advance newState.offset to 14n
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

		// The new state's offset MUST remain at 100n, not be clobbered to 14n.
		expect(newState.offset).toBe(100n)
		// And the stale records must NOT have been added to the fetch buffer.
		expect(addedToBuffer).toEqual([])
	})
})
