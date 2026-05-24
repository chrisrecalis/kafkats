import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('FetchManager OffsetOutOfRange handling', () => {
	it('discards buffered records for a partition when its fetch offset is reset out of range', async () => {
		// A fetch for t:0 comes back OffsetOutOfRange (e.g. retention deleted the segment
		// the consumer was positioned at). The partition still has records buffered from an
		// earlier successful fetch at the now-invalid position; those must be discarded when
		// we reset, otherwise the consumer delivers pre-reset records after the reset.
		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockResolvedValue({
				topics: [
					{
						topic: 't',
						partitions: [
							{
								partitionIndex: 0,
								errorCode: ErrorCode.OffsetOutOfRange,
								recordsData: null,
							},
						],
					},
				],
			}),
		}

		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		// Retention has advanced the log start; earliest is now 200n.
		vi.spyOn(offsetManager, 'getEarliestOffset').mockResolvedValue(200n)

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

		const removed: Array<{ topic: string; partition: number }> = []
		fmAny.fetchBuffer = {
			add: vi.fn(),
			removePartitions: vi.fn((parts: Array<{ topic: string; partition: number }>) => {
				removed.push(...parts)
			}),
			isFull: () => false,
		}

		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Offset is reset to the new earliest...
		expect(state.offset).toBe(200n)
		// ...and the now-stale buffered records for this partition are discarded.
		expect(removed).toContainEqual({ topic: 't', partition: 0 })
	})

	it('discards buffered records BEFORE the listOffsets round-trip (no stale delivery window)', async () => {
		// The buffer must be cleared synchronously, before awaiting the offset lookup —
		// otherwise a concurrent poll() could drain the stale records while we await, and a
		// transient lookup failure would skip the clear entirely.
		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockResolvedValue({
				topics: [
					{
						topic: 't',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.OffsetOutOfRange, recordsData: null }],
					},
				],
			}),
		}

		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')

		// A slow listOffsets round-trip: resolve it only after we have observed the clear.
		let resolveOffset: (o: bigint) => void = () => {}
		const offsetLookup = new Promise<bigint>(resolve => {
			resolveOffset = resolve
		})
		let lookupStarted = false
		vi.spyOn(offsetManager, 'getEarliestOffset').mockImplementation(() => {
			lookupStarted = true
			return offsetLookup
		})

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

		let clearedDuringLookup = false
		fmAny.fetchBuffer = {
			add: vi.fn(),
			removePartitions: vi.fn(() => {
				// The clear must run before the (still-pending) offset lookup resolves.
				if (!lookupStarted) clearedDuringLookup = true
			}),
			isFull: () => false,
		}

		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const state = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Give the microtask queue a chance to reach the await on the offset lookup.
		await new Promise(resolve => setTimeout(resolve, 0))

		// Buffer was cleared before the lookup resolved (i.e. before the await).
		expect(clearedDuringLookup).toBe(true)
		expect(fmAny.fetchBuffer.removePartitions).toHaveBeenCalledWith([{ topic: 't', partition: 0 }])

		// Let the lookup complete and the handler finish.
		resolveOffset(200n)
		await inflight
		expect(state.offset).toBe(200n)
	})
})
