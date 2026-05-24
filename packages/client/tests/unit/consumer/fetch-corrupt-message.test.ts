import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

function makeFetchManager() {
	const broker = { nodeId: 1, fetch: vi.fn() }
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
	return { fm, fmAny: fm as any, broker }
}

describe('FetchManager CORRUPT_MESSAGE handling', () => {
	it('surfaces a CORRUPT_MESSAGE fetch error from poll() instead of silently dropping it', async () => {
		const { fm, fmAny } = makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 5n }])
		const state = fmAny.partitionStates.get('t:0')

		// A corrupt-message error for the partition's fetch.
		await fmAny.handleFetchError(state, ErrorCode.CorruptMessage)

		// poll() must raise it (not return empty / loop forever), and not advance past the bad offset.
		await expect(fm.poll()).rejects.toThrow(/corrupt/i)
		expect(state.offset).toBe(5n)
	})

	it('drives CORRUPT_MESSAGE end-to-end through a fetch response', async () => {
		const { fm, fmAny, broker } = makeFetchManager()
		broker.fetch.mockResolvedValue({
			topics: [
				{
					topic: 't',
					partitions: [
						{
							partitionIndex: 0,
							errorCode: ErrorCode.CorruptMessage,
							highWatermark: 0n,
							lastStableOffset: 0n,
							logStartOffset: 0n,
							abortedTransactions: [],
							preferredReadReplica: -1,
							recordsData: null,
						},
					],
				},
			],
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			sessionId: 0,
		})
		// poll() lazily starts the buffer/loop; give it a buffer up front so we can drive one fetch.
		fmAny.fetchBuffer = { add: vi.fn(), isFull: () => false }

		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])
		expect(fmAny.pendingError).toBeInstanceOf(Error)
		expect((fmAny.pendingError as Error).message).toMatch(/corrupt/i)
	})

	it('surfaces an error recorded while poll() is already waiting (not an empty poll)', async () => {
		const { fm, fmAny } = makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		const state = fmAny.partitionStates.get('t:0')

		// Simulate the background loop recording a corrupt-message error shortly after poll() began
		// waiting on an empty buffer (no data added, so the waiter is never signalled).
		setTimeout(() => {
			void fmAny.handleFetchError(state, ErrorCode.CorruptMessage)
		}, 5)

		await expect(fm.poll()).rejects.toThrow(/corrupt/i)
	})
})
