import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'

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
	const fmAny = fm as any
	// Buffer is never appended to in these cases (zero user records), but must be non-null.
	fmAny.fetchBuffer = { add: vi.fn(), isFull: () => false }
	return { broker, fm, fmAny }
}

function fetchResponse(recordsData: Buffer, highWatermark: bigint) {
	return {
		topics: [
			{
				topic: 't',
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						highWatermark,
						lastStableOffset: highWatermark,
						logStartOffset: 0n,
						abortedTransactions: [],
						preferredReadReplica: -1,
						recordsData,
					},
				],
			},
		],
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	}
}

describe('FetchManager position advancement past record-less batches', () => {
	it('advances the fetch position past a control-only batch (no user records)', async () => {
		const { broker, fm, fmAny } = makeFetchManager()

		// A control batch at offset 10 (e.g. a transaction marker). It carries one control record
		// but yields zero user records; the position must still advance to 11.
		const controlBatch = encodeRecordBatchSync(createRecordBatch([{ value: 'marker' }], 10n, 0n), {
			isControlBatch: true,
		})
		broker.fetch.mockResolvedValue(fetchResponse(controlBatch, 11n))

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Without advancing here the consumer re-fetches offset 10 forever.
		expect(state.offset).toBe(11n)
		expect(fmAny.fetchBuffer.add).not.toHaveBeenCalled()
	})

	it('advances the fetch position past an empty/compacted batch spanning multiple offsets', async () => {
		const { broker, fm, fmAny } = makeFetchManager()

		// A batch whose records were all compacted away: baseOffset 20, lastOffsetDelta 4 (covers
		// 20..24), zero records. The position must jump to 25.
		const batch = createRecordBatch([], 20n, 0n)
		batch.lastOffsetDelta = 4
		const emptyBatch = encodeRecordBatchSync(batch)
		broker.fetch.mockResolvedValue(fetchResponse(emptyBatch, 25n))

		fm.addPartitions([{ topic: 't', partition: 0, offset: 20n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])

		expect(state.offset).toBe(25n)
	})

	it('keeps the surviving record of a sparse (compacted) batch instead of skipping it', async () => {
		const { broker, fm, fmAny } = makeFetchManager()

		// A compacted batch: baseOffset 0, lastOffsetDelta 10 (declares offsets 0..10), but only one
		// record survived — at offsetDelta 10 (absolute offset 10). recordCount(1) !== lastOffsetDelta+1,
		// so the sequential fast path must NOT be used, otherwise the record decodes as offset 0, gets
		// filtered below the fetch offset (5), and is silently skipped.
		const batch = createRecordBatch([{ value: 'live' }], 0n, 0n)
		batch.records[0]!.offsetDelta = 10
		batch.lastOffsetDelta = 10
		const sparse = encodeRecordBatchSync(batch)
		broker.fetch.mockResolvedValue(fetchResponse(sparse, 11n))

		fm.addPartitions([{ topic: 't', partition: 0, offset: 5n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])

		// The live record at offset 10 must be buffered (decoded with its real offset), not skipped.
		expect(fmAny.fetchBuffer.add).toHaveBeenCalledTimes(1)
		const buffered = fmAny.fetchBuffer.add.mock.calls[0][0]
		expect(buffered.records.map((r: { offset: bigint }) => r.offset)).toEqual([10n])
		expect(state.offset).toBe(11n)
	})
})
