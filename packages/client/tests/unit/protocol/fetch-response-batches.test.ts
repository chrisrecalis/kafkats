import { describe, expect, it } from 'vitest'

import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/record-batch.js'
import { decodePartitionRecordBatches, type FetchPartitionResponse } from '@/protocol/messages/responses/fetch.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

function partitionWith(recordsData: Buffer): FetchPartitionResponse {
	return {
		partitionIndex: 0,
		errorCode: ErrorCode.None,
		highWatermark: 100n,
		lastStableOffset: 100n,
		logStartOffset: 0n,
		abortedTransactions: [],
		preferredReadReplica: -1,
		recordsData,
	}
}

function encodeBatch(baseOffset: bigint, key: string, value: string): Buffer {
	return encodeRecordBatchSync(createRecordBatch([{ key, value }], baseOffset))
}

describe('decodePartitionRecordBatches', () => {
	it('decodes multiple complete batches', async () => {
		const data = Buffer.concat([encodeBatch(0n, 'k1', 'v1'), encodeBatch(1n, 'k2', 'v2')])

		const batches = await decodePartitionRecordBatches(partitionWith(data))

		expect(batches).toHaveLength(2)
		expect(batches[0]!.records[0]!.key?.toString()).toBe('k1')
		expect(batches[1]!.records[0]!.key?.toString()).toBe('k2')
	})

	it('throws on a complete batch with a CRC mismatch instead of silently dropping it', async () => {
		const good = encodeBatch(0n, 'k1', 'v1')
		const corrupt = Buffer.from(encodeBatch(1n, 'k2', 'v2'))
		// Flip a byte inside the CRC-protected record data of a COMPLETE batch.
		const last = corrupt.length - 1
		corrupt.writeUInt8(corrupt.readUInt8(last) ^ 0xff, last)

		await expect(decodePartitionRecordBatches(partitionWith(Buffer.concat([good, corrupt])))).rejects.toThrow(
			/CRC mismatch/
		)
	})

	it('still decodes the complete prefix when the trailing batch is truncated mid-body', async () => {
		const good = encodeBatch(0n, 'k1', 'v1')
		const truncated = encodeBatch(1n, 'k2', 'v2').subarray(0, 20) // header present, body cut off

		const batches = await decodePartitionRecordBatches(partitionWith(Buffer.concat([good, truncated])))

		expect(batches).toHaveLength(1)
		expect(batches[0]!.records[0]!.key?.toString()).toBe('k1')
	})

	it('still decodes the complete prefix when the trailing batch is truncated inside the header', async () => {
		const good = encodeBatch(0n, 'k1', 'v1')
		const truncated = encodeBatch(1n, 'k2', 'v2').subarray(0, 8) // not even baseOffset + batchLength

		const batches = await decodePartitionRecordBatches(partitionWith(Buffer.concat([good, truncated])))

		expect(batches).toHaveLength(1)
	})

	it('returns an empty array for null or empty records data', async () => {
		expect(await decodePartitionRecordBatches(partitionWith(Buffer.alloc(0)))).toEqual([])
	})
})
