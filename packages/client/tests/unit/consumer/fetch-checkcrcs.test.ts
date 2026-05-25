import { describe, expect, it } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'

function makeFetchManager(checkCrcs: boolean) {
	const cluster = {
		getLeaderForPartition: () => undefined,
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
			checkCrcs,
		}
	)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return fm as any
}

// A valid batch with its CRC field corrupted (the record data itself is intact, so decode
// succeeds when CRC checking is off and only the checksum comparison fails when it is on).
function batchWithBadCrc(): Buffer {
	const buf = Buffer.from(encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n)))
	// CRC is the uint32 at offset 17: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1).
	buf[17] = buf[17]! ^ 0xff
	return buf
}

describe('FetchManager checkCrcs', () => {
	it('throws on a corrupt batch CRC when checkCrcs is true', () => {
		const fm = makeFetchManager(true)
		expect(() => fm.decodeRecords(batchWithBadCrc(), [], 100n)).toThrow(/CRC mismatch/)
	})

	it('skips CRC verification and decodes the batch when checkCrcs is false', () => {
		const fm = makeFetchManager(false)
		const result = fm.decodeRecords(batchWithBadCrc(), [], 100n) as {
			records: DecodedRecord[]
			nextOffset: bigint | null
		}
		expect(result).not.toBeInstanceOf(Promise)
		expect(result.records.map(r => r.value?.toString('utf-8'))).toEqual(['hello'])
	})
})
