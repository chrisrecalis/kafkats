import { describe, expect, it } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'

function makeFetchManager() {
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
		}
	)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return fm as any
}

describe('FetchManager.decodeRecords corruption handling', () => {
	it('throws on a corrupt leading batch instead of silently dropping the partition records', () => {
		const fm = makeFetchManager()
		// Too short to even read a record-batch header → the first batch fails to decode.
		// The broker always returns at least one complete batch, so this is genuine
		// corruption, not a truncated trailing batch.
		const corrupt = Buffer.from([1, 2, 3, 4, 5])
		expect(() => fm.decodeRecords(corrupt, [], 100n)).toThrow(/corrupt data/)
	})

	it('stops at a truncated trailing batch but returns the records decoded before it', () => {
		const fm = makeFetchManager()
		const valid = encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n))
		// One complete batch followed by a truncated second batch (header underflows).
		const buf = Buffer.concat([valid, valid.subarray(0, 6)])

		const result = fm.decodeRecords(buf, [], 100n) as
			| { records: DecodedRecord[]; nextOffset: bigint | null }
			| Promise<{ records: DecodedRecord[]; nextOffset: bigint | null }>
		expect(result).not.toBeInstanceOf(Promise)
		const { records } = result as { records: DecodedRecord[]; nextOffset: bigint | null }
		expect(records.map(r => r.value?.toString('utf-8'))).toEqual(['hello'])
	})
})
