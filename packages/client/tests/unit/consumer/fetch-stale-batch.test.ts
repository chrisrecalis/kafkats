import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import type { DecodedRecord } from '@/protocol/records/index.js'

// An eager rebalance removes and re-adds retained partitions in FetchManager. A batch
// drained by poll() BEFORE the rebalance must not be delivered afterwards: the partition
// was re-added at the committed offset, so the same records will be fetched again and a
// plain "is the partition assigned" check would double-deliver them.
function buildFetchManager() {
	const cluster = {
		getLeaderForPartition: vi.fn().mockRejectedValue(new Error('no broker in unit test')),
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = {} as any
	const fm = new FetchManager(
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster as any,
		offsetManager,
		'earliest',
		{ maxWaitMs: 20 }
	)
	return fm
}

function fakeRecords(offsets: bigint[]): DecodedRecord[] {
	return offsets.map(offset => ({ offset }) as unknown as DecodedRecord)
}

describe('FetchManager stale batch detection across eager rebalance', () => {
	it('drops a drained batch when its partition was removed and re-added between drain and delivery', async () => {
		const fm = buildFetchManager()
		try {
			fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

			// First poll lazily initializes the fetch buffer / background loop.
			await fm.poll()

			// Inject a completed fetch the way the background loop would buffer it.
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			;(fm as any).fetchBuffer.add({
				topic: 't',
				partition: 0,
				records: fakeRecords([0n, 1n]),
				byteSize: 128,
				assignmentEpoch: 0,
			})

			const batches = await fm.poll()
			expect(batches).toHaveLength(1)

			// Batch drained while the partition is still owned: deliverable.
			expect(fm.isBatchAssigned(batches[0]!)).toBe(true)

			// Eager rebalance between drain and delivery: partition removed, then re-added
			// (retained partition) at its committed offset.
			fm.removePartitions([{ topic: 't', partition: 0 }])
			fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

			// The stale drained batch must NOT be considered deliverable anymore — its
			// records will be re-fetched from the committed offset by the new assignment.
			expect(fm.isBatchAssigned(batches[0]!)).toBe(false)

			// A batch buffered AFTER the re-add carries the new epoch and is deliverable.
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			;(fm as any).fetchBuffer.add({
				topic: 't',
				partition: 0,
				records: fakeRecords([0n, 1n]),
				byteSize: 128,
				assignmentEpoch: fm.getAssignmentEpoch('t', 0),
			})
			const fresh = await fm.poll()
			expect(fresh).toHaveLength(1)
			expect(fm.isBatchAssigned(fresh[0]!)).toBe(true)
		} finally {
			fm.stop()
		}
	})

	it('still treats batches from fully-revoked partitions as not deliverable', async () => {
		const fm = buildFetchManager()
		try {
			fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
			await fm.poll()

			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			;(fm as any).fetchBuffer.add({
				topic: 't',
				partition: 0,
				records: fakeRecords([0n]),
				byteSize: 64,
				assignmentEpoch: 0,
			})
			const batches = await fm.poll()
			expect(batches).toHaveLength(1)

			// Partition moved to another consumer and never came back.
			fm.removePartitions([{ topic: 't', partition: 0 }])
			expect(fm.isBatchAssigned(batches[0]!)).toBe(false)
		} finally {
			fm.stop()
		}
	})
})
