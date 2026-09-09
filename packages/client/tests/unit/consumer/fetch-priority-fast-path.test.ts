import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'

describe('FetchManager default fast path', () => {
	it('drains synchronously with plain buffered fetches when priority is disabled', async () => {
		const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g')
		const manager = new FetchManager(cluster as never, offsetManager, 'earliest', {
			maxBytesPerPartition: 1024,
			maxRecords: 10,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
		})
		await manager.poll()

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const internal = manager as any
		const fetch = { topic: 'topic-a', partition: 0, records: [{ offset: 0n }], byteSize: 10, assignmentEpoch: 0 }
		internal.fetchBuffer.add(fetch)

		expect(Object.keys(internal.fetchBuffer.queue[0])).toEqual([...Object.keys(fetch), 'nextRecord'])
		const drained = internal.drainBuffer()
		expect(drained).not.toBeInstanceOf(Promise)
		expect(drained[0].records).toEqual([{ offset: 0n }])
		manager.stop()
	})
})
