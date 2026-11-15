import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'

function setUp(fetch: (request: unknown, owner?: object) => Promise<unknown>) {
	const broker = { nodeId: 1, fetch: vi.fn(fetch), closeFetchConnection: vi.fn().mockResolvedValue(undefined) }
	const cluster = {
		getLeaderForPartition: vi.fn().mockResolvedValue(broker),
		getLogger: () => null,
	}
	const onError = vi.fn()
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g1')
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fm = new FetchManager(cluster as any, offsetManager, 'earliest', {
		maxBytesPerPartition: 1024,
		minBytes: 1,
		maxWaitMs: 5000,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
		onError,
	})
	fm.setPartitions([{ topic: 't', partition: 0, offset: 0n }])
	return { broker, fm, onError }
}

describe('FetchManager fetch connections', () => {
	it('fetches on a connection of its own and closes it on stop', async () => {
		// A long poll that never answers by itself; closing the connection rejects it.
		let rejectFetch!: (error: Error) => void
		const { broker, fm, onError } = setUp(
			() =>
				new Promise((_, reject) => {
					rejectFetch = reject
				})
		)

		void fm.poll()
		await vi.waitFor(() => expect(broker.fetch).toHaveBeenCalledTimes(1))
		expect(broker.fetch.mock.calls[0]![1]).toBe(fm)

		fm.stop()
		expect(broker.closeFetchConnection).toHaveBeenCalledWith(fm)

		// The long poll fails because its connection was closed: not a fetch failure to report.
		rejectFetch(new Error('Connection closing'))
		await new Promise(resolve => setTimeout(resolve, 10))
		expect(onError).not.toHaveBeenCalled()
	})
})
