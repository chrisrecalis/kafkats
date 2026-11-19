import { describe, expect, it, onTestFinished, vi } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import { FetchManager } from '@/consumer/fetch-manager.js'
import type { OffsetManager } from '@/consumer/offset-manager.js'
import { fetchResponse, TOPIC, tp } from './fetch-harness.js'

interface FetchRequest {
	topics: Array<{ partitions: Array<{ partitionIndex: number; fetchOffset: bigint }> }>
}

/** A FetchManager whose background loop fetches from one broker that answers every Fetch with no records. */
function runningFetchManager() {
	const requests: FetchRequest[] = []
	const broker = {
		nodeId: 1,
		fetch: vi.fn(async (request: FetchRequest) => {
			requests.push(request)
			await new Promise(resolve => setTimeout(resolve, 2))
			const partitions = request.topics[0]!.partitions
			return fetchResponse(...partitions.map(({ partitionIndex }) => ({ partition: partitionIndex })))
		}),
		closeFetchConnection: vi.fn().mockResolvedValue(undefined),
	}
	const cluster = { getLeaderForPartition: vi.fn().mockResolvedValue(broker), getLogger: () => null }
	const fm = new FetchManager(cluster as unknown as Cluster, {} as OffsetManager, 'earliest', {
		maxWaitMs: 1,
		isolationLevel: 'read_uncommitted',
	})
	onTestFinished(() => fm.stop())

	/** The partitions requested by the fetches made after `from` (an index into all requests). */
	const fetchedSince = (from: number) =>
		new Set(requests.slice(from).flatMap(request => request.topics[0]!.partitions.map(p => p.partitionIndex)))
	/** Wait for a few more fetches, so a partition that is fetchable has certainly been included. */
	const fetchesAfter = async (from = requests.length) => {
		await vi.waitFor(() => expect(requests.length).toBeGreaterThanOrEqual(from + 3))
		return fetchedSince(from)
	}
	return { fm, requests, fetchesAfter }
}

describe('FetchManager held partitions', () => {
	it('does not fetch a held partition until it is released', async () => {
		const { fm, fetchesAfter } = runningFetchManager()
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		fm.addPartitions([{ ...tp(1), offset: 0n }], { held: true })
		await fm.poll()

		expect(await fetchesAfter()).toEqual(new Set([0]))
		expect(fm.currentLag(TOPIC, 1)).toBeUndefined()
		expect(fm.nextOffset(TOPIC, 1)).toBe(0n)

		fm.releaseHeld([tp(1)])
		expect(await fetchesAfter()).toEqual(new Set([0, 1]))
	})

	it('first fetches a held partition at the offset it was sought to while held', async () => {
		const { fm, requests } = runningFetchManager()
		fm.addPartitions([{ ...tp(0), offset: 0n }], { held: true })
		fm.seekPartition(TOPIC, 0, 42n)
		fm.releaseHeld([tp(0)])
		await fm.poll()

		await vi.waitFor(() => expect(requests.length).toBeGreaterThan(0))
		expect(requests[0]!.topics[0]!.partitions[0]!.fetchOffset).toBe(42n)
	})

	it('keeps a partition paused while held paused after its release, until it is resumed', async () => {
		const { fm, fetchesAfter } = runningFetchManager()
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		fm.addPartitions([{ ...tp(1), offset: 0n }], { held: true })
		fm.pausePartitions([tp(1)])
		fm.releaseHeld([tp(1)])
		await fm.poll()

		expect(await fetchesAfter()).toEqual(new Set([0]))
		expect(fm.partitionMetrics().find(p => p.partition === 1)!.paused).toBe(true)

		fm.resumePartitions([tp(1)])
		expect(await fetchesAfter()).toEqual(new Set([0, 1]))
	})

	it('does not release a held partition on resume', async () => {
		const { fm, fetchesAfter } = runningFetchManager()
		fm.addPartitions([{ ...tp(0), offset: 0n }])
		fm.addPartitions([{ ...tp(1), offset: 0n }], { held: true })
		fm.pausePartitions([tp(1)])
		fm.resumePartitions([tp(1)])
		await fm.poll()

		expect(await fetchesAfter()).toEqual(new Set([0]))

		fm.releaseHeld([tp(1)])
		expect(await fetchesAfter()).toEqual(new Set([0, 1]))
	})

	it('keeps the state of a partition that is already assigned when it is added held again', async () => {
		const { fm, fetchesAfter } = runningFetchManager()
		fm.addPartitions([{ ...tp(0), offset: 5n }])
		fm.addPartitions([{ ...tp(0), offset: 0n }], { held: true })
		await fm.poll()

		expect(await fetchesAfter()).toEqual(new Set([0]))
		expect(fm.getFetchPosition(TOPIC, 0)).toBe(5n)
	})
})
