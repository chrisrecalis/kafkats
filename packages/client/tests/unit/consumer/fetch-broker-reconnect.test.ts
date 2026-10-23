import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ConnectionClosedError } from '@/network/errors.js'

const pending = () => new Promise<never>(() => {})
const disconnected = new ConnectionClosedError('Cannot send on disconnected connection')

describe('FetchManager broker reconnect', () => {
	let manager: FetchManager

	function createManager(cluster: object, maxBufferedBytes = 10_240) {
		manager = new FetchManager(cluster as never, new OffsetManager(cluster as never, 'g'), 'earliest', {
			maxBytesPerPartition: 1024,
			maxWaitMs: 1,
			isolationLevel: 'read_uncommitted',
			maxBufferedBytes,
		})
		manager.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		return manager
	}

	async function start() {
		const poll = manager.poll()
		await vi.advanceTimersByTimeAsync(1)
		await poll
	}

	beforeEach(() => vi.useFakeTimers())
	afterEach(() => {
		manager?.stop()
		vi.useRealTimers()
	})

	it.each([false, true])('reconnects after a fetch fails (metadata refresh fails: %s)', async refreshFails => {
		const closedBroker = { nodeId: 1, fetch: vi.fn().mockRejectedValue(disconnected) }
		const reconnectedBroker = { nodeId: 1, fetch: vi.fn(pending) }
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValueOnce(closedBroker).mockResolvedValue(reconnectedBroker),
			refreshMetadata: refreshFails
				? vi.fn().mockRejectedValue(disconnected)
				: vi.fn().mockResolvedValue(undefined),
		}
		createManager(cluster)
		await start()
		await vi.advanceTimersByTimeAsync(250)

		expect(closedBroker.fetch).toHaveBeenCalledTimes(1)
		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['t'])
		expect(reconnectedBroker.fetch).toHaveBeenCalledTimes(1)
	})

	it('backs off failed leader lookups while another broker is in flight', async () => {
		const healthyBroker = { nodeId: 2, fetch: vi.fn(pending) }
		const attempts: number[] = []
		const cluster = {
			getLeaderForPartition: vi.fn(async (_topic: string, partition: number) => {
				if (partition === 1) return healthyBroker
				attempts.push(Date.now())
				throw disconnected
			}),
		}
		createManager(cluster).addPartitions([{ topic: 't', partition: 1, offset: 0n }])
		await start()
		await vi.advanceTimersByTimeAsync(350)

		expect(attempts).toHaveLength(4)
		for (let i = 1; i < attempts.length; i++) expect(attempts[i]! - attempts[i - 1]!).toBeGreaterThanOrEqual(100)
	})

	it('keeps fetching healthy brokers during a reconnect and deduplicates the pending lookup', async () => {
		let resolveLeader!: (broker: typeof reconnectedBroker) => void
		const closedBroker = { nodeId: 1, fetch: vi.fn().mockRejectedValue(disconnected) }
		const reconnectedBroker = { nodeId: 1, fetch: vi.fn(pending) }
		const healthyBroker = { nodeId: 2, fetch: vi.fn().mockResolvedValue({ topics: [] }) }
		const reconnect = new Promise<typeof reconnectedBroker>(resolve => {
			resolveLeader = resolve
		})
		const lookup = vi.fn().mockResolvedValueOnce(closedBroker).mockReturnValue(reconnect)
		const cluster = {
			getLeaderForPartition: vi.fn((_topic: string, partition: number) =>
				partition === 0 ? lookup() : Promise.resolve(healthyBroker)
			),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
		}
		createManager(cluster).addPartitions([{ topic: 't', partition: 1, offset: 0n }])
		await start()
		await vi.advanceTimersByTimeAsync(150)
		const healthyFetches = healthyBroker.fetch.mock.calls.length
		await vi.advanceTimersByTimeAsync(150)

		expect(healthyBroker.fetch.mock.calls.length).toBeGreaterThan(healthyFetches)
		expect(lookup).toHaveBeenCalledTimes(2)
		resolveLeader(reconnectedBroker)
		await vi.advanceTimersByTimeAsync(5)
		expect(reconnectedBroker.fetch).toHaveBeenCalledTimes(1)
	})

	it('releases failed fetch capacity while metadata refresh is pending', async () => {
		const closedBroker = { nodeId: 1, fetch: vi.fn().mockRejectedValue(disconnected) }
		const healthyBroker = { nodeId: 2, fetch: vi.fn(pending) }
		const cluster = {
			getLeaderForPartition: vi.fn(async (_topic: string, partition: number) => {
				if (partition === 1) return healthyBroker
				return closedBroker
			}),
			refreshMetadata: vi.fn(pending),
		}
		createManager(cluster, 1024)
		await start()
		manager.removePartitions([{ topic: 't', partition: 0 }])
		manager.addPartitions([{ topic: 't', partition: 1, offset: 0n }])
		await vi.advanceTimersByTimeAsync(150)

		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['t'])
		expect(healthyBroker.fetch).toHaveBeenCalledTimes(1)
	})
})
