import { afterEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { TimeoutError } from '@/client/errors.js'
import { createMockCluster } from './_helpers.js'

describe('Producer.partitionCount()', () => {
	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('looks the topic up once and answers from the cache afterwards', async () => {
		const cluster = createMockCluster({ partitionCount: 3 })
		const producer = new Producer(cluster)

		await expect(producer.partitionCount('test-topic')).resolves.toBe(3)
		await expect(producer.partitionCount('test-topic')).resolves.toBe(3)
		expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['test-topic'])
	})

	it('shares the cache with send()', async () => {
		const cluster = createMockCluster({ partitionCount: 2 })
		const producer = new Producer(cluster)

		await producer.partitionCount('test-topic')
		// Appending needs no lookup now: the partition is validated against the cached count synchronously.
		await expect(producer.send('test-topic', { value: Buffer.from('v'), partition: 5 })).rejects.toThrow(
			/Invalid partition 5/
		)
		expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
	})

	it('rejects with a TimeoutError when the topic does not show up within timeoutMs', async () => {
		const producer = new Producer(createMockCluster())

		await expect(producer.partitionCount('missing-topic', { timeoutMs: 30 })).rejects.toThrow(TimeoutError)
	})

	it('looks up nothing more once timeoutMs passed', async () => {
		vi.useFakeTimers()
		try {
			const cluster = createMockCluster()
			const producer = new Producer(cluster)

			const counted = expect(producer.partitionCount('missing-topic', { timeoutMs: 30 })).rejects.toThrow(
				TimeoutError
			)
			await vi.advanceTimersByTimeAsync(30)
			await counted
			await vi.advanceTimersByTimeAsync(2_000)
			expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
		} finally {
			vi.useRealTimers()
		}
	})

	it('rejects with the reason of an aborted signal', async () => {
		const producer = new Producer(createMockCluster())
		const controller = new AbortController()

		const counting = producer.partitionCount('missing-topic', { signal: controller.signal })
		controller.abort(new Error('closing'))

		await expect(counting).rejects.toThrow('closing')
	})

	it('rejects when the topic does not exist', async () => {
		vi.useFakeTimers()
		try {
			const producer = new Producer(createMockCluster())

			const counted = expect(producer.partitionCount('missing-topic')).rejects.toThrow(
				'Topic missing-topic not found'
			)
			await vi.advanceTimersByTimeAsync(2_000)
			await counted
		} finally {
			vi.useRealTimers()
		}
	})
})
