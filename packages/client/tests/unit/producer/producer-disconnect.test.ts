import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { createMockBroker, createMockCluster, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer.disconnect()', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('rejects orphaned send promises when flush() throws during disconnect', async () => {
		const producer = new Producer(mockCluster, { lingerMs: 0, retries: 0 })
		mockBroker.produce.mockRejectedValue(new Error('broker exploded'))

		const sendPromise = producer.send(TOPIC, { value: Buffer.from('m1') })
		sendPromise.catch(() => {}) // suppress unhandled rejection

		await producer.disconnect()
		await expect(sendPromise).rejects.toThrow()
	})

	it('rejects sends still queued in the accumulator (no broker call yet)', async () => {
		// lingerMs huge → message sits in accumulator without a batchReady event;
		// disconnect must reject the promise via clearWithRejection.
		const producer = new Producer(mockCluster, { lingerMs: 60_000, retries: 0 })

		const sendPromise = producer.send(TOPIC, { value: Buffer.from('m1') })
		sendPromise.catch(() => {})

		// Force flush() to throw so the cleanup branch runs (otherwise flush would
		// drain the accumulator and broker.produce would be called).
		mockBroker.produce.mockRejectedValue(new Error('broker exploded'))

		await producer.disconnect()
		await expect(sendPromise).rejects.toThrow()
	})
})
