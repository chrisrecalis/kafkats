import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

/** Record count of an encoded record batch (int32 at byte 57 of the v2 batch header). */
const recordCount = (batch: Buffer): number => batch.readInt32BE(57)

describe('Producer: batches queued behind an in-flight request are merged', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>
	/** One entry per produce request: records and encoded bytes of its partition-0 batch. */
	let requests: Array<{ records: number; bytes: number }>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		requests = []
		let nextOffset = 0n
		mockBroker.produce.mockImplementation(async (request: ProduceRequest) => {
			const batch = request.topics[0]!.partitions[0]!.records
			requests.push({ records: recordCount(batch), bytes: batch.length })
			const baseOffset = nextOffset
			nextOffset += BigInt(recordCount(batch))
			await new Promise(resolve => setTimeout(resolve, 5))
			return buildProduceResponse([
				{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset }] },
			])
		})
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('sends concurrent single-record sends in few requests, in order, never above maxBatchBytes', async () => {
		const maxBatchBytes = 1000
		const producer = new Producer(mockCluster, { maxBatchBytes, retries: 0 })
		const value = Buffer.alloc(60, 'x')

		const results = await Promise.all(
			Array.from({ length: 200 }, () => producer.send('test-topic', { value, partition: 0 }))
		)

		// Everything arrived, in the order it was sent.
		expect(requests.reduce((sum, r) => sum + r.records, 0)).toBe(200)
		expect(results.map(r => r.offset)).toEqual(Array.from({ length: 200 }, (_, i) => BigInt(i)))
		// Merged: far fewer requests than records...
		expect(requests.length).toBeLessThan(50)
		// ...but no batch grows past the configured batch size.
		for (const request of requests) {
			expect(request.bytes).toBeLessThanOrEqual(maxBatchBytes)
		}

		await producer.disconnect()
	})

	it('appends back-to-back sends in call order even when their preparation takes different times', async () => {
		// The first lookup of a topic's partition count is a metadata request; make the first
		// one slow and later ones fast, so a later send would overtake an earlier one unless the
		// preparation stage runs in call order.
		const metadata = await mockCluster.refreshMetadata(['test-topic'])
		let lookups = 0
		mockCluster.refreshMetadata.mockImplementation(async () => {
			const delay = lookups++ === 0 ? 20 : 0
			await new Promise(resolve => setTimeout(resolve, delay))
			return metadata
		})
		const producer = new Producer(mockCluster, { retries: 0 })

		const results = await Promise.all(
			Array.from({ length: 20 }, (_, i) =>
				producer.send('test-topic', { value: Buffer.from(String(i)), partition: 0 })
			)
		)

		expect(results.map(r => r.offset)).toEqual(Array.from({ length: 20 }, (_, i) => BigInt(i)))

		await producer.disconnect()
	})

	it('does not merge anything into a batch that is already at the limit', async () => {
		const maxBatchBytes = 1000
		const producer = new Producer(mockCluster, { maxBatchBytes, retries: 0 })
		const small = Buffer.alloc(60, 'x')
		// One send() call is one batch, whatever its size: this one alone exceeds the limit.
		const oversized = Array.from({ length: 30 }, () => ({ value: small, partition: 0 }))

		const first = producer.send('test-topic', { value: small, partition: 0 })
		const big = producer.send('test-topic', oversized)
		const after = producer.send('test-topic', { value: small, partition: 0 })
		await Promise.all([first, big, after])

		// The oversized send keeps its own request; the sends around it are not folded into it.
		expect(requests.map(r => r.records)).toEqual([1, 30, 1])

		await producer.disconnect()
	})
})
