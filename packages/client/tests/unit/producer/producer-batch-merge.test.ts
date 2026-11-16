import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'
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

	it('does not hold a send behind the preparation of a send to another topic', async () => {
		const metadata = await mockCluster.refreshMetadata(['test-topic'])
		let releaseSlowLookup!: () => void
		const slowLookup = new Promise<void>(resolve => {
			releaseSlowLookup = resolve
		})
		mockCluster.refreshMetadata.mockImplementation(async (topics?: string[]) => {
			if (topics?.includes('slow-topic')) await slowLookup
			return metadata
		})
		const producer = new Producer(mockCluster, { retries: 0 })

		const slow = producer.send('slow-topic', { value: Buffer.from('s'), partition: 0 })
		slow.catch(() => {})
		const fast = await producer.send('test-topic', { value: Buffer.from('f'), partition: 0 })
		expect(fast.offset).toBe(0n)

		releaseSlowLookup()
		await producer.disconnect()
		await slow.catch(() => {})
	})

	it('fails the sends waiting for a topic that is not found together, not one lookup after another', async () => {
		const found = await mockCluster.refreshMetadata(['test-topic'])
		let exists = false
		mockCluster.refreshMetadata.mockImplementation(async () => (exists ? found : { ...found, topics: new Map() }))
		mockCluster.refreshMetadata.mockClear()
		const producer = new Producer(mockCluster, { retries: 0 })

		const sends = Array.from({ length: 5 }, (_, i) =>
			producer.send('test-topic', { value: Buffer.from(String(i)), partition: 0 })
		)
		const outcomes = await Promise.allSettled(sends)

		expect(outcomes.map(o => (o.status === 'rejected' ? (o.reason as Error).message : 'sent'))).toEqual(
			Array.from({ length: 5 }, () => 'Topic test-topic not found')
		)
		// One lookup (five attempts) answered all of them.
		expect(mockCluster.refreshMetadata).toHaveBeenCalledTimes(5)

		// A send issued after that lookup looks again, so a topic created since is found.
		exists = true
		expect((await producer.send('test-topic', { value: Buffer.from('later'), partition: 0 })).offset).toBe(0n)

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

	it('keeps every partition in order when partitions back up independently', async () => {
		const maxBatchBytes = 1000
		const partitionCount = 3
		mockCluster = createMockCluster({ partitionCount })
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		// Values (and encoded bytes) seen per partition, in the order the broker received them.
		const received = new Map<number, string[]>()
		const nextOffsets = new Map<number, bigint>()
		mockBroker.produce.mockImplementation(async (request: ProduceRequest) => {
			const partitions = []
			for (const { partitionIndex, records } of request.topics[0]!.partitions) {
				const batch = await decodeRecordBatch(records)
				// Only a single over-limit send may exceed the limit, and nothing is merged into it.
				if (records.length > maxBatchBytes) {
					const sendIds = new Set(batch.records.map(r => r.value!.toString().split('.')[0]))
					expect(sendIds.size).toBe(1)
				}
				const values = received.get(partitionIndex) ?? []
				for (const record of batch.records) values.push(record.value!.toString())
				received.set(partitionIndex, values)
				const baseOffset = nextOffsets.get(partitionIndex) ?? 0n
				nextOffsets.set(partitionIndex, baseOffset + BigInt(batch.records.length))
				partitions.push({ partitionIndex, errorCode: ErrorCode.None, baseOffset })
			}
			// Partition 0 is slow, so its sends back up while the others keep going.
			const slow = request.topics[0]!.partitions.some(p => p.partitionIndex === 0)
			await new Promise(resolve => setTimeout(resolve, slow ? 10 : 1))
			return buildProduceResponse([{ name: 'test-topic', partitions }])
		})
		const producer = new Producer(mockCluster, { maxBatchBytes, retries: 0 })
		const pad = 'x'.repeat(40)

		// Interleaved single sends, some multi-record sends (one batch each, one over the limit).
		const expected = new Map<number, string[]>()
		const sends: Array<Promise<unknown>> = []
		for (let i = 0; i < 300; i++) {
			const partition = i % partitionCount
			const count = i % 37 === 0 ? 20 : i % 11 === 0 ? 3 : 1
			const messages = Array.from({ length: count }, (_, j) => ({
				value: Buffer.from(`${i}.${j}:${pad}`),
				partition,
			}))
			const values = expected.get(partition) ?? []
			for (const message of messages) values.push(message.value.toString())
			expected.set(partition, values)
			sends.push(producer.send('test-topic', messages))
			// Let some sends go out on their own between bursts.
			if (i % 50 === 0) await new Promise(resolve => setTimeout(resolve, 2))
		}
		await Promise.all(sends)

		for (let partition = 0; partition < partitionCount; partition++) {
			expect(received.get(partition)).toEqual(expected.get(partition))
		}
		await producer.disconnect()
	})

	it('sends the remainder of a sequence-wrap split before the batches queued after it', async () => {
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
		mockBroker.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 9n,
			producerEpoch: 0,
		})
		const batches: Array<{ baseSequence: number; values: string[] }> = []
		mockBroker.produce.mockImplementation(async (request: ProduceRequest) => {
			const batch = await decodeRecordBatch(request.topics[0]!.partitions[0]!.records)
			batches.push({ baseSequence: batch.baseSequence, values: batch.records.map(r => r.value!.toString()) })
			await new Promise(resolve => setTimeout(resolve, 5))
			return buildProduceResponse([
				{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }] },
			])
		})
		// Room for four one-byte records per batch (the accumulator counts 51 bytes for each).
		const producer = new Producer(mockCluster, { idempotent: true, retries: 1, maxBatchBytes: 220 })
		await producer.send('test-topic', { value: Buffer.from('init'), partition: 0 })
		batches.length = 0
		;(producer as unknown as { sequences: Map<string, number> }).sequences.set('test-topic:0', 0x7fffffff - 1)

		const send = (value: string) => producer.send('test-topic', { value: Buffer.from(value), partition: 0 })
		// p, a, b, c are merged into one batch and d, e stay queued (no room). Only p and a fit below
		// the sequence wrap (MAX-1, MAX): b and c are split off and must still go before d and e.
		await Promise.all(['p', 'a', 'b', 'c', 'd', 'e'].map(send))

		expect(batches).toEqual([
			{ baseSequence: 0x7fffffff - 1, values: ['p', 'a'] },
			{ baseSequence: 0, values: ['b', 'c', 'd', 'e'] },
		])
		await producer.disconnect()
	})
})

describe('Producer: a send with nothing to wait for does not overtake one that waits', () => {
	const TOPIC = 'test-topic'
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>
	/** Values received by the broker, per partition, in order. */
	let received: Map<number, string[]>

	beforeEach(() => {
		mockCluster = createMockCluster({ partitionCount: 2 })
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		mockCluster.getCoordinator.mockResolvedValue(mockBroker)
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
		mockBroker.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		mockBroker.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
		received = new Map()
		mockBroker.produce.mockImplementation(async (request: ProduceRequest) => {
			const partitions = []
			for (const { partitionIndex, records } of request.topics[0]!.partitions) {
				const batch = await decodeRecordBatch(records)
				const values = received.get(partitionIndex) ?? []
				for (const record of batch.records) values.push(record.value!.toString())
				received.set(partitionIndex, values)
				partitions.push({ partitionIndex, errorCode: ErrorCode.None, baseOffset: 0n })
			}
			return buildProduceResponse([{ name: TOPIC, partitions }])
		})
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('waits behind a send whose partition count lookup is finishing', async () => {
		const producer = new Producer(mockCluster, { retries: 1 })
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const p = producer as any
		const sends: Array<Promise<unknown>> = []
		const send = (value: string) => {
			sends.push(producer.send(TOPIC, { value: Buffer.from(value), partition: 0 }))
		}
		// The moment the first lookup fills the cache, before the send that asked for it has
		// appended, issue another send: it finds the count cached, but must still go after.
		const cacheSet = p.partitionCache.set.bind(p.partitionCache)
		vi.spyOn(p.partitionCache, 'set').mockImplementationOnce((...args: unknown[]) => {
			cacheSet(...args)
			queueMicrotask(() => send('second'))
		})

		send('first')
		while (sends.length < 2) await new Promise(resolve => setTimeout(resolve, 1))
		await Promise.all(sends)
		send('third')
		await Promise.all(sends)

		expect(received.get(0)).toEqual(['first', 'second', 'third'])
		await producer.disconnect()
	})

	it('waits behind a transactional send whose other partition is being registered', async () => {
		mockBroker.addPartitionsToTxn.mockImplementation(async request => {
			const partitions = request.topics[0]!.partitions
			// Registering partition 0 is slow.
			if (partitions.includes(0)) await new Promise(resolve => setTimeout(resolve, 20))
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				results: [
					{
						name: TOPIC,
						resultsByPartition: partitions.map(partitionIndex => ({
							partitionIndex,
							errorCode: ErrorCode.None,
						})),
					},
				],
			}
		})
		const producer = new Producer(mockCluster, { retries: 1, transactionalId: 'tx-1' })

		await producer.transaction(async tx => {
			// Partition 1 is part of the transaction (and the partition count cached) after this.
			await tx.send(TOPIC, { value: Buffer.from('a'), partition: 1 })
			// Waits for partition 0 to be registered...
			const waiting = tx.send(TOPIC, [
				{ value: Buffer.from('b'), partition: 1 },
				{ value: Buffer.from('x'), partition: 0 },
			])
			// ...and this one, which needs nothing, must not get to partition 1 before it.
			const ready = tx.send(TOPIC, { value: Buffer.from('c'), partition: 1 })
			await Promise.all([waiting, ready])
			// Once nothing waits, sends append right away again.
			await tx.send(TOPIC, { value: Buffer.from('d'), partition: 1 })
		})

		expect(received.get(1)).toEqual(['a', 'b', 'c', 'd'])
		expect(received.get(0)).toEqual(['x'])
		expect(mockBroker.addPartitionsToTxn).toHaveBeenCalledTimes(2)
	})
})
