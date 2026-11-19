import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { compressionCodecs } from '@/protocol/records/codec-registry.js'
import { CompressionType, gzipCodec, type CompressionCodec } from '@/protocol/records/compression.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'
import { buildProduceResponse, createMockBroker, createMockCluster } from './_helpers.js'

const TOPIC = 'test-topic'

function tick(ms = 10): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * An idempotent producer re-initializes while a drain is being compressed: the batches of one drain are encoded
 * under different epochs. Only the epoch of the batch whose answer failed decides what fails, and a batch whose
 * bytes carry the old epoch never goes out under the new one.
 */
describe('Producer: a drain compressed across a re-initialization', () => {
	let cluster: ReturnType<typeof createMockCluster>
	let broker: ReturnType<typeof createMockBroker>
	let original: CompressionCodec | undefined
	/** Produce requests in arrival order, each answered by `answer()` (per partition, default NONE). */
	let requests: Array<{ request: ProduceRequest; answer: (codes?: Record<number, ErrorCode>) => void }>
	/** Compressing records whose content includes `held` waits until `release()`. */
	let compression: { held?: string; waiting: boolean; release: () => void }

	beforeEach(() => {
		cluster = createMockCluster({ partitionCount: 4 })
		broker = createMockBroker(1)
		cluster.getLeaderForPartition.mockResolvedValue(broker)
		cluster.getAnyBroker.mockResolvedValue(broker)
		let epoch = 0
		broker.initProducerId.mockImplementation(async () => ({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: epoch++,
		}))
		requests = []
		broker.produce.mockImplementation(
			request =>
				new Promise(resolve => {
					requests.push({
						request,
						answer: (codes = {}) =>
							resolve(
								buildProduceResponse(
									request.topics.map(({ name, partitions }) => ({
										name,
										partitions: partitions.map(({ partitionIndex }) => ({
											partitionIndex,
											errorCode: codes[partitionIndex] ?? ErrorCode.None,
										})),
									}))
								)
							),
					})
				})
		)
		compression = { waiting: false, release: () => {} }
		original = compressionCodecs.get(CompressionType.Snappy)
		compressionCodecs.register(CompressionType.Snappy, {
			compress: async data => {
				if (compression.held && data.includes(compression.held)) {
					compression.waiting = true
					await new Promise<void>(resolve => (compression.release = resolve))
				}
				return gzipCodec.compress(data)
			},
			decompress: data => gzipCodec.decompress(data),
		})
	})

	afterEach(() => {
		if (original) compressionCodecs.register(CompressionType.Snappy, original)
		vi.restoreAllMocks()
	})

	/** Partition, epoch, base sequence and values of every batch produced. */
	async function produced(): Promise<string[]> {
		const batches = await Promise.all(
			requests.flatMap(({ request }) =>
				request.topics.flatMap(t =>
					t.partitions.map(async p => ({
						partition: p.partitionIndex,
						batch: await decodeRecordBatch(p.records),
					}))
				)
			)
		)
		return batches.map(
			({ partition, batch }) =>
				`p${partition} epoch ${batch.producerEpoch} seq ${batch.baseSequence} ${batch.records.map(r => String(r.value)).join(',')}`
		)
	}

	/**
	 * Epoch 0 fails (an out-of-order answer to partition 0) while a drain of partitions 1-3 is being compressed, and the
	 * producer initializes again (epoch 1) before P2's compression ends: P1 is encoded under epoch 0, P2's bytes too,
	 * P3 under epoch 1. Resolves once the drain was sent.
	 */
	async function drainAcrossReinitialization() {
		const producer = new Producer(cluster, {
			lingerMs: 0,
			idempotent: true,
			compression: 'snappy',
			retries: 3,
			retryBackoffMs: 1,
		})
		const warmUp = producer.send(TOPIC, { value: Buffer.from('W'), partition: 0 })
		await vi.waitFor(() => expect(requests).toHaveLength(1))
		requests[0]!.answer()
		await warmUp

		const failing = producer.send(TOPIC, { value: Buffer.from('R'), partition: 0 }).catch((e: unknown) => e)
		await vi.waitFor(() => expect(requests).toHaveLength(2))
		compression.held = 'P2'
		const sends = ['P1', 'P2', 'P3'].map((v, i) =>
			producer.send(TOPIC, { value: Buffer.from(v), partition: i + 1 }).then(
				result => result,
				(e: unknown) => e
			)
		)
		await vi.waitFor(() => expect(compression.waiting).toBe(true))
		requests[1]!.answer({ 0: ErrorCode.OutOfOrderSequenceNumber })
		const outOfOrder = await failing
		expect(outOfOrder).toMatchObject({ errorCode: ErrorCode.OutOfOrderSequenceNumber })
		await vi.waitFor(() => expect(broker.initProducerId).toHaveBeenCalledTimes(2))
		await tick()
		compression.release()
		await vi.waitFor(() => expect(requests).toHaveLength(3))
		return { producer, sends, outOfOrder }
	}

	it('a batch whose bytes carry the failed epoch is not sent, also when its compression ended after the new one began', async () => {
		const { producer, sends, outOfOrder } = await drainAcrossReinitialization()
		requests[2]!.answer()

		// P1 and P2 were rejected with the failure of their epoch: writing them would contradict that.
		expect(await sends[0]).toBe(outOfOrder)
		expect(await sends[1]).toBe(outOfOrder)
		expect(await sends[2]).toMatchObject({ partition: 3 })
		expect(await produced()).toEqual(['p0 epoch 0 seq 0 W', 'p0 epoch 0 seq 1 R', 'p3 epoch 1 seq 0 P3'])

		const next = producer.send(TOPIC, { value: Buffer.from('N'), partition: 2 })
		await vi.waitFor(() => expect(requests).toHaveLength(4))
		requests[3]!.answer()
		await next
		expect((await produced()).at(-1)).toBe('p2 epoch 1 seq 0 N')
		await producer.disconnect({ timeoutMs: 100 })
	})

	it('a failed answer to a batch of the new epoch fails it, also when its drain held batches of the old one', async () => {
		const { producer, sends } = await drainAcrossReinitialization()
		requests[2]!.answer({ 3: ErrorCode.OutOfOrderSequenceNumber })

		expect(await sends[2]).toMatchObject({ errorCode: ErrorCode.OutOfOrderSequenceNumber })
		await vi.waitFor(() => expect(broker.initProducerId).toHaveBeenCalledTimes(3))
		await tick()
		// The next epoch starts its sequences at 0; one more under epoch 1 would follow a gap.
		const next = producer.send(TOPIC, { value: Buffer.from('N'), partition: 3 })
		await vi.waitFor(() => expect(requests).toHaveLength(4))
		requests[3]!.answer()
		await next
		expect((await produced()).at(-1)).toBe('p3 epoch 2 seq 0 N')
		await producer.disconnect({ timeoutMs: 100 })
	})
})
