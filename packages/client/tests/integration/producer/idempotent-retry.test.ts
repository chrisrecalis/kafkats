import { describe, expect, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Decoder } from '@/protocol/primitives/index.js'
import { decodeRecordBatchFrom } from '@/protocol/records/index.js'
import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - idempotency', () => {
	it('prevents duplicates when a successful batch is retried', async () => {
		const client = createClient('it-idempotent-dedup')
		await client.connect()

		const topicName = uniqueName('it-idempotent-dedup')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		await vi.waitFor(
			async () => {
				const meta = await client.getMetadata([topicName])
				expect(meta.topics.get(topicName)?.partitions.size).toBe(1)
			},
			{ timeout: 30_000 }
		)

		const producer = client.producer({
			idempotent: true,
			lingerMs: 0,
			retries: 3,
			retryBackoffMs: 100,
			maxRetryBackoffMs: 250,
			maxInFlight: 1,
		})

		// Fault injection: throw after broker ack but before committing local sequence state.
		// This forces a retry of the exact same encoded batch (same baseSequence).
		const producerAny = producer as unknown as Record<string, unknown>
		const originalCommitSequence = producerAny['commitSequence'] as
			| ((topic: string, partition: number, recordCount: number) => void)
			| undefined

		expect(typeof originalCommitSequence).toBe('function')

		const boundCommitSequence = originalCommitSequence!.bind(producer)

		let injected = false
		producerAny['commitSequence'] = (topic: string, partition: number, recordCount: number) => {
			if (!injected) {
				injected = true
				throw new Error('injected commitSequence failure')
			}
			return boundCommitSequence(topic, partition, recordCount)
		}

		const first = await producer.send(testTopic, { value: 'first' })
		await producer.flush()

		const second = await producer.send(testTopic, { value: 'second' })
		await producer.flush()

		expect(first.partition).toBe(0)
		expect(second.partition).toBe(0)
		expect(first.offset).toBe(0n)
		expect(second.offset).toBe(1n)

		await vi.waitFor(
			async () => {
				const fetchRes = await client.fetch(topicName, 0, 0n)
				expect(fetchRes.errorCode).toBe(ErrorCode.None)
				expect(fetchRes.recordsData).not.toBeNull()

				const decoder = new Decoder(fetchRes.recordsData!)
				const values: string[] = []
				while (decoder.remaining() > 0) {
					const batch = await decodeRecordBatchFrom(decoder)
					for (const record of batch.records) {
						values.push(record.value?.toString('utf-8') ?? '')
					}
				}

				expect(values).toEqual(['first', 'second'])
			},
			{ timeout: 30_000 }
		)

		await producer.disconnect()
		await client.disconnect()
	})

	it('keeps at most one in-flight batch per partition (ordering, maxInFlight > 1)', async () => {
		const client = createClient('it-idempotent-inflight')
		await client.connect()

		const topicName = uniqueName('it-idempotent-inflight')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		await vi.waitFor(
			async () => {
				const meta = await client.getMetadata([topicName])
				expect(meta.topics.get(topicName)?.partitions.size).toBe(1)
			},
			{ timeout: 30_000 }
		)

		// maxInFlight > 1 allows multiple ProduceRequests concurrently; the idempotent
		// producer must still never have two in-flight batches for the SAME partition,
		// otherwise a retriable failure on the earlier batch reorders sequences and the
		// broker fences the producer with OutOfOrderSequenceNumber.
		const producer = client.producer({ idempotent: true, lingerMs: 0, maxInFlight: 5 })

		// Instrument the per-broker send to detect overlapping in-flight sends per
		// partition. The artificial delay widens the window so any second drain for the
		// same partition during an in-flight send is observed.
		const producerAny = producer as unknown as Record<string, unknown>
		const originalSend = (
			producerAny['doSendBrokerBatches'] as (
				broker: unknown,
				batches: Array<{ topic: string; partition: number }>
			) => Promise<void>
		).bind(producer)
		const active = new Set<string>()
		const overlaps: string[] = []
		producerAny['doSendBrokerBatches'] = async (
			broker: unknown,
			batches: Array<{ topic: string; partition: number }>
		): Promise<void> => {
			const keys = batches.map(b => `${b.topic}:${b.partition}`)
			for (const key of keys) {
				if (active.has(key)) overlaps.push(key)
				active.add(key)
			}
			try {
				await new Promise(resolve => setTimeout(resolve, 50))
				return await originalSend(broker, batches)
			} finally {
				for (const key of keys) active.delete(key)
			}
		}

		// 5 separate send() calls → 5 distinct batches for partition 0 (each send wraps
		// its append in its own synchronous begin/endAppendTransaction, so they do not
		// coalesce).
		const sends = Array.from({ length: 5 }, (_, i) => producer.send(testTopic, { value: `m-${i}`, partition: 0 }))
		const settled = await Promise.allSettled(sends)
		await producer.flush().catch(() => {})

		// Primary invariant: no partition ever has two overlapping in-flight sends.
		expect(overlaps).toEqual([])

		// All sends succeed (no OutOfOrderSequenceNumber fence) and produce offsets
		// 0..4 exactly once — no loss, no duplication. (Send-call order need not match
		// offset order here: the concurrent sends race through async producer init.)
		expect(settled.every(s => s.status === 'fulfilled')).toBe(true)
		const offsets = settled.flatMap(s => (s.status === 'fulfilled' ? [s.value.offset] : []))
		expect([...offsets].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))).toEqual([0n, 1n, 2n, 3n, 4n])

		await producer.disconnect()
		await client.disconnect()
	})

	it('flush() resolves while batches are queued behind an in-flight send (fire-and-forget)', async () => {
		const client = createClient('it-idempotent-flush')
		await client.connect()

		const topicName = uniqueName('it-idempotent-flush')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		await vi.waitFor(
			async () => {
				const meta = await client.getMetadata([topicName])
				expect(meta.topics.get(topicName)?.partitions.size).toBe(1)
			},
			{ timeout: 30_000 }
		)

		const producer = client.producer({ idempotent: true, lingerMs: 0, maxInFlight: 5 })

		// Warm up so the producer id is initialized and subsequent sends queue immediately.
		await producer.send(testTopic, { value: 'warmup', partition: 0 })
		await producer.flush()

		// Slow the network send so later batches pile up behind the first (muted) one.
		const producerAny = producer as unknown as Record<string, unknown>
		const originalSend = (
			producerAny['doSendBrokerBatches'] as (broker: unknown, batches: unknown) => Promise<void>
		).bind(producer)
		producerAny['doSendBrokerBatches'] = async (broker: unknown, batches: unknown): Promise<void> => {
			await new Promise(resolve => setTimeout(resolve, 200))
			return originalSend(broker, batches)
		}

		// Fire several same-partition sends without awaiting: the first goes in flight and
		// mutes partition 0; the rest are deferred in pendingBatches with drainScheduled
		// false, waiting on the in-flight send's completion.
		const sends = Array.from({ length: 4 }, (_, i) => producer.send(testTopic, { value: `m-${i}`, partition: 0 }))
		for (const s of sends) void s.catch(() => {})
		await new Promise(resolve => setTimeout(resolve, 50))

		// flush() must resolve. If its wait spins on microtasks it starves the event loop
		// so the in-flight send never settles and flush()/disconnect() hang forever.
		let hangTimer: ReturnType<typeof setTimeout> | undefined
		try {
			await Promise.race([
				producer.flush(),
				new Promise<never>((_, reject) => {
					hangTimer = setTimeout(
						() => reject(new Error('flush() did not resolve within 15s (event-loop starvation)')),
						15_000
					)
				}),
			])
		} finally {
			if (hangTimer) clearTimeout(hangTimer)
		}

		await Promise.allSettled(sends)
		await producer.disconnect()
		await client.disconnect()
	})
})
