import { describe, expect, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Decoder } from '@/protocol/primitives/index.js'
import { decodeRecordBatchFrom } from '@/protocol/records/index.js'
import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - idempotency', () => {
	it('prevents duplicates when a successful batch is retried', async () => {
		await withKafka(async ({ createClient }) => {
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
	})
})
