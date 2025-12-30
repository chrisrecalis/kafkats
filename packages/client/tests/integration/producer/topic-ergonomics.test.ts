import { describe, expect, expectTypeOf, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Decoder } from '@/protocol/primitives/index.js'
import { decodeRecordBatchFrom, isControlBatch } from '@/protocol/records/index.js'
import type { KafkaClient } from '@/client/index.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

async function fetchUtf8Values(
	client: KafkaClient,
	topicName: string,
	options?: { isolationLevel?: number }
): Promise<string[]> {
	const res = await client.fetch(topicName, 0, 0n, { isolationLevel: options?.isolationLevel })
	expect(res.errorCode).toBe(ErrorCode.None)
	if (!res.recordsData) {
		return []
	}

	const decoder = new Decoder(res.recordsData)
	const values: string[] = []
	while (decoder.remaining() > 0) {
		const batch = await decodeRecordBatchFrom(decoder)
		// Skip control batches (transaction commit/abort markers)
		if (isControlBatch(batch.attributes)) {
			continue
		}
		for (const record of batch.records) {
			values.push(record.value?.toString('utf-8') ?? '')
		}
	}
	return values
}

describe.concurrent('Producer (integration) - topic ergonomics', () => {
	it('send() accepts a string topic and defaults to Buffer values', async () => {
		const client = createClient('it-producer-string-topic')
		await client.connect()

		const topicName = uniqueName('it-producer-string-topic')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })

		const result = await producer.send(topicName, { value: Buffer.from('hello', 'utf-8') })
		expectTypeOf(result).toEqualTypeOf<{ topic: string; partition: number; offset: bigint; timestamp: Date }>()
		expect(result.topic).toBe(topicName)
		expect(result.partition).toBe(0)

		await producer.flush()

		await vi.waitFor(
			async () => {
				expect(await fetchUtf8Values(client, topicName)).toEqual(['hello'])
			},
			{ timeout: 30_000 }
		)

		await producer.disconnect()
		await client.disconnect()
	})

	it('send() accepts string topics for batch sends', async () => {
		const client = createClient('it-producer-string-topic-batch')
		await client.connect()

		const topicName = uniqueName('it-producer-string-topic-batch')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })

		const results = await producer.send(topicName, [
			{ value: Buffer.from('a', 'utf-8') },
			{ value: Buffer.from('b', 'utf-8') },
			{ value: Buffer.from('c', 'utf-8') },
		])
		expect(Array.isArray(results)).toBe(true)
		expect(results.map(r => r.offset)).toEqual([0n, 1n, 2n])

		await producer.flush()

		await vi.waitFor(
			async () => {
				expect(await fetchUtf8Values(client, topicName)).toEqual(['a', 'b', 'c'])
			},
			{ timeout: 30_000 }
		)

		await producer.disconnect()
		await client.disconnect()
	})

	it('tx.send() accepts a string topic and defaults to Buffer values', async () => {
		const client = createClient('it-producer-tx-string-topic')
		await client.connect()

		const topicName = uniqueName('it-producer-tx-string-topic')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({
			transactionalId: uniqueName('tx'),
			lingerMs: 0,
			retries: 10,
			retryBackoffMs: 250,
			maxRetryBackoffMs: 1000,
		})

		await producer.transaction(async tx => {
			await tx.send(topicName, { value: Buffer.from('committed', 'utf-8') })
		})

		await vi.waitFor(
			async () => {
				expect(await fetchUtf8Values(client, topicName, { isolationLevel: 1 })).toEqual(['committed'])
			},
			{ timeout: 30_000 }
		)

		await producer.disconnect()
		await client.disconnect()
	})
})
