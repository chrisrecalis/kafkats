import { describe, expect, it } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Decoder } from '@/protocol/primitives/index.js'
import { createRecordBatch, decodeRecordBatchFrom, encodeRecordBatch } from '@/protocol/records/index.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('KafkaClient (integration) - low-level', () => {
	it('produces and fetches raw record batches', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-low-level')
			await client.connect()

			const topicName = uniqueName('it-low-level')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const now = Date.now()
			const recordBatch = createRecordBatch(
				[
					{
						key: 'k',
						value: 'hello',
						headers: { h: 'v' },
						timestamp: now,
					},
				],
				0n,
				BigInt(now)
			)

			const encoded = await encodeRecordBatch(recordBatch)
			const produceRes = await client.produce(topicName, 0, encoded)
			expect(produceRes.errorCode).toBe(ErrorCode.None)

			const fetchRes = await client.fetch(topicName, 0, 0n)
			expect(fetchRes.errorCode).toBe(ErrorCode.None)
			expect(fetchRes.recordsData).not.toBeNull()

			const decoder = new Decoder(fetchRes.recordsData!)
			const batches = []
			while (decoder.remaining() > 0) {
				batches.push(await decodeRecordBatchFrom(decoder))
			}

			expect(batches).toHaveLength(1)
			expect(batches[0]!.records).toHaveLength(1)

			const record = batches[0]!.records[0]!
			expect(record.key?.toString('utf-8')).toBe('k')
			expect(record.value?.toString('utf-8')).toBe('hello')
			expect(record.headers[0]?.key).toBe('h')
			expect(record.headers[0]?.value?.toString('utf-8')).toBe('v')

			await client.disconnect()
		})
	})
})
