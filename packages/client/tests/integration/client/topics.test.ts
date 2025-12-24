import { describe, expect, it, vi } from 'vitest'

import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('KafkaClient (integration) - topics', () => {
	it('creates topics idempotently', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-create-topics')

			await client.connect()

			const topicName = uniqueName('it-topic')

			await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])
			await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

			await vi.waitFor(
				async () => {
					const meta = await client.getMetadata([topicName])
					const created = meta.topics.get(topicName)
					expect(created?.partitions.size).toBe(3)
				},
				{ timeout: 30_000 }
			)

			await client.disconnect()
		})
	})

	it('validateOnly does not create topics', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-create-topics-validate-only')
			await client.connect()

			const topicName = uniqueName('it-validate-only')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }], {
				validateOnly: true,
			})

			const meta = await client.getMetadata([topicName])
			expect(meta.topics.has(topicName)).toBe(false)

			await client.disconnect()
		})
	})

	it('throws when replicationFactor is impossible for the cluster', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-create-topics-invalid-rf')
			await client.connect()

			const topicName = uniqueName('it-invalid-rf')

			await expect(
				client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 2 }])
			).rejects.toBeInstanceOf(KafkaProtocolError)

			await expect(
				client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 2 }])
			).rejects.toMatchObject({
				errorCode: ErrorCode.InvalidReplicationFactor,
			})

			await client.disconnect()
		})
	})
})
