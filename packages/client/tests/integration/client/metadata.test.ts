import { describe, expect, it, vi } from 'vitest'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('KafkaClient (integration) - metadata', () => {
	it('connects and fetches metadata', async () => {
		const client = createClient('it-client')

		await client.connect()
		const metadata = await client.getMetadata()

		expect(metadata.brokers.size).toBeGreaterThan(0)
		expect(metadata.controllerId).toBeGreaterThanOrEqual(0)

		await client.disconnect()
	})

	it('tracks isConnected across connect/disconnect', async () => {
		const client = createClient('it-is-connected')

		expect(client.isConnected).toBe(false)
		await client.connect()
		expect(client.isConnected).toBe(true)
		await client.disconnect()
		expect(client.isConnected).toBe(false)
	})

	it('scopes metadata topics when requested', async () => {
		const client = createClient('it-metadata-scope')
		await client.connect()

		const topicA = uniqueName('it-meta-a')
		const topicB = uniqueName('it-meta-b')
		await client.createTopics([
			{ name: topicA, numPartitions: 1, replicationFactor: 1 },
			{ name: topicB, numPartitions: 1, replicationFactor: 1 },
		])

		await vi.waitFor(
			async () => {
				const meta = await client.getMetadata([topicA])
				expect(meta.topics.has(topicA)).toBe(true)
				expect(meta.topics.has(topicB)).toBe(false)
			},
			{ timeout: 30_000 }
		)

		await client.disconnect()
	})
})
