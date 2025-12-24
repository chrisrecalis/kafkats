import { describe, expect, it } from 'vitest'

import { withKafkaSasl } from '../helpers/kafka-sasl.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('SASL PLAIN authentication (integration)', () => {
	it('authenticates successfully with valid credentials', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ createClient }) => {
			const client = createClient('it-sasl-plain-success')

			await expect(client.connect()).resolves.not.toThrow()
			expect(client.isConnected).toBe(true)

			// Verify we can perform operations after authentication
			const metadata = await client.getMetadata()
			expect(metadata.brokers.size).toBeGreaterThan(0)

			await client.disconnect()
		})
	})

	it('can create topics after authentication', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ createClient }) => {
			const client = createClient('it-sasl-plain-topics')
			await client.connect()

			const topicName = uniqueName('sasl-plain-topic')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Verify topic was created by fetching metadata
			const metadata = await client.getMetadata([topicName])
			expect(metadata.topics.has(topicName)).toBe(true)

			await client.disconnect()
		})
	})

	it('supports multiple concurrent connections with SASL', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ createClient }) => {
			const client1 = createClient('it-sasl-plain-concurrent-1')
			const client2 = createClient('it-sasl-plain-concurrent-2')

			// Connect both clients concurrently
			await Promise.all([client1.connect(), client2.connect()])

			expect(client1.isConnected).toBe(true)
			expect(client2.isConnected).toBe(true)

			// Both should be able to fetch metadata
			const [metadata1, metadata2] = await Promise.all([client1.getMetadata(), client2.getMetadata()])

			expect(metadata1.brokers.size).toBeGreaterThan(0)
			expect(metadata2.brokers.size).toBeGreaterThan(0)

			await Promise.all([client1.disconnect(), client2.disconnect()])
		})
	})

	it('can reconnect after disconnection', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ createClient }) => {
			const client = createClient('it-sasl-plain-reconnect')

			// First connection
			await client.connect()
			expect(client.isConnected).toBe(true)

			const metadata1 = await client.getMetadata()
			expect(metadata1.brokers.size).toBeGreaterThan(0)

			// Disconnect
			await client.disconnect()
			expect(client.isConnected).toBe(false)

			// Reconnect
			await client.connect()
			expect(client.isConnected).toBe(true)

			const metadata2 = await client.getMetadata()
			expect(metadata2.brokers.size).toBeGreaterThan(0)

			await client.disconnect()
		})
	})
})
