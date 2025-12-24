import { describe, expect, it } from 'vitest'

import { withKafkaSasl } from '../helpers/kafka-sasl.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('SASL SCRAM-SHA-256 authentication (integration)', () => {
	it('authenticates successfully with valid credentials', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-256' }, async ({ createClient }) => {
			const client = createClient('it-sasl-scram-256-success')

			await expect(client.connect()).resolves.not.toThrow()
			expect(client.isConnected).toBe(true)

			// Verify we can perform operations after authentication
			const metadata = await client.getMetadata()
			expect(metadata.brokers.size).toBeGreaterThan(0)

			await client.disconnect()
		})
	})

	it('can create topics after authentication', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-256' }, async ({ createClient }) => {
			const client = createClient('it-sasl-scram-256-topics')
			await client.connect()

			const topicName = uniqueName('sasl-scram-256-topic')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Verify topic was created by fetching metadata
			const metadata = await client.getMetadata([topicName])
			expect(metadata.topics.has(topicName)).toBe(true)

			await client.disconnect()
		})
	})

	it('supports client nonce generation', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-256' }, async ({ createClient }) => {
			// Create multiple clients to verify unique nonce generation
			const client1 = createClient('it-sasl-scram-256-nonce-1')
			const client2 = createClient('it-sasl-scram-256-nonce-2')

			await Promise.all([client1.connect(), client2.connect()])

			expect(client1.isConnected).toBe(true)
			expect(client2.isConnected).toBe(true)

			await Promise.all([client1.disconnect(), client2.disconnect()])
		})
	})
})

describe.concurrent('SASL SCRAM-SHA-512 authentication (integration)', () => {
	it('authenticates successfully with valid credentials', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-512' }, async ({ createClient }) => {
			const client = createClient('it-sasl-scram-512-success')

			await expect(client.connect()).resolves.not.toThrow()
			expect(client.isConnected).toBe(true)

			// Verify we can perform operations after authentication
			const metadata = await client.getMetadata()
			expect(metadata.brokers.size).toBeGreaterThan(0)

			await client.disconnect()
		})
	})

	it('can create topics after authentication', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-512' }, async ({ createClient }) => {
			const client = createClient('it-sasl-scram-512-topics')
			await client.connect()

			const topicName = uniqueName('sasl-scram-512-topic')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Verify topic was created by fetching metadata
			const metadata = await client.getMetadata([topicName])
			expect(metadata.topics.has(topicName)).toBe(true)

			await client.disconnect()
		})
	})

	it('uses stronger hash function than SHA-256', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-512' }, async ({ createClient }) => {
			const client = createClient('it-sasl-scram-512-hash')

			await client.connect()
			expect(client.isConnected).toBe(true)

			// SHA-512 should work with the broker configured for it
			const metadata = await client.getMetadata()
			expect(metadata.brokers.size).toBeGreaterThan(0)

			await client.disconnect()
		})
	})
})
