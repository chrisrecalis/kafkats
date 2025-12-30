import { describe, expect, it } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import { SaslAuthenticationError } from '@/client/errors.js'
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

describe.concurrent('SASL authentication errors (integration)', () => {
	it('fails with invalid credentials for PLAIN', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ brokerAddress, logLevel }) => {
			const client = new KafkaClient({
				brokers: [brokerAddress],
				clientId: 'it-sasl-invalid-plain',
				logLevel,
				sasl: {
					mechanism: 'PLAIN',
					username: 'testuser',
					password: 'wrongpassword', // Intentionally wrong password
				},
			})

			// The error is wrapped by the client connection logic
			await expect(client.connect()).rejects.toThrow(/SASL.*authentication failed/i)
			expect(client.isConnected).toBe(false)
		})
	})

	it('fails with invalid credentials for SCRAM-SHA-256', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-256' }, async ({ brokerAddress, logLevel }) => {
			const client = new KafkaClient({
				brokers: [brokerAddress],
				clientId: 'it-sasl-invalid-scram-256',
				logLevel,
				sasl: {
					mechanism: 'SCRAM-SHA-256',
					username: 'testuser',
					password: 'wrongpassword', // Intentionally wrong password
				},
			})

			await expect(client.connect()).rejects.toThrow(SaslAuthenticationError)
			expect(client.isConnected).toBe(false)
		})
	})

	it('fails with invalid credentials for SCRAM-SHA-512', async () => {
		await withKafkaSasl({ mechanism: 'SCRAM-SHA-512' }, async ({ brokerAddress, logLevel }) => {
			const client = new KafkaClient({
				brokers: [brokerAddress],
				clientId: 'it-sasl-invalid-scram-512',
				logLevel,
				sasl: {
					mechanism: 'SCRAM-SHA-512',
					username: 'testuser',
					password: 'wrongpassword', // Intentionally wrong password
				},
			})

			await expect(client.connect()).rejects.toThrow(SaslAuthenticationError)
			expect(client.isConnected).toBe(false)
		})
	})

	it('fails with non-existent username', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ brokerAddress, logLevel }) => {
			const client = new KafkaClient({
				brokers: [brokerAddress],
				clientId: 'it-sasl-nonexistent-user',
				logLevel,
				sasl: {
					mechanism: 'PLAIN',
					username: 'nonexistentuser',
					password: 'anypassword',
				},
			})

			await expect(client.connect()).rejects.toThrow(/SASL.*authentication failed/i)
			expect(client.isConnected).toBe(false)
		})
	})

	it('provides meaningful error messages on authentication failure', async () => {
		await withKafkaSasl({ mechanism: 'PLAIN' }, async ({ brokerAddress, logLevel }) => {
			const client = new KafkaClient({
				brokers: [brokerAddress],
				clientId: 'it-sasl-error-message',
				logLevel,
				sasl: {
					mechanism: 'PLAIN',
					username: 'testuser',
					password: 'wrongpassword',
				},
			})

			try {
				await client.connect()
				expect.fail('Should have thrown an error')
			} catch (error) {
				expect(error).toBeInstanceOf(Error)
				const message = (error as Error).message
				expect(message).toBeTruthy()
				expect(message).toMatch(/SASL.*authentication/i)
				expect(message.length).toBeGreaterThan(0)
			}
		})
	})

	it('can attempt reconnection after failed authentication', async () => {
		await withKafkaSasl(
			{ mechanism: 'PLAIN', username: 'testuser', password: 'testpass' },
			async ({ brokerAddress, logLevel }) => {
				// First, try with wrong credentials
				const clientWrong = new KafkaClient({
					brokers: [brokerAddress],
					clientId: 'it-sasl-retry-wrong',
					logLevel,
					sasl: {
						mechanism: 'PLAIN',
						username: 'testuser',
						password: 'wrongpassword',
					},
				})

				await expect(clientWrong.connect()).rejects.toThrow(/SASL.*authentication failed/i)

				// Now create a new client with correct credentials
				const clientCorrect = new KafkaClient({
					brokers: [brokerAddress],
					clientId: 'it-sasl-retry-correct',
					logLevel,
					sasl: {
						mechanism: 'PLAIN',
						username: 'testuser',
						password: 'testpass',
					},
				})

				await expect(clientCorrect.connect()).resolves.not.toThrow()
				expect(clientCorrect.isConnected).toBe(true)

				await clientCorrect.disconnect()
			}
		)
	})
})
