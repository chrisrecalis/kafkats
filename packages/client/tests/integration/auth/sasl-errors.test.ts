import { describe, expect, it } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import { SaslAuthenticationError } from '@/client/errors.js'
import { withKafkaSasl } from '../helpers/kafka-sasl.js'

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
