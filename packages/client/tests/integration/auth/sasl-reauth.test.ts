import { describe, expect, it } from 'vitest'

import { Connection } from '@/network/connection.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { encodeApiVersionsRequest } from '@/protocol/messages/requests/api-versions.js'
import { withKafkaSasl } from '../helpers/kafka-sasl.js'

/**
 * KIP-368 SASL re-authentication (integration)
 *
 * The broker is configured with connections.max.reauth.ms=10000, so every SASL
 * session expires after at most 10s and the broker kills connections that keep
 * issuing requests without re-authenticating. The client schedules
 * re-authentication 5s before expiry.
 *
 * Per KIP-368 the re-authentication must start with a SaslHandshake on the
 * already-authenticated channel; a SaslAuthenticate sent directly is answered
 * with ILLEGAL_SASL_STATE and the connection is destroyed on every re-auth.
 * This test drives a raw Connection well past the original session lifetime and
 * asserts it stays healthy with no errors or disconnects.
 */
describe('SASL re-authentication (KIP-368) (integration)', () => {
	it('keeps a SCRAM connection healthy across broker-enforced session expiry', async () => {
		await withKafkaSasl(
			{ mechanism: 'SCRAM-SHA-256', brokerEnv: { KAFKA_CONNECTIONS_MAX_REAUTH_MS: '10000' } },
			async ({ brokerAddress }) => {
				const lastColonIndex = brokerAddress.lastIndexOf(':')
				const host = brokerAddress.slice(0, lastColonIndex)
				const port = parseInt(brokerAddress.slice(lastColonIndex + 1), 10)

				const errors: Error[] = []
				const disconnects: Array<Error | undefined> = []

				const connection = new Connection({
					host,
					port,
					clientId: 'it-sasl-reauth',
					sasl: {
						mechanism: 'SCRAM-SHA-256',
						username: 'testuser',
						password: 'testpass',
						reauthenticationThresholdMs: 5000,
					},
				})
				connection.on('error', error => {
					errors.push(error)
				})
				connection.on('disconnect', error => {
					disconnects.push(error)
				})

				await connection.connect()
				expect(connection.isConnected).toBe(true)

				const sendApiVersions = () =>
					connection.send(ApiKey.ApiVersions, 0, encoder => {
						encodeApiVersionsRequest(encoder, 0, {})
					})

				await expect(sendApiVersions()).resolves.toBeInstanceOf(Buffer)

				// Keep the connection busy well past the original ~10s session
				// lifetime so at least one full re-authentication cycle runs.
				for (let i = 0; i < 15; i++) {
					await new Promise(resolve => setTimeout(resolve, 1000))
					await expect(sendApiVersions()).resolves.toBeInstanceOf(Buffer)
				}

				expect(errors).toEqual([])
				expect(disconnects).toEqual([])
				expect(connection.isConnected).toBe(true)

				await connection.close(false)
			}
		)
	}, 300_000)
})
