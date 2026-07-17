import { describe, expect, it } from 'vitest'

import { SaslAuthenticator } from '@/auth/sasl-authenticator.js'
import { UnsupportedSaslMechanismError, IllegalSaslStateError, SaslAuthenticationError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

const clientId = 'unit-test-client'

function buildHandshakeResponse(enabledMechanisms: string[], errorCode: ErrorCode = ErrorCode.None): Buffer {
	const encoder = new Encoder()
	encoder.writeInt32(0)
	encoder.writeInt16(errorCode)
	encoder.writeInt32(enabledMechanisms.length)
	for (const mechanism of enabledMechanisms) {
		encoder.writeString(mechanism)
	}
	return encoder.toBuffer()
}

function buildAuthenticateResponse(options: {
	errorCode: ErrorCode
	errorMessage?: string | null
	authBytes?: Buffer
}): Buffer {
	const encoder = new Encoder()
	encoder.writeInt32(0)
	encoder.writeInt16(options.errorCode)
	encoder.writeNullableString(options.errorMessage ?? null)
	encoder.writeBytes(options.authBytes ?? Buffer.alloc(0))
	encoder.writeInt64(BigInt(0))
	return encoder.toBuffer()
}

describe('SaslAuthenticator error mapping', () => {
	it('surfaces supported mechanisms when auth rejects mechanism', async () => {
		const responses = [
			buildHandshakeResponse(['PLAIN', 'SCRAM-SHA-256']),
			buildAuthenticateResponse({
				errorCode: ErrorCode.UnsupportedSaslMechanism,
				errorMessage: 'Unsupported mechanism',
			}),
		]

		const authenticator = new SaslAuthenticator({
			config: {
				mechanism: 'PLAIN',
				username: 'user',
				password: 'pass',
			},
			clientId,
			brokerHost: 'localhost',
			brokerPort: 9092,
			sendRaw: async () => {
				const next = responses.shift()
				if (!next) {
					throw new Error('Unexpected sendRaw call')
				}
				return next
			},
		})

		try {
			await authenticator.authenticate()
			expect.fail('Expected UnsupportedSaslMechanismError')
		} catch (error) {
			expect(error).toBeInstanceOf(UnsupportedSaslMechanismError)
			const typed = error as UnsupportedSaslMechanismError
			expect(typed.supportedMechanisms).toEqual(['PLAIN', 'SCRAM-SHA-256'])
		}
	})

	it('maps IllegalSaslState to IllegalSaslStateError', async () => {
		const responses = [
			buildHandshakeResponse(['PLAIN']),
			buildAuthenticateResponse({
				errorCode: ErrorCode.IllegalSaslState,
				errorMessage: 'Illegal state',
			}),
		]

		const authenticator = new SaslAuthenticator({
			config: {
				mechanism: 'PLAIN',
				username: 'user',
				password: 'pass',
			},
			clientId,
			brokerHost: 'localhost',
			brokerPort: 9092,
			sendRaw: async () => {
				const next = responses.shift()
				if (!next) {
					throw new Error('Unexpected sendRaw call')
				}
				return next
			},
		})

		await expect(authenticator.authenticate()).rejects.toBeInstanceOf(IllegalSaslStateError)
	})

	it('rejects OAUTHBEARER authentication when the broker sends a failure challenge', async () => {
		// Per KIP-255/RFC 7628 an invalid token is NOT reported via errorCode on the
		// first response: the broker sends its JSON error document as a challenge with
		// errorCode=NONE. The client must answer with 0x01 and fail — not treat the
		// exchange as successful.
		const errorJson = '{"status":"invalid_token"}'
		const responses = [
			buildHandshakeResponse(['OAUTHBEARER']),
			buildAuthenticateResponse({
				errorCode: ErrorCode.None,
				authBytes: Buffer.from(errorJson, 'utf8'),
			}),
			// Broker fails the exchange after the client's 0x01 abort byte.
			buildAuthenticateResponse({
				errorCode: ErrorCode.SaslAuthenticationFailed,
				errorMessage: 'Authentication failed',
			}),
		]

		const sentFrames: Buffer[] = []
		const authenticator = new SaslAuthenticator({
			config: {
				mechanism: 'OAUTHBEARER',
				oauthBearerProvider: async () => ({ value: 'expired-token' }),
			},
			clientId,
			brokerHost: 'localhost',
			brokerPort: 9092,
			sendRaw: async request => {
				sentFrames.push(request)
				const next = responses.shift()
				if (!next) {
					throw new Error('Unexpected sendRaw call')
				}
				return next
			},
		})

		await expect(authenticator.authenticate()).rejects.toBeInstanceOf(SaslAuthenticationError)

		// handshake + initial token + 0x01 abort byte
		expect(sentFrames).toHaveLength(3)
	})
})
