import { describe, expect, it } from 'vitest'

import { SaslAuthenticator } from '@/auth/sasl-authenticator.js'
import { UnsupportedSaslMechanismError, IllegalSaslStateError } from '@/client/errors.js'
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
})
