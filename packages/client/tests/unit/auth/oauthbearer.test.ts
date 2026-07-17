import { describe, expect, it } from 'vitest'

import { OAuthBearerMechanism } from '@/auth/oauthbearer.js'
import { SaslAuthenticationError } from '@/client/errors.js'

describe('OAuthBearerMechanism', () => {
	it('builds the correct client-first message and passes provider context', async () => {
		const mechanism = new OAuthBearerMechanism({
			context: { host: 'b-1.example', port: 9098, clientId: 'cid' },
			provider: async context => {
				expect(context).toEqual({ host: 'b-1.example', port: 9098, clientId: 'cid' })
				return {
					value: 'token123',
					extensions: { 'x-custom': 'abc' },
				}
			},
		})

		const gen = mechanism.authenticate()
		const first = await gen.next()
		if (first.done) {
			throw new Error('expected initial client response')
		}
		expect(first.value.toString('utf8')).toBe('n,,\u0001auth=Bearer token123\u0001x-custom=abc\u0001\u0001')

		const done = await gen.next(Buffer.alloc(0))
		expect(done.done).toBe(true)
	})

	it('treats a non-empty server response as a failure challenge (KIP-255 / RFC 7628)', async () => {
		const mechanism = new OAuthBearerMechanism({
			context: { host: 'b-1.example', port: 9098, clientId: 'cid' },
			provider: async () => ({ value: 'bad-token' }),
		})

		const gen = mechanism.authenticate()
		const first = await gen.next()
		expect(first.done).toBe(false)

		// The broker rejects the token by sending its JSON error document as a
		// SASL challenge (with errorCode=NONE). The client must answer with a
		// single 0x01 byte, then fail the authentication.
		const errorJson = '{"status":"invalid_token","scope":"kafka"}'
		const second = await gen.next(Buffer.from(errorJson, 'utf8'))
		expect(second.done).toBe(false)
		if (second.done) {
			throw new Error('expected 0x01 abort response')
		}
		expect(second.value).toEqual(Buffer.from([0x01]))

		await expect(gen.next(Buffer.alloc(0))).rejects.toBeInstanceOf(SaslAuthenticationError)
	})
})
