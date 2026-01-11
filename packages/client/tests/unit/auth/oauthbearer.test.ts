import { describe, expect, it } from 'vitest'

import { OAuthBearerMechanism } from '@/auth/oauthbearer.js'

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
})
