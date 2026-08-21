import { describe, expect, it } from 'vitest'

import { encodeOffsetFetchRequest, OFFSET_FETCH_VERSIONS } from '@/protocol/messages/requests/offset-fetch.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

describe('OffsetFetch protocol', () => {
	it('requires v7 so committed-offset reads cannot silently omit requireStable', () => {
		expect(OFFSET_FETCH_VERSIONS).toEqual({ min: 7, max: 7 })
	})

	it('encodes v7 requireStable without tagged fields on primitive partition indexes', () => {
		const encoder = new Encoder()

		encodeOffsetFetchRequest(encoder, 7, {
			groupId: 'g',
			topics: [{ name: 't', partitions: [{ partitionIndex: 2 }] }],
			requireStable: true,
		})

		expect(encoder.toBuffer().toString('hex')).toBe('02670202740200000002000100')
	})
})
