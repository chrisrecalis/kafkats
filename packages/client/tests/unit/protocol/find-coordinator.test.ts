import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

import { encodeFindCoordinatorRequest } from '@/protocol/messages/requests/find-coordinator.js'

describe('FindCoordinator request codec', () => {
	it('encodes FindCoordinatorRequest v4 (flexible coordinatorKeys as compact array of structs)', () => {
		const enc = new Encoder()
		encodeFindCoordinatorRequest(enc, 4, {
			key: 'g1',
			keyType: 0,
			coordinatorKeys: ['g1', 'g2'],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(0) // keyType
		expect(
			dec.readCompactArray(d => {
				const key = d.readCompactString()
				d.skipTaggedFields()
				return key
			})
		).toEqual(['g1', 'g2'])
		expect(dec.readUVarInt()).toBe(0) // tagged fields
		expect(dec.remaining()).toBe(0)
	})

	it('encodes FindCoordinatorRequest v6 (flexible share keyType)', () => {
		const enc = new Encoder()
		encodeFindCoordinatorRequest(enc, 6, {
			key: 'share-group',
			keyType: 2,
			coordinatorKeys: ['share-group:00000000-0000-0000-0000-000000000001:0'],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(2) // keyType = SHARE
		expect(
			dec.readCompactArray(d => {
				const key = d.readCompactString()
				d.skipTaggedFields()
				return key
			})
		).toEqual(['share-group:00000000-0000-0000-0000-000000000001:0'])
		expect(dec.readUVarInt()).toBe(0) // tagged fields
		expect(dec.remaining()).toBe(0)
	})
})
