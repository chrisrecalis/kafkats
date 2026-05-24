import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

import { encodeFindCoordinatorRequest } from '@/protocol/messages/requests/find-coordinator.js'

// In the Kafka FindCoordinator request schema, CoordinatorKeys (v4+) is `[]string` — a compact
// array of primitive strings. A primitive array has NO per-element tagged fields (those exist only
// for arrays of sub-structs), so each element is just a compact string with nothing after it.
function readCoordinatorKeys(dec: Decoder): string[] {
	return dec.readCompactArray(d => d.readCompactString())
}

describe('FindCoordinator request codec', () => {
	it('encodes FindCoordinatorRequest v4 with coordinatorKeys as a compact array of strings', () => {
		const enc = new Encoder()
		encodeFindCoordinatorRequest(enc, 4, {
			key: 'g1',
			keyType: 0,
			coordinatorKeys: ['g1', 'g2'],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(0) // keyType
		expect(readCoordinatorKeys(dec)).toEqual(['g1', 'g2'])
		expect(dec.readUVarInt()).toBe(0) // request-level tagged fields
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
		expect(readCoordinatorKeys(dec)).toEqual(['share-group:00000000-0000-0000-0000-000000000001:0'])
		expect(dec.readUVarInt()).toBe(0) // request-level tagged fields
		expect(dec.remaining()).toBe(0)
	})

	it('falls back to the single key when coordinatorKeys is omitted', () => {
		const enc = new Encoder()
		encodeFindCoordinatorRequest(enc, 4, { key: 'only', keyType: 1 })

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(1)
		expect(readCoordinatorKeys(dec)).toEqual(['only'])
		expect(dec.readUVarInt()).toBe(0)
		expect(dec.remaining()).toBe(0)
	})
})
