import { describe, expect, it } from 'vitest'

import {
	assignmentToTopicPartitions,
	decodeAssignment,
	decodeSubscriptionMetadata,
	encodeAssignment,
	encodeSubscriptionMetadata,
} from '@/consumer/assignors/consumer-protocol.js'

describe('consumer protocol encoding', () => {
	it('encodes and decodes subscription metadata v0', () => {
		const buffer = encodeSubscriptionMetadata({
			version: 0,
			topics: ['a', 'b'],
			userData: null,
		})
		const decoded = decodeSubscriptionMetadata(buffer)
		expect(decoded.version).toBe(0)
		expect(decoded.topics).toEqual(['a', 'b'])
		expect(decoded.userData).toBeNull()
		expect(decoded.ownedPartitions).toBeUndefined()
	})

	it('encodes and decodes subscription metadata with owned partitions', () => {
		const buffer = encodeSubscriptionMetadata({
			version: 1,
			topics: ['a'],
			userData: Buffer.from('x'),
			ownedPartitions: [{ topic: 'a', partitions: [0, 2] }],
		})
		const decoded = decodeSubscriptionMetadata(buffer)
		expect(decoded.version).toBe(1)
		expect(decoded.userData?.toString()).toBe('x')
		expect(decoded.ownedPartitions).toEqual([{ topic: 'a', partitions: [0, 2] }])
	})

	it('decodes subscription metadata without owned partitions', () => {
		const buffer = encodeSubscriptionMetadata({
			version: 1,
			topics: ['a'],
			userData: null,
		})
		const decoded = decodeSubscriptionMetadata(buffer)
		expect(decoded.ownedPartitions).toBeUndefined()
	})

	it('encodes and decodes member assignment', () => {
		const buffer = encodeAssignment({
			version: 2,
			partitions: [
				{ topic: 'a', partitions: [0, 1] },
				{ topic: 'b', partitions: [2] },
			],
			userData: Buffer.from('y'),
		})
		const decoded = decodeAssignment(buffer)
		expect(decoded.version).toBe(2)
		expect(decoded.partitions).toEqual([
			{ topic: 'a', partitions: [0, 1] },
			{ topic: 'b', partitions: [2] },
		])
		expect(decoded.userData?.toString()).toBe('y')
	})

	it('handles empty assignments', () => {
		const decoded = decodeAssignment(Buffer.alloc(0))
		expect(decoded.version).toBe(0)
		expect(decoded.partitions).toEqual([])
		expect(decoded.userData).toBeNull()
	})

	it('flattens assignments to topic partitions', () => {
		const flat = assignmentToTopicPartitions({
			version: 0,
			partitions: [{ topic: 'a', partitions: [0, 2] }],
			userData: null,
		})
		expect(flat).toEqual([
			{ topic: 'a', partition: 0 },
			{ topic: 'a', partition: 2 },
		])
	})
})
