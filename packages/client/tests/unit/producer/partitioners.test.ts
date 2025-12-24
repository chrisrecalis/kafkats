import { describe, expect, it } from 'vitest'

import { murmur2, murmur2Partitioner } from '@/producer/partitioners/murmur2.js'
import { createRoundRobinPartitioner } from '@/producer/partitioners/round-robin.js'

describe('murmur2 partitioner', () => {
	it('computes deterministic hashes', () => {
		expect(murmur2(Buffer.from('test'))).toBe(716234879)
		expect(murmur2(Buffer.from('hello'))).toBe(2132663229)
	})

	it('returns -1 when key is null', () => {
		const partition = murmur2Partitioner('topic', null, Buffer.from('value'), 3)
		expect(partition).toBe(-1)
	})

	it('returns a partition index within range', () => {
		const partition = murmur2Partitioner('topic', Buffer.from('key'), Buffer.from('value'), 5)
		expect(partition).toBeGreaterThanOrEqual(0)
		expect(partition).toBeLessThan(5)
	})
})

describe('round robin partitioner', () => {
	it('cycles partitions per topic', () => {
		const partitioner = createRoundRobinPartitioner()
		expect(partitioner('topic', null, Buffer.alloc(0), 3)).toBe(0)
		expect(partitioner('topic', null, Buffer.alloc(0), 3)).toBe(1)
		expect(partitioner('topic', null, Buffer.alloc(0), 3)).toBe(2)
		expect(partitioner('topic', null, Buffer.alloc(0), 3)).toBe(0)
	})

	it('keeps separate counters per topic', () => {
		const partitioner = createRoundRobinPartitioner()
		expect(partitioner('topic-a', null, Buffer.alloc(0), 2)).toBe(0)
		expect(partitioner('topic-b', null, Buffer.alloc(0), 2)).toBe(0)
		expect(partitioner('topic-a', null, Buffer.alloc(0), 2)).toBe(1)
		expect(partitioner('topic-b', null, Buffer.alloc(0), 2)).toBe(1)
	})

	it('works with single-partition topics', () => {
		const partitioner = createRoundRobinPartitioner()
		expect(partitioner('topic', null, Buffer.alloc(0), 1)).toBe(0)
		expect(partitioner('topic', null, Buffer.alloc(0), 1)).toBe(0)
	})
})
