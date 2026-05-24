import { describe, expect, it } from 'vitest'

import { murmur2, murmur2Partitioner } from '@/producer/partitioners/murmur2.js'
import { createRoundRobinPartitioner, nextRoundRobinPartition } from '@/producer/partitioners/round-robin.js'

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

describe('nextRoundRobinPartition', () => {
	it('cycles through partitions starting from a fresh (-1) state', () => {
		expect(nextRoundRobinPartition(-1, 3)).toBe(0)
		expect(nextRoundRobinPartition(0, 3)).toBe(1)
		expect(nextRoundRobinPartition(1, 3)).toBe(2)
		expect(nextRoundRobinPartition(2, 3)).toBe(0)
	})

	it('always returns a partition in range — state stays bounded forever', () => {
		// The previous partition is always < partitionCount, so the value the partitioner stores
		// never grows toward Number.MAX_SAFE_INTEGER (which would freeze an unbounded counter and
		// pin every message to one partition).
		for (const count of [1, 2, 7]) {
			for (let previous = -1; previous < count; previous++) {
				const partition = nextRoundRobinPartition(previous, count)
				expect(partition).toBeGreaterThanOrEqual(0)
				expect(partition).toBeLessThan(count)
				expect(Number.isSafeInteger(partition)).toBe(true)
			}
		}
	})
})
