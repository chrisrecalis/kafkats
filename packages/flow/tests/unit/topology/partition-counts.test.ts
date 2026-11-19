import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
import { resolvePartitionCounts } from '@/topology/partition-counts.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { builder, chain, globalStore, noop, repartition, repartitionTopic, store } from './support.js'

function counts(b: InternalTopologyBuilder, external: Record<string, number>): Map<string, number> {
	return new Map(resolvePartitionCounts(b.build(), new Map(Object.entries(external))))
}

/** A processor reading `parents` and connected to a new store `<name>-store`. */
function stateful(b: InternalTopologyBuilder, name: string, parents: string[]): string {
	b.addStateStore(store(`${name}-store`))
	b.addProcessor({ name, supplier: noop(), parents, stores: [`${name}-store`] })
	return name
}

describe('resolvePartitionCounts', () => {
	it('takes source and global topics from metadata and ignores unrelated topics', () => {
		const b = builder()
		b.addGlobalStore(globalStore('lookup', 'g'))
		chain(b, 'a', ['a'], 'p')
		expect(counts(b, { a: 3, g: 2, other: 9 })).toEqual(
			new Map([
				['a', 3],
				['g', 2],
			])
		)
	})

	it.each<Record<string, number>>([{}, { a: 0 }])(
		'throws SourceTopicNotFoundError for a missing source topic (%o), naming its subtopology',
		external => {
			const b = builder()
			chain(b, 'a', ['a'], 'p')
			expect(() => counts(b, external)).toThrow(SourceTopicNotFoundError)
			expect(() => counts(b, external)).toThrow(
				expect.objectContaining({ topic: 'a', readBy: { subtopology: 0 } })
			)
		}
	)

	it('a missing source topic of a stateful subtopology names the subtopology, not a store or its changelog', () => {
		const b = builder()
		chain(b, 'x', ['x'], 'px')
		b.addSource({ name: 'a', topics: ['a', 'b'], internal: false })
		stateful(b, 'agg', ['a'])
		expect(() => counts(b, { x: 1, a: 1 })).toThrow(
			expect.objectContaining({
				topic: 'b',
				readBy: { subtopology: 1 },
				message: 'Source topic "b" of subtopology 1 does not exist. Create it first.',
			})
		)
	})

	it('throws SourceTopicNotFoundError for a missing global topic, naming the global store', () => {
		const b = builder()
		b.addGlobalStore(globalStore('lookup', 'g'))
		expect(() => counts(b, {})).toThrow(
			expect.objectContaining({
				name: 'SourceTopicNotFoundError',
				topic: 'g',
				readBy: { globalStore: 'lookup' },
				message: 'Source topic "g" of global store "lookup" does not exist. Create it first.',
			})
		)
	})

	it('sizes a repartition topic by the max source count of its writer, through chained repartitions', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a', 'b'], internal: false })
		const r1 = repartition(b, 'a', 'r1')
		b.addProcessor({ name: 'p', supplier: noop(), parents: [r1], stores: [] })
		repartition(b, 'p', 'r2')
		const result = counts(b, { a: 3, b: 5 })
		expect(result.get(repartitionTopic('r1'))).toBe(5)
		expect(result.get(repartitionTopic('r2'))).toBe(5)
	})

	it('takes an explicit repartition count and sizes downstream topics by it', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const r1 = repartition(b, 'a', 'r1', { partitions: 7 })
		repartition(b, r1, 'r2')
		const result = counts(b, { a: 3 })
		expect(result.get(repartitionTopic('r1'))).toBe(7)
		expect(result.get(repartitionTopic('r2'))).toBe(7)
	})

	it('gives a repartition topic nobody writes one partition', () => {
		const b = builder()
		b.addRepartitionTopic('orphan', {})
		chain(b, 'a', ['a'], 'p')
		expect(counts(b, { a: 3 }).get(repartitionTopic('orphan'))).toBe(1)
	})

	it('sizes a repartition topic its own writer reads by the counts known of its other upstream topics (KS)', () => {
		const b = builder()
		const owner = {}
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const loop = repartition(b, 'a', 'loop', { owner })
		const topic = b.addRepartitionTopic('loop', owner)
		b.addSink({ name: 'back', parents: [loop], topic, internal: 'repartition' })
		repartition(b, loop, 'after')
		const result = counts(b, { a: 3 })
		expect(result.get(topic)).toBe(3)
		expect(result.get(repartitionTopic('after'))).toBe(3)
	})

	it('throws when no topic upstream of a repartition topic has a known count', () => {
		const b = builder()
		chain(b, 'a', ['a'], 'p')
		const topic = b.addRepartitionTopic('loop', {})
		b.addSource({ name: 'loop', topics: [topic], internal: 'repartition' })
		b.addSink({ name: 'back', parents: ['loop'], topic, internal: 'repartition' })
		expect(() => counts(b, { a: 3 })).toThrow(
			`The partition count of repartition topic '${topic}' is unknown: no topic upstream of it has a known count`
		)
	})

	it('lets stateless subtopologies read topics with different counts', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a', 'b'], internal: false })
		b.addProcessor({ name: 'p', supplier: noop(), parents: ['a'], stores: [] })
		expect(() => counts(b, { a: 3, b: 5 })).not.toThrow()
	})

	it('requires the external topics of a stateful subtopology to have equal counts', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a', 'b'], internal: false })
		stateful(b, 'join', ['a'])
		expect(() => counts(b, { a: 4, b: 4 })).not.toThrow()
		expect(() => counts(b, { a: 4, b: 8 })).toThrow(CoPartitioningError)
		expect(() => counts(b, { a: 4, b: 8 })).toThrow('Topics [a (4), b (8)] feed the same stateful operation')
	})

	it('DC4: coerces a repartition topic to the count of the table it is joined with', () => {
		// stream(orders: 4).selectKey().join(table(customers: 8))
		const b = builder()
		chain(b, 'orders', ['orders'], 'select-key')
		const r = repartition(b, 'select-key', 'r')
		b.addSource({ name: 'customers', topics: ['customers'], internal: false })
		stateful(b, 'join', [r, 'customers'])
		const result = counts(b, { orders: 4, customers: 8 })
		expect(result.get(repartitionTopic('r'))).toBe(8)
		expect(result.get('app-join-store-changelog')).toBe(8)
	})

	it('DC4: recomputes the topics downstream of a coerced repartition topic', () => {
		// orders (8) -> r, joined with customers (4) -> r coerced to 4 -> r2 sized 4, not 8
		const b = builder()
		b.addSource({ name: 'orders', topics: ['orders'], internal: false })
		const r = repartition(b, 'orders', 'r')
		b.addSource({ name: 'customers', topics: ['customers'], internal: false })
		const join = stateful(b, 'join', [r, 'customers'])
		repartition(b, join, 'r2')
		const result = counts(b, { orders: 8, customers: 4 })
		expect(result.get(repartitionTopic('r'))).toBe(4)
		expect(result.get(repartitionTopic('r2'))).toBe(4)
	})

	it('rejects an explicit repartition count that differs from the co-partitioned topics', () => {
		const b = builder()
		b.addSource({ name: 'orders', topics: ['orders'], internal: false })
		const r = repartition(b, 'orders', 'r', { partitions: 3 })
		b.addSource({ name: 'customers', topics: ['customers'], internal: false })
		stateful(b, 'join', [r, 'customers'])
		expect(() => counts(b, { orders: 8, customers: 4 })).toThrow(
			`Repartition topic '${repartitionTopic('r')}' has 3 partitions, but the topics it is co-partitioned with have 4`
		)
	})

	it('co-partitions repartition-only groups to the max count, or to their explicit count', () => {
		const join = (partitions?: number) => {
			const b = builder()
			b.addSource({ name: 'a', topics: ['a'], internal: false })
			b.addSource({ name: 'b', topics: ['b'], internal: false })
			const left = repartition(b, 'a', 'left')
			const right = repartition(b, 'b', 'right', { partitions })
			stateful(b, 'join', [left, right])
			return counts(b, { a: 2, b: 6 })
		}
		expect(join().get(repartitionTopic('left'))).toBe(6)
		expect(join(3).get(repartitionTopic('left'))).toBe(3)
		expect(join(3).get('app-join-store-changelog')).toBe(3)
	})

	it('rejects co-partitioned repartition topics with different explicit counts', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const left = repartition(b, 'a', 'left', { partitions: 2 })
		const right = repartition(b, 'a', 'right', { partitions: 3 })
		stateful(b, 'join', [left, right])
		expect(() => counts(b, { a: 2 })).toThrow(TopologyError)
	})

	it('sizes changelog topics by the task count of their subtopology', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		stateful(b, 'count-a', ['a'])
		b.addSource({ name: 'b', topics: ['b'], internal: false })
		const r = repartition(b, 'b', 'r', { partitions: 5 })
		stateful(b, 'count-r', [r])
		const result = counts(b, { a: 3, b: 2 })
		expect(result.get('app-count-a-store-changelog')).toBe(3)
		expect(result.get('app-count-r-store-changelog')).toBe(5)
	})
})
