import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { Processor } from '@/processor/api.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import { MAX_COMMIT_METADATA_BYTES } from '@/topology/subtopologies.js'
import { builder, chain, globalStore, noop, repartition, repartitionTopic, store } from './support.js'

const wallClock: TimestampExtractor = () => Date.now()

describe('InternalTopologyBuilder names', () => {
	it('generates node names from one counter; an explicit name consumes none', () => {
		const b = builder()
		expect(b.nodeName('KSTREAM-SOURCE')).toBe('KSTREAM-SOURCE-0000000000')
		expect(b.nodeName('KSTREAM-FILTER', 'only-paid')).toBe('only-paid')
		expect(b.nodeName('KSTREAM-FILTER')).toBe('KSTREAM-FILTER-0000000001')
	})

	it('counts stores and repartition topics separately from nodes', () => {
		const b = builder()
		b.nodeName('KSTREAM-SOURCE')
		expect([b.storeId(), b.storeId(), b.repartitionId(), b.repartitionId()]).toEqual([0, 1, 0, 1])
	})
})

describe('InternalTopologyBuilder registrations', () => {
	it('rejects a node name used twice, global store nodes included', () => {
		const b = builder()
		b.addSource({ name: 'src', topics: ['a'], internal: false })
		expect(() => b.addSource({ name: 'src', topics: ['b'], internal: false })).toThrow(TopologyError)
		b.addGlobalStore(globalStore('lookup', 'g'))
		expect(() =>
			b.addProcessor({ name: 'lookup-processor', supplier: noop(), parents: ['src'], stores: [] })
		).toThrow("A node named 'lookup-processor' already exists")
	})

	it('rejects sources without topics and nodes without, with unknown or with sink parents', () => {
		const b = builder()
		expect(() => b.addSource({ name: 'src', topics: [], internal: false })).toThrow('reads no topic')
		b.addSource({ name: 'src', topics: ['a'], internal: false })
		b.addSink({ name: 'sink', parents: ['src'], topic: 'out', internal: false })
		const processor = { supplier: noop(), stores: [] }
		expect(() => b.addProcessor({ name: 'p', parents: [], ...processor })).toThrow('needs at least one parent')
		expect(() => b.addProcessor({ name: 'p', parents: ['nope'], ...processor })).toThrow(
			"Parent 'nope' of 'p' is not registered"
		)
		expect(() => b.addProcessor({ name: 'p', parents: ['sink'], ...processor })).toThrow(
			"Sink 'sink' cannot be the parent of 'p'"
		)
	})

	it('SI9: a supplier returning the same processor twice throws synchronously and registers nothing', () => {
		const b = builder()
		b.addSource({ name: 'src', topics: ['a'], internal: false })
		const shared: Processor<unknown, unknown, unknown, unknown> = { process() {} }
		expect(() => b.addProcessor({ name: 'p', supplier: () => shared, parents: ['src'], stores: [] })).toThrow(
			"The supplier of 'p' returned the same processor twice"
		)
		expect(() => b.addProcessor({ name: 'p', supplier: noop(), parents: ['src'], stores: [] })).not.toThrow()
		const global = { ...globalStore('lookup', 'g'), supplier: () => shared }
		expect(() => b.addGlobalStore(global)).toThrow(TopologyError)
	})

	it('connects stores only to registered processors, never global stores', () => {
		const b = builder()
		chain(b, 'src', ['a'], 'p')
		b.addGlobalStore(globalStore('lookup', 'g'))
		expect(() => b.addProcessor({ name: 'q', supplier: noop(), parents: ['src'], stores: ['nope'] })).toThrow(
			"State store 'nope' of 'q' is not registered"
		)
		expect(() => b.addStateStore(store('s'), ['src'])).toThrow("'src' is not a registered processor")
		expect(() => b.connectProcessorAndStores('p', ['lookup'])).toThrow(
			"Global store 'lookup' is readable by every processor and cannot be connected to 'p'"
		)
		// The failed addStateStore registered nothing.
		expect(() => b.addStateStore(store('s'), ['p'])).not.toThrow()
	})

	it('requires global stores without a changelog', () => {
		const b = builder()
		expect(() => b.addGlobalStore({ ...globalStore('lookup', 'g'), store: store('lookup') })).toThrow(
			"Global store 'lookup' cannot have a changelog"
		)
	})

	it('accepts repartition and through nodes only for registered topics', () => {
		const b = builder()
		b.addSource({ name: 'src', topics: ['a'], internal: false })
		const topic = repartitionTopic('r')
		expect(() => b.addSink({ name: 'sink', parents: ['src'], topic, internal: 'repartition' })).toThrow(
			`'sink' uses '${topic}' as a repartition topic, but it is not registered`
		)
		expect(() => b.addSource({ name: 'r-src', topics: [topic], internal: 'repartition' })).toThrow(TopologyError)
		expect(() =>
			b.addSink({ name: 'sink', parents: ['src'], topic: () => topic, internal: 'repartition' })
		).toThrow('must write a fixed topic')
		expect(() => b.addSource({ name: 't-src', topics: ['t'], internal: 'through' })).toThrow(
			"reads 't' as a through topic, but it is not marked as one"
		)
		b.markThroughTopic('t')
		expect(() => b.addSource({ name: 't-src', topics: ['t'], internal: 'through' })).not.toThrow()
	})
})

describe('InternalTopologyBuilder repartition topics', () => {
	it('decorates the name with the application id', () => {
		expect(builder().addRepartitionTopic('repartition-0', {})).toBe('app-repartition-0-repartition')
	})

	it('KF6: the same named grouping claims its name again and gets the same topic', () => {
		const b = builder()
		const grouping = {}
		expect(b.addRepartitionTopic('by-user', grouping)).toBe(b.addRepartitionTopic('by-user', grouping))
	})

	it('B4: a name claimed by a different operation throws', () => {
		const b = builder()
		b.addRepartitionTopic('by-user', {})
		expect(() => b.addRepartitionTopic('by-user', {})).toThrow(
			"The repartition topic name 'by-user' is already used by another operation"
		)
	})

	it.each([0, -1, 1.5, Number.NaN])('rejects the partition count %s', partitions => {
		expect(() => builder().addRepartitionTopic('r', {}, partitions)).toThrow(TopologyError)
	})
})

describe('InternalTopologyBuilder.build', () => {
	it('is idempotent and freezes the builder', () => {
		const b = builder()
		chain(b, 'src', ['a'], 'p')
		const built = b.build()
		expect(b.build()).toBe(built)
		expect(() => b.addSource({ name: 'other', topics: ['b'], internal: false })).toThrow('topology already built')
		expect(() => b.nodeName('KSTREAM-FILTER')).toThrow(TopologyError)
		expect(() => b.storeId()).toThrow(TopologyError)
		expect(() => b.addRepartitionTopic('r', {})).toThrow(TopologyError)
	})

	it('rejects sources of one topic with different timestamp extractors', () => {
		const b = builder()
		b.addSource({ name: 's1', topics: ['a'], internal: false })
		b.addSource({ name: 's2', topics: ['a'], internal: false, timestampExtractor: wallClock })
		expect(() => b.build()).toThrow("Sources 's1' and 's2' read topic 'a' with different timestamp extractors")
	})

	it('rejects sources of one topic with different offset reset policies', () => {
		const b = builder()
		b.addSource({ name: 's1', topics: ['a'], internal: false, offsetReset: 'latest' })
		b.addSource({ name: 's2', topics: ['a'], internal: false })
		expect(() => b.build()).toThrow('different offset reset policies (latest and default)')
	})

	it('accepts sources of one topic that agree', () => {
		const b = builder()
		b.addSource({
			name: 's1',
			topics: ['a'],
			internal: false,
			offsetReset: 'latest',
			timestampExtractor: wallClock,
		})
		b.addSource({
			name: 's2',
			topics: ['a'],
			internal: false,
			offsetReset: 'latest',
			timestampExtractor: wallClock,
		})
		expect(() => b.build()).not.toThrow()
	})

	it('rejects a topic read by a global store and a stream source', () => {
		const b = builder()
		b.addGlobalStore(globalStore('lookup', 'a'))
		b.addSource({ name: 'src', topics: ['a'], internal: false })
		expect(() => b.build()).toThrow("Topic 'a' is read by global store 'lookup' and by source 'src'")
	})

	it("rejects a store named like another store's metadata key", () => {
		const b = builder()
		b.addStateStore(store('w', { kind: 'window', clock: {} }))
		b.addStateStore(store('w.observed'))
		chain(b, 'src', ['a'], 'p', ['w', 'w.observed'])
		expect(() => b.build()).toThrow(
			"State stores 'w' and 'w.observed' both use the commit metadata key 'w.observed'"
		)
	})

	it('rejects a subtopology whose commit metadata would exceed the broker limit', () => {
		const b = builder()
		const names = Array.from({ length: 30 }, (_, i) => `${'s'.repeat(60)}-${i}`)
		for (const name of names) b.addStateStore(store(name, { kind: 'window', clock: {} }))
		chain(b, 'src', ['a'], 'p', names)
		expect(() => b.build()).toThrow(`(limit ${MAX_COMMIT_METADATA_BYTES}); use shorter or fewer state stores`)
	})

	it('rejects two stores logging to one changelog topic', () => {
		const logging = { enabled: true, topic: 'shared-changelog', configs: {}, skipRestoration: false } as const
		const b = builder()
		b.addStateStore(store('s1', { logging }))
		b.addStateStore(store('s2', { logging }))
		chain(b, 'src', ['a'], 'p', ['s1', 's2'])
		expect(() => b.build()).toThrow(
			"State store 's2' logs to 'shared-changelog', which is already the changelog topic of state store 's1'"
		)
	})

	it('rejects a changelog topic named like a repartition topic', () => {
		const b = builder()
		chain(b, 'src', ['a'], 'p')
		repartition(b, 'p', 'r')
		const topic = repartitionTopic('r')
		b.addStateStore(store('s', { logging: { enabled: true, topic, configs: {}, skipRestoration: false } }))
		b.addProcessor({ name: 'q', supplier: noop(), parents: ['r-source'], stores: ['s'] })
		expect(() => b.build()).toThrow(`State store 's' logs to '${topic}', which is already a repartition topic`)
	})
})
