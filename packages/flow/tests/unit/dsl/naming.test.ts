import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { NodePrefix, StorePrefix, repartitionName, storeName, suffixed } from '@/dsl/naming.js'
import { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import { TestDriver } from '@/testing/driver.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import { MAX_NAME_LENGTH } from '@/topology/names.js'
import { builder, chain, globalStore, store } from '../topology/support.js'

const str = codec.string()
const S = { key: str, value: str }

function topologyOf(driver: TestDriver): BuiltTopology {
	return internals(driver.flow).build()
}

function nodeNames(topology: BuiltTopology): string[] {
	return [
		...topology.subtopologies.flatMap(s => s.nodes.map(n => n.name)),
		...topology.globalStores.flatMap(g => [g.sourceName, g.processorName]),
	]
}

function storeNames(topology: BuiltTopology): string[] {
	return [
		...topology.subtopologies.flatMap(s => s.stores.map(store => store.name)),
		...topology.globalStores.map(g => g.store.name),
	]
}

function topicsOf(topology: BuiltTopology, kind: 'repartition' | 'changelog'): string[] {
	return [...topology.internalTopics.values()].filter(t => t.kind === kind).map(t => t.name)
}

/** A stream whose keys changed: key-based operations on it repartition. */
function rekeyed(driver: TestDriver, topic = 'in'): KStream<string, string> {
	return driver.input(topic, S).selectKey((_k, v) => v!)
}

describe('generated names', () => {
	it('names nodes KS style, with explicit names taking precedence', () => {
		const builder = new InternalTopologyBuilder('app')
		expect(builder.nodeName(NodePrefix.SOURCE)).toBe('KSTREAM-SOURCE-0000000000')
		expect(builder.nodeName(NodePrefix.TABLE_FILTER, 'active')).toBe('active')
		expect(builder.nodeName(NodePrefix.TABLE_FILTER)).toBe('KTABLE-FILTER-0000000001')
	})

	it("keeps today's store names: a generated name per store, counted apart from nodes", () => {
		const builder = new InternalTopologyBuilder('app')
		builder.nodeName(NodePrefix.SOURCE)
		expect(storeName(builder, StorePrefix.table('orders'))).toBe('table-orders-0')
		expect(storeName(builder, StorePrefix.COUNT, 'counts')).toBe('counts')
		expect(storeName(builder, StorePrefix.COUNT)).toBe('count-store-1')
		expect(storeName(builder, StorePrefix.globalTable('users'))).toBe('global-table-users-2')
	})

	it('names repartition topics repartition-<N> unless named', () => {
		const builder = new InternalTopologyBuilder('app')
		expect(repartitionName(builder)).toBe('repartition-0')
		expect(repartitionName(builder, 'by-user')).toBe('by-user')
		expect(builder.addRepartitionTopic(repartitionName(builder), {})).toBe('app-repartition-1-repartition')
	})

	it('suffixes the names an operator derives from its own; an unnamed operator generates them', () => {
		expect(suffixed('pairs', 'merge')).toBe('pairs-merge')
		expect(suffixed(undefined, 'merge')).toBeUndefined()
	})

	it('follow the order of the DSL calls, so one program always gets the same names', () => {
		const build = () => {
			const driver = new TestDriver()
			rekeyed(driver)
				.filter(() => true)
				.groupByKey({ key: str })
				.count()
				.toStream()
				.to('out')
			return topologyOf(driver)
		}
		const topology = build()
		expect(nodeNames(topology)).toEqual([
			'KSTREAM-SOURCE-0000000000',
			'KSTREAM-KEY-SELECT-0000000001',
			'KSTREAM-FILTER-0000000002',
			'KSTREAM-SINK-0000000003',
			'KSTREAM-SOURCE-0000000004',
			'KSTREAM-AGGREGATE-0000000005',
			'KTABLE-TOSTREAM-0000000006',
			'KSTREAM-SINK-0000000007',
		])
		expect(storeNames(topology)).toEqual(['count-store-0'])
		expect([...topology.internalTopics.keys()]).toEqual([
			'test-app-repartition-0-repartition',
			'test-app-count-store-0-changelog',
		])
		const again = build()
		expect([nodeNames(again), storeNames(again), [...again.internalTopics.keys()]]).toEqual([
			nodeNames(topology),
			storeNames(topology),
			[...topology.internalTopics.keys()],
		])
	})
})

describe('explicit names (6.22)', () => {
	/** Every operator of the DSL, each given a name (and its result store a storeName). */
	function namedProgram(driver: TestDriver): void {
		const orders = driver.input('orders', { ...S, name: 'orders' })
		const more = driver.input('more', { ...S, name: 'more' })
		const users = driver.table('users', { ...S, name: 'users', materialized: { storeName: 'users-store' } })
		const regions = driver.globalTable('regions', {
			...S,
			name: 'regions',
			materialized: { storeName: 'regions-store' },
		})

		const keyed = orders
			.filter(() => true, { name: 'filter' })
			.filterNot(() => false, { name: 'filter-not' })
			.map((k, v) => [k!, v!] as const, { name: 'map' })
			.mapValues(v => v, { name: 'map-values' })
			.flatMap((k, v) => [[k!, v!] as const], { name: 'flat-map' })
			.flatMapValues(v => [v!], { name: 'flat-map-values' })
			.peek(() => {}, { name: 'peek' })
			.selectKey((_k, v) => v!, { name: 'select-key' })
			.merge(more, { name: 'merge' })
		keyed.foreach(() => {}, { name: 'foreach' })
		keyed.to('out', { ...S, name: 'to' })
		keyed.repartition({ ...S, name: 'repartition' }).to('out', { ...S, name: 'to-again' })
		keyed.toTable({ ...S, name: 'to-table', storeName: 'to-table-store' })
		keyed.groupBy((_k, v) => v, { ...S, name: 'group-by' }).count({ name: 'count', storeName: 'count-store' })
		const grouped = keyed.groupByKey({ ...S, name: 'grouped' })
		grouped.reduce((a, b) => a + b, { name: 'reduce', storeName: 'reduce-store' })
		grouped
			.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
			.count({ name: 'windowed', storeName: 'windowed-store' })
			.suppress({ until: 'window-close', name: 'final' })
		grouped
			.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10))
			.count({ name: 'sessions', storeName: 'sessions' })
		grouped
			.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
			.count({ name: 'sliding', storeName: 'sliding' })
		keyed.join(users, (v, u) => v + u, { ...S, name: 'enrich' })
		keyed.leftJoin(users, (v, u) => v + u, { ...S, name: 'enrich-left' })
		const concat = (a: string | null, b: string | null) => `${a}${b}`
		const region = (_k: string | null, v: string) => v
		orders.join(regions, region, concat, { name: 'regional' })
		orders.leftJoin(regions, region, concat, { name: 'regional-left' })
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(10)
		const joined = { ...S, otherValue: str }
		keyed.join(more, concat, windows, { ...joined, name: 'pairs' })
		keyed.leftJoin(more, concat, windows, { ...joined, name: 'left-pairs' })
		keyed.outerJoin(more, concat, windows, { ...joined, name: 'outer-pairs', storeName: 'outer' })

		const active = users
			.filter(() => true, { name: 'table-filter', materialized: { storeName: 'table-filter-store' } })
			.filterNot(() => false, { name: 'table-filter-not' })
			.mapValues(v => v, { name: 'table-map-values', materialized: { value: str } })
		active.toStream({ name: 'to-stream' })
		active.toStream(k => k, { name: 'to-stream-mapped' })
		active.suppress({ until: 'time-limit', timeLimit: 10, name: 'held' })
		active.join(users, (a, b) => a + b, { name: 'table-join' })
		const leftStore = { storeName: 'left-store', value: str }
		active.leftJoin(users, (a, b) => a + b, { name: 'table-left-join', materialized: leftStore })
		const regrouped = active.groupBy((k, v) => [v, k] as const, { ...S, name: 'regrouped' })
		regrouped.count({ name: 'table-count', storeName: 'table-count-store' })
		regrouped.reduce(
			(a, b) => a + b,
			a => a,
			{ name: 'table-reduce', storeName: 'table-reduce-store' }
		)
	}

	it('every operator takes a name, and a fully named program has no generated name left', () => {
		const driver = new TestDriver()
		namedProgram(driver)
		const topology = topologyOf(driver)

		const nodes = nodeNames(topology)
		expect(nodes.filter(name => /-\d{10}$/.test(name))).toEqual([])
		expect(storeNames(topology).filter(name => /-\d+$/.test(name))).toEqual([])
		expect([...topology.internalTopics.keys()].filter(name => /repartition-\d+/.test(name))).toEqual([])
		expect(nodes).toEqual(
			expect.arrayContaining([
				'orders',
				'more',
				'filter',
				'filter-not',
				'map',
				'map-values',
				'flat-map',
				'flat-map-values',
				'peek',
				'select-key',
				'merge',
				'foreach',
				'to',
				'to-again',
				'to-table',
				'group-by',
				'count',
				'reduce',
				'windowed',
				'final',
				'sessions',
				'sliding',
				'enrich',
				'enrich-left',
				'regional',
				'regional-left',
				'users',
				'table-filter',
				'table-filter-not',
				'table-map-values',
				'to-stream',
				'to-stream-mapped',
				'held',
				'table-join',
				'table-left-join',
				'regrouped',
				'table-count',
				'table-reduce',
				'regions',
			])
		)
	})

	it('an operator registering several nodes, stores or topics names them after its own name (KS suffixes)', () => {
		const driver = new TestDriver()
		namedProgram(driver)
		const topology = topologyOf(driver)
		const nodes = nodeNames(topology)

		// table() and globalTable(): the source `<name>-source`, then the table's processor `<name>` (KS).
		expect(nodes).toEqual(expect.arrayContaining(['users-source', 'users', 'regions-source', 'regions']))
		expect(nodes).not.toContain('users-table-source')
		// Repartitions: the topic and its sink and source (KS createRepartitionedSource).
		for (const name of ['repartition', 'to-table', 'group-by', 'grouped', 'enrich', 'enrich-left', 'regrouped']) {
			expect(nodes).toEqual(expect.arrayContaining([`${name}-repartition-sink`, `${name}-repartition-source`]))
		}
		// Stream-stream joins: both sides, the merger and the repartition of the re-keyed side.
		expect(nodes).toEqual(
			expect.arrayContaining([
				'pairs-left-repartition-sink',
				'pairs-left-repartition-source',
				'pairs-this-join',
				'pairs-other-join',
				'pairs-merge',
				'left-pairs-this-join',
				'left-pairs-outer-other-join',
				'outer-pairs-outer-this-join',
				'outer-pairs-outer-other-join',
				'outer-pairs-merge',
			])
		)
		// Table-table joins: the merger takes the name, the sides `-join-this` and `-join-other`.
		expect(nodes).toEqual(expect.arrayContaining(['table-join-join-this', 'table-join-join-other']))

		// Join stores are named after StreamJoined.storeName, else the name; suppress's buffer is `<name>-store`.
		expect(storeNames(topology).sort()).toEqual([
			'count-store',
			'final-store',
			'held-store',
			'left-pairs-left-shared-join-store',
			'left-pairs-other-join-store',
			'left-pairs-this-join-store',
			'left-store',
			'outer-other-join-store',
			'outer-outer-shared-join-store',
			'outer-this-join-store',
			'pairs-other-join-store',
			'pairs-this-join-store',
			'reduce-store',
			'regions-store',
			'sessions',
			'sliding',
			'table-count-store',
			'table-filter-store',
			'table-reduce-store',
			'to-table-store',
			'users-store',
			'windowed-store',
		])
		expect(topicsOf(topology, 'repartition').sort()).toEqual([
			'test-app-enrich-left-repartition',
			'test-app-enrich-repartition',
			'test-app-group-by-repartition',
			'test-app-grouped-repartition',
			'test-app-left-pairs-left-repartition',
			'test-app-outer-pairs-left-repartition',
			'test-app-pairs-left-repartition',
			'test-app-regrouped-repartition',
			'test-app-repartition-repartition',
			'test-app-to-table-repartition',
		])
		// Changelogs follow the store names; a global store has none.
		expect(topicsOf(topology, 'changelog').sort()).toEqual(
			storeNames(topology)
				.filter(name => name !== 'regions-store')
				.map(name => `test-app-${name}-changelog`)
				.sort()
		)
	})

	it('KF6: a named grouping repartitions once, under its own name, for all its aggregations', async () => {
		const driver = new TestDriver()
		const grouped = rekeyed(driver).groupByKey({ ...S, name: 'by-value' })
		grouped.count({ storeName: 'a' }).toStream().to('counts', { key: str, value: codec.json<number>() })
		grouped.reduce((x, y) => x + y, { storeName: 'b' })
		grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count({ storeName: 'c' })
		const topology = topologyOf(driver)
		expect(topicsOf(topology, 'repartition')).toEqual(['test-app-by-value-repartition'])
		expect(nodeNames(topology).filter(name => name.startsWith('by-value'))).toEqual([
			'by-value-repartition-sink',
			'by-value-repartition-source',
		])

		await driver.run(async ({ send, producer }) => {
			await send('in', 'x', { key: Buffer.from('k'), codec: str })
			await send('in', 'x', { key: Buffer.from('k'), codec: str })
			expect(producer.messages.filter(m => m.topic === 'test-app-by-value-repartition')).toHaveLength(2)
			expect(producer.messages.filter(m => m.topic === 'counts').map(m => m.value?.toString())).toEqual([
				'1',
				'2',
			])
		})
	})

	it('an unnamed grouping repartitions per aggregation, through a topic named after its store if it has one', () => {
		const driver = new TestDriver()
		const grouped = rekeyed(driver).groupByKey(S)
		grouped.count({ storeName: 'counts' })
		grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count({ storeName: 'windows' })
		grouped.reduce((x, y) => x + y)
		const topology = topologyOf(driver)
		expect(topicsOf(topology, 'repartition')).toEqual([
			'test-app-counts-repartition',
			'test-app-windows-repartition',
			'test-app-repartition-0-repartition',
		])
		expect(nodeNames(topology).filter(name => name.startsWith('counts'))).toEqual([
			'counts-repartition-sink',
			'counts-repartition-source',
		])
	})
})

describe('name conflicts are TopologyErrors at the DSL call', () => {
	it('two operators with one name', () => {
		const stream = new TestDriver().input('in', S)
		stream.filter(() => true, { name: 'only' })
		expect(() => stream.peek(() => {}, { name: 'only' })).toThrow("A node named 'only' already exists")
	})

	it('a name an operator derived from its own', () => {
		const driver = new TestDriver()
		rekeyed(driver).join(driver.input('r', S), (a, b) => a + b, JoinWindows.ofTimeDifferenceWithNoGrace(10), {
			name: 'pairs',
			key: str,
		})
		expect(() => driver.input('x', S).filter(() => true, { name: 'pairs-merge' })).toThrow(TopologyError)
		expect(() => rekeyed(driver, 'y').groupByKey({ key: str, name: 'pairs-left' }).count()).toThrow(
			"The repartition topic name 'pairs-left' is already used by another operation"
		)
	})

	it('B4: one repartition name for two different groupings or operations', () => {
		const driver = new TestDriver()
		const stream = rekeyed(driver)
		stream.groupByKey({ key: str, name: 'g' }).count()
		const used = "The repartition topic name 'g' is already used by another operation"
		expect(() => stream.groupByKey({ key: str, name: 'g' }).count()).toThrow(used)
		expect(() => stream.repartition({ key: str, name: 'g' })).toThrow(used)
		expect(() => stream.toTable({ ...S, name: 'g' })).toThrow(used)
		expect(() =>
			driver
				.table('t', S)
				.groupBy((k, v) => [v, k] as const, { ...S, name: 'g' })
				.count()
		).toThrow(used)
	})

	it('B5: one store name for two stores, of the same kind or another', () => {
		const driver = new TestDriver()
		const grouped = driver.input('in', S).groupByKey()
		grouped.count({ storeName: 'state' })
		expect(() => grouped.reduce((a, b) => a + b, { storeName: 'state' })).toThrow(
			"State store 'state' already exists; every store needs its own name"
		)
		expect(() => driver.table('t', { ...S, materialized: { storeName: 'state' } })).toThrow(TopologyError)
		expect(() => grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count({ storeName: 'state' })).toThrow(
			"State store 'state' already exists as a key-value store and cannot be used as a window store"
		)
		expect(() =>
			grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10)).count({ storeName: 'state' })
		).toThrow("State store 'state' already exists as a key-value store and cannot be used as a session store")
		// suppress() keeps its buffer in `<name>-store`.
		driver.table('u', S).suppress({ until: 'time-limit', timeLimit: 10, name: 'held' })
		expect(() => grouped.count({ storeName: 'held-store' })).toThrow("State store 'held-store' already exists")
	})

	it('B5 before B4: a store name reused by an unnamed grouping that repartitions (its topic is named after it)', () => {
		const driver = new TestDriver()
		const exists = "State store 'totals' already exists; every store needs its own name"
		rekeyed(driver).groupByKey({ key: str }).count({ storeName: 'totals' })
		expect(() => rekeyed(driver, 'b').groupByKey({ key: str }).count({ storeName: 'totals' })).toThrow(exists)
		const windowed = rekeyed(driver, 'c').groupByKey({ key: str }).windowedBy(TimeWindows.ofSizeWithNoGrace(10))
		expect(() => windowed.count({ storeName: 'totals' })).toThrow(
			"State store 'totals' already exists as a key-value store and cannot be used as a window store"
		)
		const grouped = rekeyed(driver, 'd').groupByKey({ key: str })
		grouped.count({ storeName: 'own' })
		expect(() => grouped.reduce((a, b) => a + b, { storeName: 'own' })).toThrow(
			"State store 'own' already exists; every store needs its own name"
		)
		const regrouped = driver.table('t', S).groupBy((k, v) => [v, k] as const, S)
		expect(() => regrouped.count({ storeName: 'totals' })).toThrow(exists)
	})
})

describe('illegal names are TopologyErrors at the DSL call (KS Named.validate, Kafka topic names)', () => {
	const illegal = ['', '.', '..', 'a b', 'a/b', 'ä', 'x'.repeat(MAX_NAME_LENGTH + 1)]
	const nameError = /^(Node|State store|Repartition topic) name /

	it.each(illegal)('node name %j', name => {
		const stream = new TestDriver().input('in', S)
		expect(() => stream.filter(() => true, { name })).toThrow(nameError)
		expect(() => stream.to('out', { name })).toThrow(nameError)
	})

	it.each(illegal)('store name %j', name => {
		const grouped = new TestDriver().input('in', S).groupByKey()
		expect(() => grouped.count({ storeName: name })).toThrow(nameError)
	})

	it.each(illegal)('repartition name %j', name => {
		const stream = rekeyed(new TestDriver())
		expect(() => stream.groupByKey({ key: str, name }).count()).toThrow(nameError)
		expect(() => stream.repartition({ key: str, name })).toThrow(nameError)
	})

	it('names the rule a name breaks', () => {
		const stream = new TestDriver().input('in', S)
		expect(() => stream.filter(() => true, { name: '' })).toThrow('Node name must not be empty')
		expect(() => stream.filter(() => true, { name: '..' })).toThrow("Node name cannot be '..'")
		expect(() => stream.filter(() => true, { name: 'a b' })).toThrow(
			"Node name 'a b' is illegal: names may only contain ASCII alphanumerics, '.', '_' and '-'"
		)
		expect(() => stream.groupByKey().count({ storeName: 'x'.repeat(250) })).toThrow(
			`State store name '${'x'.repeat(250)}' is 250 characters long; names may be at most 249`
		)
	})

	it('accepts ASCII alphanumerics, dots, underscores and dashes, up to 249 of them', () => {
		const stream = new TestDriver().input('in', S)
		expect(() => stream.filter(() => true, { name: 'Only.paid_orders-2' })).not.toThrow()
		expect(() => stream.peek(() => {}, { name: 'x'.repeat(MAX_NAME_LENGTH) })).not.toThrow()
	})

	it('internal topics: the application id and the decorated length count', () => {
		const long = 'x'.repeat(240)
		expect(() => rekeyed(new TestDriver()).repartition({ key: str, name: long })).toThrow(
			`Repartition topic name 'test-app-${long}-repartition' is 261 characters long`
		)
		const grouped = new TestDriver({ applicationId: 'my app' }).input('in', S).groupByKey()
		expect(() => grouped.count({ storeName: 'counts' })).toThrow(
			"Changelog topic name 'my app-counts-changelog' is illegal"
		)
	})

	it('a custom changelog topic', () => {
		const grouped = new TestDriver().input('in', S).groupByKey()
		expect(() => grouped.count({ storeName: 'counts', changelog: { topicName: 'counts changelog' } })).toThrow(
			"Changelog topic name 'counts changelog' is illegal"
		)
		expect(() => grouped.count({ storeName: 'counts', changelog: { topicName: 'counts.log' } })).not.toThrow()
	})

	it('every registration of the topology builder, Processor API ones included', () => {
		const b = builder()
		expect(() => b.addSource({ name: 'a b', topics: ['in'], internal: false })).toThrow("Node name 'a b'")
		chain(b, 'src', ['in'], 'p')
		const processor = { supplier: () => ({ process() {} }), parents: ['p'], stores: [] }
		expect(() => b.addProcessor({ name: 'a b', ...processor })).toThrow("Node name 'a b'")
		expect(() => b.addSink({ name: 'a b', parents: ['p'], topic: 'out', internal: false })).toThrow(
			"Node name 'a b'"
		)
		expect(() => b.addStateStore(store('a b'))).toThrow("State store name 'a b' is illegal")
		expect(() => b.addGlobalStore({ ...globalStore('g', 'g'), sourceName: 'a b' })).toThrow("Node name 'a b'")
		const unlogged = store('a b', { logging: { enabled: false } })
		expect(() => b.addGlobalStore({ ...globalStore('g', 'g'), store: unlogged })).toThrow(
			"State store name 'a b' is illegal"
		)
		expect(() => b.addRepartitionTopic('a b', {})).toThrow("Repartition topic name 'a b' is illegal")
	})
})
