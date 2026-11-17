import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { codec } from '@/codec.js'
import { TestDriver } from '@/testing/driver.js'
import { describeTopology } from '@/topology/describe.js'
import { builder, chain, globalStore, noop, store } from './support.js'

const client = { clientId: 'app', brokers: ['localhost:9092'] }
const str = codec.string()
const S = { key: str, value: str }

describe('describe()', () => {
	it("describes a pipeline in Kafka Streams' format, and the driver runs the described topology", async () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.filter((_key, value) => value !== '')
			.mapValues(value => value?.toUpperCase() ?? null)
			.to('out', { value: str })

		const description = driver.flow.describe()
		expect(String(description)).toBe(
			[
				'Topologies:',
				'   Sub-topology: 0',
				'    Source: KSTREAM-SOURCE-0000000000 (topics: [in])',
				'      --> KSTREAM-FILTER-0000000001',
				'    Processor: KSTREAM-FILTER-0000000001 (stores: [])',
				'      --> KSTREAM-MAPVALUES-0000000002',
				'      <-- KSTREAM-SOURCE-0000000000',
				'    Processor: KSTREAM-MAPVALUES-0000000002 (stores: [])',
				'      --> KSTREAM-SINK-0000000003',
				'      <-- KSTREAM-FILTER-0000000001',
				'    Sink: KSTREAM-SINK-0000000003 (topic: out)',
				'      <-- KSTREAM-MAPVALUES-0000000002',
				'',
				'',
			].join('\n')
		)
		expect(description.globalStores).toEqual([])
		expect(description.subtopologies).toEqual([
			{
				id: 0,
				nodes: [
					{
						name: 'KSTREAM-SOURCE-0000000000',
						kind: 'source',
						topics: ['in'],
						stores: [],
						predecessors: [],
						successors: ['KSTREAM-FILTER-0000000001'],
					},
					{
						name: 'KSTREAM-FILTER-0000000001',
						kind: 'processor',
						topics: [],
						stores: [],
						predecessors: ['KSTREAM-SOURCE-0000000000'],
						successors: ['KSTREAM-MAPVALUES-0000000002'],
					},
					{
						name: 'KSTREAM-MAPVALUES-0000000002',
						kind: 'processor',
						topics: [],
						stores: [],
						predecessors: ['KSTREAM-FILTER-0000000001'],
						successors: ['KSTREAM-SINK-0000000003'],
					},
					{
						name: 'KSTREAM-SINK-0000000003',
						kind: 'sink',
						topics: ['out'],
						stores: [],
						predecessors: ['KSTREAM-MAPVALUES-0000000002'],
						successors: [],
					},
				],
			},
		])

		await driver.run(async ({ send, output }) => {
			await send('in', 'a', { keyValue: 'k', keyCodec: str, codec: str })
			await send('in', '', { keyValue: 'k', keyCodec: str, codec: str })
			expect(output('out', S).map(record => record.value)).toEqual(['A'])
		})
	})

	it('splits subtopologies at a repartition topic, which carries its full name; stores show at their processors', async () => {
		const driver = new TestDriver({ applicationId: 'wc' })
		driver
			.input('lines', S)
			.flatMapValues(line => line?.split(' ') ?? [])
			.selectKey((_key, word) => word)
			.groupByKey({ key: str, value: str })
			.count()
			.toStream()
			.to('counts', { value: codec.json<number>() })

		expect(String(driver.flow.describe())).toBe(
			[
				'Topologies:',
				'   Sub-topology: 0',
				'    Source: KSTREAM-SOURCE-0000000000 (topics: [lines])',
				'      --> KSTREAM-FLATMAPVALUES-0000000001',
				'    Processor: KSTREAM-FLATMAPVALUES-0000000001 (stores: [])',
				'      --> KSTREAM-KEY-SELECT-0000000002',
				'      <-- KSTREAM-SOURCE-0000000000',
				'    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])',
				'      --> KSTREAM-SINK-0000000003',
				'      <-- KSTREAM-FLATMAPVALUES-0000000001',
				'    Sink: KSTREAM-SINK-0000000003 (topic: wc-repartition-0-repartition)',
				'      <-- KSTREAM-KEY-SELECT-0000000002',
				'',
				'  Sub-topology: 1',
				'    Source: KSTREAM-SOURCE-0000000004 (topics: [wc-repartition-0-repartition])',
				'      --> KSTREAM-AGGREGATE-0000000005',
				'    Processor: KSTREAM-AGGREGATE-0000000005 (stores: [count-store-0])',
				'      --> KTABLE-TOSTREAM-0000000006',
				'      <-- KSTREAM-SOURCE-0000000004',
				'    Processor: KTABLE-TOSTREAM-0000000006 (stores: [])',
				'      --> KSTREAM-SINK-0000000007',
				'      <-- KSTREAM-AGGREGATE-0000000005',
				'    Sink: KSTREAM-SINK-0000000007 (topic: counts)',
				'      <-- KTABLE-TOSTREAM-0000000006',
				'',
				'',
			].join('\n')
		)

		await driver.run(async ({ send, output }) => {
			await send('lines', 'a b a', { keyValue: 'line', keyCodec: str, codec: str })
			expect(output('counts', { key: str, value: codec.json<number>() }).map(r => [r.key, r.value])).toEqual([
				['a', 1],
				['b', 1],
				['a', 2],
			])
		})
	})

	it('describes global stores after the subtopologies: the source of the topic and the processor writing the store', async () => {
		const driver = new TestDriver()
		const users = driver.globalTable('users', S)
		driver
			.input('events', S)
			.join(users, (event, user) => `${event}@${user}`)
			.to('enriched', { value: str })

		const description = driver.flow.describe()
		expect(description.globalStores).toEqual([
			{
				id: 1,
				source: {
					name: 'KSTREAM-SOURCE-0000000000',
					kind: 'source',
					topics: ['users'],
					stores: [],
					predecessors: [],
					successors: ['KTABLE-SOURCE-0000000001'],
				},
				processor: {
					name: 'KTABLE-SOURCE-0000000001',
					kind: 'processor',
					topics: [],
					stores: ['global-table-users-0'],
					predecessors: ['KSTREAM-SOURCE-0000000000'],
					successors: [],
				},
			},
		])
		expect(String(description)).toBe(
			[
				'Topologies:',
				'   Sub-topology: 0',
				'    Source: KSTREAM-SOURCE-0000000002 (topics: [events])',
				'      --> KSTREAM-JOIN-0000000003',
				'    Processor: KSTREAM-JOIN-0000000003 (stores: [])',
				'      --> KSTREAM-SINK-0000000004',
				'      <-- KSTREAM-SOURCE-0000000002',
				'    Sink: KSTREAM-SINK-0000000004 (topic: enriched)',
				'      <-- KSTREAM-JOIN-0000000003',
				'',
				'  Sub-topology: 1 for global store (will not generate tasks)',
				'    Source: KSTREAM-SOURCE-0000000000 (topics: [users])',
				'      --> KTABLE-SOURCE-0000000001',
				'    Processor: KTABLE-SOURCE-0000000001 (stores: [global-table-users-0])',
				'      --> none',
				'      <-- KSTREAM-SOURCE-0000000000',
				'',
			].join('\n')
		)

		await driver.run(async ({ send, output }) => {
			await send('users', 'ann', { keyValue: 'u1', keyCodec: str, codec: str })
			await send('events', 'login', { keyValue: 'u1', keyCodec: str, codec: str })
			expect(output('enriched', S).map(record => record.value)).toEqual(['login@ann'])
		})
	})

	it('a sink whose topic an extractor picks lists no topic', async () => {
		const driver = new TestDriver()
		driver.input('in', S).to(key => `out-${key}`)
		const description = driver.flow.describe()
		expect(description.subtopologies[0]?.nodes[1]).toMatchObject({
			name: 'KSTREAM-SINK-0000000001',
			kind: 'sink',
			topics: [],
		})
		expect(String(description)).toContain('    Sink: KSTREAM-SINK-0000000001 (topic extractor)\n')

		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { keyValue: 'k', keyCodec: str, codec: str })
			expect(output('out-k', S).map(record => record.value)).toEqual(['v'])
		})
	})

	it('describe() builds the topology: later DSL calls throw, and an invalid topology throws its TopologyError', () => {
		const app = flow({ applicationId: 'app', client })
		const stream = app.stream('in', S)
		stream.to('out')
		const description = app.describe()
		expect(app.describe()).toEqual(description)
		expect(() => stream.to('late')).toThrow(TopologyError)

		const invalid = flow({ applicationId: 'app', client })
		invalid.stream('users', S)
		invalid.globalTable('users', S)
		expect(() => invalid.describe()).toThrow(TopologyError)
	})

	it('is plain data: JSON keeps everything but toString()', () => {
		const app = flow({ applicationId: 'app', client })
		app.globalTable('g', S)
		app.stream('in', S).to('out')
		const description = app.describe()
		expect(JSON.parse(JSON.stringify(description))).toEqual({
			subtopologies: description.subtopologies,
			globalStores: description.globalStores,
		})
	})

	it('an empty topology', () => {
		const description = flow({ applicationId: 'app', client }).describe()
		expect(description).toEqual({ subtopologies: [], globalStores: [] })
		expect(String(description)).toBe('Topologies:\n ')
	})
})

describe('describeTopology (Kafka Streams node order and format)', () => {
	it('orders nodes and neighbours by subtree size, which counts every path, then by name', () => {
		// src -> z -> (z1, z2) -> zm: 5 paths from z over 4 nodes; src -> y -> y1 -> y2 -> y3: 4 paths and nodes.
		const b = builder()
		b.addSource({ name: 'src', topics: ['t'], internal: false })
		b.addProcessor({ name: 'z', supplier: noop(), parents: ['src'], stores: [] })
		b.addProcessor({ name: 'z1', supplier: noop(), parents: ['z'], stores: [] })
		b.addProcessor({ name: 'z2', supplier: noop(), parents: ['z'], stores: [] })
		b.addProcessor({ name: 'zm', supplier: noop(), parents: ['z2', 'z1'], stores: [] })
		b.addProcessor({ name: 'y', supplier: noop(), parents: ['src'], stores: [] })
		b.addProcessor({ name: 'y1', supplier: noop(), parents: ['y'], stores: [] })
		b.addProcessor({ name: 'y2', supplier: noop(), parents: ['y1'], stores: [] })
		b.addSink({ name: 'y3', parents: ['y2'], topic: 'out', internal: false })

		const [subtopology] = describeTopology(b.build()).subtopologies
		const nodes = subtopology?.nodes ?? []
		expect(nodes.map(node => node.name)).toEqual(['src', 'z', 'y', 'y1', 'y2', 'z1', 'z2', 'y3', 'zm'])
		expect(nodes[0]?.successors).toEqual(['z', 'y'])
		expect(nodes.find(node => node.name === 'zm')?.predecessors).toEqual(['z1', 'z2'])
	})

	it('lists every topic of a source and the stores of a processor in connection order', () => {
		const b = builder()
		b.addStateStore(store('s2'))
		b.addStateStore(store('s1'))
		chain(b, 'src', ['a', 'b'], 'p', ['s2'])
		b.connectProcessorAndStores('p', ['s1'])
		b.addGlobalStore(globalStore('g', 'g-topic'))

		expect(String(describeTopology(b.build()))).toBe(
			[
				'Topologies:',
				'   Sub-topology: 0',
				'    Source: src (topics: [a, b])',
				'      --> p',
				'    Processor: p (stores: [s2, s1])',
				'      --> none',
				'      <-- src',
				'',
				'  Sub-topology: 1 for global store (will not generate tasks)',
				'    Source: g-source (topics: [g-topic])',
				'      --> g-processor',
				'    Processor: g-processor (stores: [g])',
				'      --> none',
				'      <-- g-source',
				'',
			].join('\n')
		)
	})
})
