import { describe, expect, it, vi } from 'vitest'
import { noopLogger, TimeoutError } from '@kafkats/client'
import { ChangelogPartitionMismatchError, CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { prepareTopics, type InternalTopicOptions } from '@/topology/internal-topics.js'
import type { PartitionCounts } from '@/topology/nodes.js'
import { FakeAdmin, FakeCluster } from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { builder, chain, globalStore, noop, repartition, repartitionTopic, store } from './support.js'

const COUNT = 'app-count-changelog'
const AGG = 'app-agg-changelog'
const BY_KEY = repartitionTopic('by-key')

/** a -> count store -> repartition by-key -> agg store (replication factor 3, its own configs); global table g. */
function topology(): BuiltTopology {
	const b = builder()
	b.addStateStore(store('count'))
	chain(b, 'src', ['a'], 'p', ['count'])
	const source = repartition(b, 'p', 'by-key')
	const logging = {
		enabled: true,
		topic: AGG,
		configs: { 'cleanup.policy': 'compact', 'min.insync.replicas': '3' },
		replicationFactor: 3,
		skipRestoration: false,
	} as const
	b.addStateStore(store('agg', { logging }))
	b.addProcessor({ name: 'q', supplier: noop(), parents: [source], stores: ['agg'] })
	b.addGlobalStore(globalStore('lookup', 'g'))
	return b.build()
}

function setUp(topics: Record<string, number> = { a: 3, g: 1 }) {
	const cluster = new FakeCluster()
	for (const [name, partitions] of Object.entries(topics)) cluster.createTopic(name, partitions)
	const admin = new FakeAdmin(cluster)
	const clock = new ManualClock(0)
	const created = vi.spyOn(admin, 'createTopics')
	return { cluster, admin, clock, created, deps: { admin, clock, logger: noopLogger } }
}

/** Runs prepareTopics to the end, moving the clock `stepMs` forward each time it waits. */
async function prepare(
	{ deps, clock }: ReturnType<typeof setUp>,
	options: InternalTopicOptions = {},
	{ stepMs = 100, signal = new AbortController().signal, built = topology() } = {}
): Promise<PartitionCounts> {
	const result = prepareTopics(built, deps, options, signal)
	let settled = false
	result.then(
		() => (settled = true),
		() => (settled = true)
	)
	for (;;) {
		await new Promise(resolve => setImmediate(resolve))
		if (settled) return result
		clock.advance(stepMs)
	}
}

const describes = (admin: FakeAdmin) => admin.calls.filter(call => call.startsWith('describeTopics'))

describe('prepareTopics', () => {
	it('creates the missing internal topics with their counts, replication factors and configs', async () => {
		const setup = setUp()
		const counts = await prepare(setup, {
			replicationFactor: 2,
			topicConfigs: { 'retention.ms': '86400000', 'min.insync.replicas': '2' },
		})
		expect(counts).toEqual(
			new Map([
				['a', 3],
				['g', 1],
				[BY_KEY, 3],
				[COUNT, 3],
				[AGG, 3],
			])
		)
		expect(setup.admin.calls).toEqual([
			`describeTopics a,g,${BY_KEY},${COUNT},${AGG}`,
			`createTopics ${BY_KEY},${COUNT},${AGG}`,
			`describeTopics ${BY_KEY},${COUNT},${AGG}`,
		])
		const [specs] = setup.created.mock.calls[0]!
		expect(specs).toEqual([
			{
				name: BY_KEY,
				kind: 'repartition',
				partitions: undefined,
				// Kafka Streams' repartition configs under the app's topic configs (B3, BC20).
				configs: {
					'cleanup.policy': 'delete',
					'retention.ms': '86400000',
					'segment.bytes': '52428800',
					'min.insync.replicas': '2',
				},
				replicationFactor: 2,
				validateOnly: false,
			},
			// A changelog's configs are its store's, resolved by the store builder.
			{ name: COUNT, kind: 'changelog', configs: {}, replicationFactor: 2, validateOnly: false },
			{
				name: AGG,
				kind: 'changelog',
				configs: { 'cleanup.policy': 'compact', 'min.insync.replicas': '3' },
				replicationFactor: 3,
				validateOnly: false,
			},
		])
		expect(setup.cluster.topics.get(BY_KEY)?.partitions).toHaveLength(3)
	})

	it('validates internal topics that exist and creates nothing', async () => {
		const setup = setUp({ a: 3, g: 1, [BY_KEY]: 3, [COUNT]: 3, [AGG]: 3 })
		await prepare(setup)
		expect(setup.created).not.toHaveBeenCalled()
	})

	it('rejects an internal topic with another partition count, naming the topics feeding it', async () => {
		const changelog = prepare(setUp({ a: 3, g: 1, [COUNT]: 2 }))
		await expect(changelog).rejects.toBeInstanceOf(ChangelogPartitionMismatchError)
		await expect(changelog).rejects.toMatchObject({
			changelogTopic: COUNT,
			expectedPartitions: 3,
			actualPartitions: 2,
			sourceTopics: ['a'],
		})
		await expect(prepare(setUp({ a: 3, g: 1, [BY_KEY]: 5 }))).rejects.toMatchObject({
			changelogTopic: BY_KEY,
			actualPartitions: 5,
			sourceTopics: ['a'],
		})
	})

	it('with changelog.autoCreate false, a missing internal topic is an error and nothing is created (W17)', async () => {
		const setup = setUp({ a: 3, g: 1, [BY_KEY]: 3 })
		const prepared = prepare(setup, { autoCreate: false })
		await expect(prepared).rejects.toBeInstanceOf(SourceTopicNotFoundError)
		await expect(prepared).rejects.toMatchObject({ topic: COUNT })
		expect(setup.created).not.toHaveBeenCalled()
	})

	it('looks for a missing source topic again every 500 ms for 5 s', async () => {
		const missing = setUp({ g: 1 })
		await expect(prepare(missing)).rejects.toMatchObject({ name: 'SourceTopicNotFoundError', topic: 'a' })
		expect(describes(missing.admin)).toHaveLength(11)
		expect(missing.clock.time).toBe(5000)

		const late = setUp({ g: 1 })
		late.admin.steps.describeTopics.next('ok', 'ok', 'ok', () => {
			late.cluster.createTopic('a', 3)
			return 'ok'
		})
		expect((await prepare(late)).get('a')).toBe(3)
		expect(late.clock.time).toBe(1500)
	})

	it('does not retry an error a new look at metadata cannot fix', async () => {
		const b = builder()
		b.addStateStore(store('join'))
		chain(b, 'left', ['l', 'r'], 'p', ['join'])
		const setup = setUp({ l: 2, r: 3 })
		await expect(prepare(setup, {}, { built: b.build() })).rejects.toBeInstanceOf(CoPartitioningError)
		expect(describes(setup.admin)).toHaveLength(1)
	})

	it('waits until the created topics show up in metadata, for 30 s at most', async () => {
		const hidden = (setup: ReturnType<typeof setUp>, until: number) => {
			const describe = setup.admin.describeTopics.bind(setup.admin)
			vi.spyOn(setup.admin, 'describeTopics').mockImplementation(async (topics, options) => {
				const visible = await describe(topics, options)
				if (setup.clock.now() < until) visible.delete(COUNT)
				return visible
			})
		}
		const slow = setUp()
		hidden(slow, 300)
		await expect(prepare(slow)).resolves.toBeInstanceOf(Map)
		expect(slow.clock.time).toBe(300)

		const never = setUp()
		hidden(never, Infinity)
		const prepared = prepare(never, {}, { stepMs: 1000 })
		await expect(prepared).rejects.toBeInstanceOf(TimeoutError)
		await expect(prepared).rejects.toThrow(COUNT)
		expect(never.clock.time).toBe(30_000)
	})

	it('checks the count of a topic another instance created first', async () => {
		const setup = setUp()
		setup.admin.steps.createTopics.next(() => {
			setup.cluster.createTopic(COUNT, 5)
			return 'ok'
		})
		await expect(prepare(setup)).rejects.toMatchObject({
			name: 'ChangelogPartitionMismatchError',
			changelogTopic: COUNT,
			actualPartitions: 5,
		})
	})

	it('stops waiting when its signal aborts', async () => {
		const setup = setUp({ g: 1 })
		const controller = new AbortController()
		const prepared = prepareTopics(topology(), setup.deps, {}, controller.signal)
		await new Promise(resolve => setImmediate(resolve))
		expect(setup.clock.pendingTimers).toBe(1)
		controller.abort()
		await expect(prepared).rejects.toMatchObject({ name: 'AbortError' })
		expect(setup.clock.pendingTimers).toBe(0)
	})
})
