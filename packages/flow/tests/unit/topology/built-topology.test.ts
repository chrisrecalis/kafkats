import { describe, expect, it } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import { builder, chain, globalStore, noop, repartition, repartitionTopic, store } from './support.js'

/** a + b merged (stateless) -> R -> count (store 'counts'), joined with table c (store 'c-table'). */
function topology() {
	const b = builder()
	b.addGlobalStore(globalStore('lookup', 'g'))
	b.addSource({ name: 'a', topics: ['a'], internal: false, offsetReset: 'latest' })
	b.addSource({ name: 'b', topics: ['b'], internal: false })
	b.addProcessor({ name: 'merge', supplier: noop(), parents: ['a', 'b'], stores: [] })
	const r = repartition(b, 'merge', 'r', { partitions: 6 })
	b.addStateStore(store('counts'))
	b.addStateStore(store('c-table', { logging: { enabled: false } }))
	b.addProcessor({ name: 'count', supplier: noop(), parents: [r], stores: ['counts'] })
	chain(b, 'c', ['c'], 'table', ['c-table'])
	b.connectProcessorAndStores('count', ['c-table'])
	b.addSink({ name: 'out', parents: ['count'], topic: 'out', internal: false })
	b.markThroughTopic('t')
	b.addSink({ name: 'to-t', parents: ['count'], topic: 't', internal: false })
	b.addSource({ name: 'from-t', topics: ['t'], internal: 'through' })
	b.addSink({ name: 'dynamic', parents: ['from-t'], topic: () => 'x', internal: false })
	return b.build()
}

describe('BuiltTopology', () => {
	it('maps every source topic to its subtopology', () => {
		const built = topology()
		expect(built.applicationId).toBe('app')
		expect(built.subtopologies.map(s => [...s.sourceTopics])).toEqual([
			['a', 'b'],
			[repartitionTopic('r'), 'c'],
			['t'],
		])
		expect(built.subtopologyOf('c')).toBe(built.subtopologies[1])
		expect(built.subtopologyOf('g')).toBeUndefined()
		expect(built.subtopologyOf('out')).toBeUndefined()
	})

	it('subscribes the group to every source topic and preloads static sink topics', () => {
		const built = topology()
		expect([...built.groupTopics]).toEqual(['a', 'b', repartitionTopic('r'), 'c', 't'])
		expect([...built.staticSinkTopics]).toEqual([repartitionTopic('r'), 'out', 't'])
		expect(built.globalStores.map(g => g.topic)).toEqual(['g'])
	})

	it('lists repartition topics, then the changelogs of logged stores', () => {
		const built = topology()
		expect([...built.internalTopics.values()]).toEqual([
			{ name: repartitionTopic('r'), kind: 'repartition', partitions: 6, configs: {}, validateOnly: false },
			{
				name: 'app-counts-changelog',
				kind: 'changelog',
				configs: {},
				replicationFactor: undefined,
				validateOnly: false,
			},
		])
	})

	it('carries changelog configs and replication factor of the store', () => {
		const b = builder()
		const logging = {
			enabled: true,
			topic: 'custom',
			configs: { 'cleanup.policy': 'compact' },
			replicationFactor: 3,
			skipRestoration: false,
		} as const
		b.addStateStore(store('s', { logging }))
		chain(b, 'a', ['a'], 'p', ['s'])
		expect(b.build().internalTopics.get('custom')).toEqual({
			name: 'custom',
			kind: 'changelog',
			configs: { 'cleanup.policy': 'compact' },
			replicationFactor: 3,
			validateOnly: false,
		})
	})

	it('co-partitions the source topics of stateful subtopologies with several topics only', () => {
		const b = builder()
		b.addStateStore(store('single'))
		chain(b, 'x', ['x'], 'px', ['single'])
		b.addSource({ name: 'y', topics: ['y', 'z'], internal: false })
		b.addProcessor({ name: 'stateless', supplier: noop(), parents: ['y'], stores: [] })
		expect(b.build().copartitionGroups).toEqual([])
		expect(topology().copartitionGroups).toEqual([new Set([repartitionTopic('r'), 'c'])])
	})

	it('resolves reset policies: repartition and through topics earliest, else the source or the app default', () => {
		const built = topology()
		expect(built.resetPolicy('a', 'none')).toBe('latest')
		expect(built.resetPolicy('b', 'none')).toBe('none')
		expect(built.resetPolicy(repartitionTopic('r'), 'latest')).toBe('earliest')
		expect(built.resetPolicy('t', 'latest')).toBe('earliest')
		expect(built.resetPolicy('unknown', 'latest')).toBe('latest')
	})

	it('gives a task the partitions of the source topics that have its partition', () => {
		const built = topology()
		const merged = built.subtopologies[0]!
		const counts = new Map([
			['a', 4],
			['b', 2],
		])
		expect(built.inputPartitions(merged, 1, counts)).toEqual([
			{ topic: 'a', partition: 1 },
			{ topic: 'b', partition: 1 },
		])
		expect(built.inputPartitions(merged, 3, counts)).toEqual([{ topic: 'a', partition: 3 }])
		expect(() => built.inputPartitions(merged, 0, new Map([['a', 4]]))).toThrow(IllegalStateError)
	})
})
