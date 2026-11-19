import { describe, expect, it } from 'vitest'
import type { Subtopology } from '@/topology/built-topology.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { builder, chain, globalStore, noop, repartition, repartitionTopic, store } from './support.js'

function nodeNames(subtopology: Subtopology | undefined): string[] {
	return subtopology?.nodes.map(node => node.name) ?? []
}

function groups(b: InternalTopologyBuilder): string[][] {
	return b.build().subtopologies.map(nodeNames)
}

describe('subtopologies (union-find)', () => {
	it('unites processors and sinks with their parents', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		b.addSource({ name: 'b', topics: ['b'], internal: false })
		b.addProcessor({ name: 'merge', supplier: noop(), parents: ['a', 'b'], stores: [] })
		b.addSink({ name: 'out', parents: ['merge'], topic: 'out', internal: false })
		expect(groups(b)).toEqual([['a', 'b', 'merge', 'out']])
	})

	it('unites processors connected to the same store', () => {
		const b = builder()
		b.addStateStore(store('s'))
		chain(b, 'a', ['a'], 'pa', ['s'])
		chain(b, 'b', ['b'], 'pb')
		b.connectProcessorAndStores('pb', ['s'])
		expect(groups(b)).toEqual([['a', 'pa', 'b', 'pb']])
	})

	it('unites source nodes of the same topic (stream(t) beside table(t))', () => {
		const b = builder()
		chain(b, 'stream', ['t'], 'ps')
		chain(b, 'table', ['t'], 'pt')
		expect(groups(b)).toEqual([['stream', 'ps', 'table', 'pt']])
	})

	it('never unites a sink with a source: repartition, through and to()/stream() topics split', () => {
		const b = builder()
		chain(b, 'a', ['a'], 'p')
		const r = repartition(b, 'p', 'r')
		b.markThroughTopic('t')
		b.addSink({ name: 'to-t', parents: [r], topic: 't', internal: false })
		b.addSource({ name: 'from-t', topics: ['t'], internal: 'through' })
		b.addSink({ name: 'to-u', parents: ['from-t'], topic: 'u', internal: false })
		b.addSource({ name: 'from-u', topics: ['u'], internal: false })
		b.addSink({ name: 'out', parents: ['from-u'], topic: 'out', internal: false })
		expect(groups(b)).toEqual([
			['a', 'p', 'r-sink'],
			['r-source', 'to-t'],
			['from-t', 'to-u'],
			['from-u', 'out'],
		])
	})

	it('numbers subtopologies by the first appearance of their nodes in insertion order', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		b.addSource({ name: 'b', topics: ['b'], internal: false })
		b.addProcessor({ name: 'pb', supplier: noop(), parents: ['b'], stores: [] })
		b.addProcessor({ name: 'pa', supplier: noop(), parents: ['a'], stores: [] })
		const [first, second] = b.build().subtopologies
		expect([first?.id, nodeNames(first)]).toEqual([0, ['a', 'pa']])
		expect([second?.id, nodeNames(second)]).toEqual([1, ['b', 'pb']])
	})

	it('keeps global stores out of every subtopology', () => {
		const b = builder()
		b.addGlobalStore(globalStore('lookup', 'g'))
		chain(b, 'a', ['a'], 'p')
		expect(groups(b)).toEqual([['a', 'p']])
	})
})

describe('subtopology contents', () => {
	it('lists sources per topic in registration order, source, repartition and sink topics', () => {
		const b = builder()
		b.addSource({ name: 'stream-a', topics: ['a', 'b'], internal: false })
		b.addSource({ name: 'table-a', topics: ['a'], internal: false })
		b.addProcessor({ name: 'p', supplier: noop(), parents: ['stream-a', 'table-a'], stores: [] })
		const r = repartition(b, 'p', 'r')
		b.addSink({ name: 'static', parents: [r], topic: 'out', internal: false })
		b.addSink({ name: 'dynamic', parents: [r], topic: () => 'somewhere', internal: false })
		const [upstream, downstream] = b.build().subtopologies
		expect(upstream?.sourcesByTopic.get('a')?.map(s => s.name)).toEqual(['stream-a', 'table-a'])
		expect([...(upstream?.sourceTopics ?? [])]).toEqual(['a', 'b'])
		expect([...(upstream?.repartitionSourceTopics ?? [])]).toEqual([])
		expect([...(upstream?.sinkTopics ?? [])]).toEqual([repartitionTopic('r')])
		expect([...(downstream?.repartitionSourceTopics ?? [])]).toEqual([repartitionTopic('r')])
		expect([...(downstream?.sinkTopics ?? [])]).toEqual(['out'])
	})

	it('holds the connected stores in registration order with their changelogs', () => {
		const b = builder()
		b.addStateStore(store('unused'))
		b.addStateStore(store('second', { logging: { enabled: false } }))
		b.addStateStore(store('first'))
		chain(b, 'a', ['a'], 'p', ['first'])
		b.addStateStore(store('third'), ['p'])
		b.connectProcessorAndStores('p', ['second'])
		const [subtopology] = b.build().subtopologies
		expect(subtopology?.stores.map(s => s.name)).toEqual(['second', 'first', 'third'])
		expect([...(subtopology?.changelogTopics ?? [])]).toEqual([
			['first', 'app-first-changelog'],
			['third', 'app-third-changelog'],
		])
		expect(subtopology?.stateful).toBe(true)
		const processor = subtopology?.nodes.find(node => node.name === 'p')
		expect(processor?.kind === 'processor' && processor.stores).toEqual(['first', 'third', 'second'])
	})

	it('is stateless without connected stores and sees every global store', () => {
		const b = builder()
		b.addGlobalStore(globalStore('g1', 'g1'))
		b.addGlobalStore(globalStore('g2', 'g2'))
		chain(b, 'a', ['a'], 'p')
		const [subtopology] = b.build().subtopologies
		expect(subtopology?.stateful).toBe(false)
		expect(subtopology?.globalStores).toEqual(['g1', 'g2'])
		expect(subtopology?.metadataKeys.size).toBe(0)
	})

	it('allows the processor clock of every store, the store clock of clocked stores and emit progress', () => {
		const b = builder()
		b.addStateStore(store('kv'))
		b.addStateStore(store('win', { kind: 'window', clock: {} }))
		b.addStateStore(store('final', { kind: 'session', clock: { emitFinal: { kind: 'session' } } }))
		chain(b, 'a', ['a'], 'p', ['kv', 'win', 'final'])
		const [subtopology] = b.build().subtopologies
		expect([...(subtopology?.metadataKeys ?? [])]).toEqual([
			'kv',
			'win',
			'win.observed',
			'final',
			'final.observed',
			'final.emit',
		])
	})
})
