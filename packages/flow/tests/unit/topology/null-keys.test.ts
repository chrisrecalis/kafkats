import { describe, expect, it } from 'vitest'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import type { SinkDef } from '@/topology/nodes.js'
import { builder, noop, repartition } from './support.js'

function nullKeys(b: InternalTopologyBuilder): Record<string, SinkDef['nullKeys']> {
	const marks: Record<string, SinkDef['nullKeys']> = {}
	for (const node of b.build().subtopologies.flatMap(s => s.nodes)) {
		if (node.kind === 'sink') marks[node.name] = node.nullKeys
	}
	return marks
}

function processor(b: InternalTopologyBuilder, name: string, parents: string[], relaxed = false): string {
	b.addProcessor({ name, supplier: noop(), parents, stores: [], ...(relaxed ? { relaxedNullKeyJoin: true } : {}) })
	return name
}

describe('null keys on repartition sinks (KF12)', () => {
	it('keeps null keys through two repartitions and a filter before a left join', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const r1 = repartition(b, processor(b, 'select-1', ['a']), 'r1')
		const filtered = processor(b, 'filter', [r1])
		const r2 = repartition(b, processor(b, 'select-2', [filtered]), 'r2')
		b.addSource({ name: 'table', topics: ['t'], internal: false })
		processor(b, 'left-join', [r2, 'table'], true)
		expect(nullKeys(b)).toEqual({ 'r1-sink': 'keep', 'r2-sink': 'keep' })
	})

	it('drops null keys when no relaxed join is downstream', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const r = repartition(b, 'a', 'r')
		processor(b, 'count', [r])
		b.addSink({ name: 'out', parents: ['count'], topic: 'out', internal: false })
		expect(nullKeys(b)).toEqual({ 'r-sink': 'drop', out: undefined })
	})

	it('marks each repartition by its own descendants', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const joined = repartition(b, 'a', 'joined')
		const counted = repartition(b, 'a', 'counted')
		processor(b, 'outer-join', [joined], true)
		processor(b, 'count', [counted])
		expect(nullKeys(b)).toEqual({ 'joined-sink': 'keep', 'counted-sink': 'drop' })
	})

	it('does not follow user topics: a relaxed join behind to()/stream() does not keep null keys', () => {
		const b = builder()
		b.addSource({ name: 'a', topics: ['a'], internal: false })
		const r = repartition(b, 'a', 'r')
		b.addSink({ name: 'to-u', parents: [r], topic: 'u', internal: false })
		b.addSource({ name: 'from-u', topics: ['u'], internal: false })
		processor(b, 'left-join', ['from-u'], true)
		expect(nullKeys(b)).toEqual({ 'r-sink': 'drop', 'to-u': undefined })
	})
})
