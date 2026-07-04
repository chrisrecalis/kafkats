import { describe, it, expect } from 'vitest'

import {
	StreamStreamJoinNode,
	StreamStreamLeftJoinNode,
	StreamStreamOuterJoinNode,
} from '@/processors/joins/stream-stream.js'
import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import { InMemoryWindowStore, codec } from '../../src/index.js'

const stringValueCodec = codec.string()

class CollectorNode<K, V> extends Processor<K, V> {
	readonly records: Array<StreamRecord<K, V>> = []

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return this
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		this.records.push(record)
	}
}

function record(key: string, value: string, ts: number, offset: number): StreamRecord<string, string> {
	return {
		key,
		value,
		timestamp: BigInt(ts),
		topic: 't',
		partition: 0,
		offset: BigInt(offset),
		headers: {},
	}
}

const JOIN_WINDOW_MS = 5000

function makeStore(name: string) {
	return new InMemoryWindowStore<string, string>(name, {
		keyCodec: codec.string(),
		valueCodec: stringValueCodec,
		retentionMs: JOIN_WINDOW_MS * 2,
		windowSizeMs: JOIN_WINDOW_MS,
	})
}

// Mirrors the kstream.ts leftJoin wiring: left side is a LeftJoinNode, right side an inner node.
function makeLeftJoin() {
	const leftRef = { store: makeStore('left') }
	const rightRef = { store: makeStore('right') }
	const joiner = (l: string, r: string | null) => `${l}+${r ?? 'NULL'}`
	const leftNode = new StreamStreamLeftJoinNode<string, string, string, string>(
		leftRef,
		rightRef,
		joiner,
		JOIN_WINDOW_MS
	)
	const rightNode = new StreamStreamJoinNode<string, string, string, string>(
		rightRef,
		leftRef,
		(r, l) => joiner(l, r),
		JOIN_WINDOW_MS
	)
	const results = new CollectorNode<string, string>()
	leftNode.connect(results)
	rightNode.connect(results)
	return { leftNode, rightNode, results }
}

// Mirrors the kstream.ts outerJoin wiring: both sides are OuterJoinNodes with swapped joiners.
function makeOuterJoin() {
	const leftRef = { store: makeStore('left') }
	const rightRef = { store: makeStore('right') }
	const joiner = (l: string | null, r: string | null) => `${l ?? 'NULL'}+${r ?? 'NULL'}`
	const leftNode = new StreamStreamOuterJoinNode<string, string, string, string>(
		leftRef,
		rightRef,
		joiner,
		JOIN_WINDOW_MS
	)
	const rightNode = new StreamStreamOuterJoinNode<string, string, string, string>(
		rightRef,
		leftRef,
		(r, l) => joiner(l, r),
		JOIN_WINDOW_MS
	)
	const results = new CollectorNode<string, string>()
	leftNode.connect(results)
	rightNode.connect(results)
	return { leftNode, rightNode, results }
}

describe('Left join emits null-padded results only when the window closes (no spurious results)', () => {
	it('suppresses the eager null-padded result when a match arrives later in the window', async () => {
		const { leftNode, rightNode, results } = makeLeftJoin()

		// Left record with no match yet — must NOT eagerly emit (v, null).
		await leftNode.process(record('k', 'v', 0, 0))
		// Match arrives later, inside the window.
		await rightNode.process(record('k', 'r', 1000, 0))
		// Advance stream time past window close via an unrelated key.
		await leftNode.process(record('other', 'x', 20_000, 1))

		const kResults = results.records.filter(r => r.key === 'k').map(r => r.value)
		// Pre-KIP-633 bug behavior emitted BOTH 'v+NULL' (spurious) and 'v+r'.
		expect(kResults).toEqual(['v+r'])
	})

	it('emits exactly one null-padded result at window close when no match ever arrives', async () => {
		const { leftNode, results } = makeLeftJoin()

		await leftNode.process(record('k', 'v', 0, 0))
		// Nothing emitted while the window is still open.
		expect(results.records.filter(r => r.key === 'k')).toHaveLength(0)

		// Advance stream time past window close (0 + 5000).
		await leftNode.process(record('other', 'x', 20_000, 1))

		const kResults = results.records.filter(r => r.key === 'k').map(r => r.value)
		expect(kResults).toEqual(['v+NULL'])
	})

	it('the right (inner) side advancing stream time also closes the left window', async () => {
		const { leftNode, rightNode, results } = makeLeftJoin()

		await leftNode.process(record('k', 'v', 0, 0))
		// Only the right stream advances stream time past the close.
		await rightNode.process(record('other', 'r', 20_000, 0))

		const kResults = results.records.filter(r => r.key === 'k').map(r => r.value)
		expect(kResults).toEqual(['v+NULL'])
	})
})

describe('Outer join emits null-padded results only when the window closes', () => {
	it('suppresses eager null-padded results on both sides when a match arrives in the window', async () => {
		const { leftNode, rightNode, results } = makeOuterJoin()

		await leftNode.process(record('k', 'v', 0, 0))
		await rightNode.process(record('k', 'r', 1000, 0))
		await leftNode.process(record('other', 'x', 20_000, 1))

		const kResults = results.records.filter(r => r.key === 'k').map(r => r.value)
		expect(kResults).toEqual(['v+r'])
	})

	it('emits one null-padded result per unmatched side at window close', async () => {
		const { leftNode, rightNode, results } = makeOuterJoin()

		await leftNode.process(record('k1', 'v', 0, 0))
		await rightNode.process(record('k2', 'r', 1000, 0))
		expect(results.records).toHaveLength(0)

		// Advance stream time past both closes.
		await leftNode.process(record('other', 'x', 20_000, 1))

		const values = results.records.map(r => `${r.key}:${r.value}`).sort()
		expect(values).toEqual(['k1:v+NULL', 'k2:NULL+r'])
	})
})
