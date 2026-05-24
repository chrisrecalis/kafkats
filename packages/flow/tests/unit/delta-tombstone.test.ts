import { describe, it, expect } from 'vitest'

import { TableDeltaReduceNode, TableDeltaAggregateNode } from '@/processors/table.js'
import type { StreamRecord } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'

const DELTA_OP_HEADER = '__kafkats_delta_op'
const DELTA_ADD = Buffer.from('add')
const DELTA_SUB = Buffer.from('sub')

// Minimal Map-backed KeyValueStore that records whether delete() was called.
function mapStore<V>(): KeyValueStore<string, V> & { deleted: string[] } {
	const data = new Map<string, V>()
	const deleted: string[] = []
	return {
		deleted,
		name: 'agg',
		get: async (k: string) => (data.has(k) ? data.get(k)! : undefined),
		put: async (k: string, v: V) => {
			data.set(k, v)
		},
		delete: async (k: string) => {
			deleted.push(k)
			data.delete(k)
		},
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any
}

function record<V>(key: string, value: V, op: Buffer): StreamRecord<string, V> {
	return {
		key,
		value,
		timestamp: 0n,
		topic: 't',
		partition: 0,
		offset: 0n,
		headers: { [DELTA_OP_HEADER]: op },
	}
}

describe('delta reduce/aggregate tombstone on null result', () => {
	it('TableDeltaReduceNode tombstones (delete + forward null) when the subtractor empties the group', async () => {
		const store = mapStore<number>()
		const storeRef = { store }
		// price-sum reduce; subtractor returns null when the running total hits zero (group empty).
		const node = new TableDeltaReduceNode<string, number>(
			'agg',
			storeRef,
			(agg, v) => agg + v,
			(agg, v) => (agg - v === 0 ? (null as unknown as number) : agg - v)
		)

		const forwards: Array<{ key: string | null; value: number | null }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (r: StreamRecord<string, number>) => {
			forwards.push({ key: r.key, value: r.value })
		}

		await node.process(record('books', 10, DELTA_ADD))
		expect(forwards.at(-1)).toEqual({ key: 'books', value: 10 })

		// Last item removed: subtractor(10, 10) === null -> the group is empty.
		await node.process(record('books', 10, DELTA_SUB))
		expect(store.deleted).toEqual(['books']) // store entry removed, not left as null
		expect(forwards.at(-1)).toEqual({ key: 'books', value: null }) // tombstone forwarded

		// Re-adding restarts from the new value (store was cleared, so current is undefined,
		// not a leftover null that would corrupt the adder).
		await node.process(record('books', 7, DELTA_ADD))
		expect(forwards.at(-1)).toEqual({ key: 'books', value: 7 })
	})

	it('TableDeltaAggregateNode tombstones (delete + forward null) when the subtractor empties the group', async () => {
		const store = mapStore<number>()
		const storeRef = { store }
		const node = new TableDeltaAggregateNode<string, number, number>(
			'agg',
			storeRef,
			() => 0,
			(_key, v, agg) => agg + v,
			(_key, v, agg) => (agg - v === 0 ? (null as unknown as number) : agg - v)
		)

		const forwards: Array<{ key: string | null; value: number | null }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (r: StreamRecord<string, number>) => {
			forwards.push({ key: r.key, value: r.value })
		}

		await node.process(record('books', 10, DELTA_ADD))
		expect(forwards.at(-1)).toEqual({ key: 'books', value: 10 })

		await node.process(record('books', 10, DELTA_SUB))
		expect(store.deleted).toEqual(['books'])
		expect(forwards.at(-1)).toEqual({ key: 'books', value: null })

		// Re-add after tombstone: must reinitialize (0 + 7), not aggregate onto a stale null.
		await node.process(record('books', 7, DELTA_ADD))
		expect(forwards.at(-1)).toEqual({ key: 'books', value: 7 })
	})
})
