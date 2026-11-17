import { describe, expect, it, vi } from 'vitest'
import { codec } from '@/codec.js'
import {
	DELTA_OP_HEADER,
	groupedRowCodec,
	repartitionMapProcessor,
	type GroupedRow,
} from '@/operators/ktable-repartition-map.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'
import { processorDef, ProcessorHarness, sourceDef } from './support/processor-harness.js'

const str = codec.string()

/** KTable.groupBy's repartition map over the changes of the source 'table', regrouped by `selector`. */
async function repartitionMap(selector: (key: string, value: number) => readonly [string, number]) {
	const order: string[] = []
	const forwarded: StreamRecord<string, number>[] = []
	const mapping = new InMemoryKeyValueStore<string, GroupedRow<string, number>>('mapping', {
		keyCodec: str,
		valueCodec: groupedRowCodec(str, codec.json<number>()),
	})
	const put = mapping.put.bind(mapping)
	vi.spyOn(mapping, 'put').mockImplementation((key, row) => {
		order.push(`put(${key}, group=${row.groupedKey}, value=${row.value})`)
		return put(key, row)
	})
	const deleted = vi.spyOn(mapping, 'delete')
	const record = (r: StreamRecord<string, number>) => {
		forwarded.push(r)
		order.push(`forward(${r.headers[DELTA_OP_HEADER]?.toString()}: key=${r.key}, value=${r.value})`)
	}
	const harness = await ProcessorHarness.start(
		[
			sourceDef('source', ['table']),
			processorDef('select', ['source'], repartitionMapProcessor(selector, 'mapping'), ['mapping']),
			processorDef('sink', ['select'], () => ({ process: record })),
		],
		{ stores: [mapping] }
	)
	const update = (key: string, newValue: number | null, headers: RecordHeaders = {}) =>
		harness.process('table', { key, value: { newValue, oldValue: null }, timestamp: 3, headers })
	return { order, forwarded, mapping, deleted, update }
}

describe('KTable.groupBy repartition map (mapping store)', () => {
	it('forwards the retraction and the addition BEFORE persisting the new mapping (a replay re-emits them)', async () => {
		const { order, mapping, update } = await repartitionMap((_k, v) => ['new-group', v])
		await mapping.put('source-key', { groupedKey: 'old-group', value: 10 })
		order.length = 0

		await update('source-key', 20)

		expect(order).toEqual([
			'forward(sub: key=old-group, value=10)',
			'forward(add: key=new-group, value=20)',
			'put(source-key, group=new-group, value=20)',
		])
	})

	it('a deleted row only retracts its contribution and drops its mapping; user headers are kept', async () => {
		const { forwarded, mapping, deleted, update } = await repartitionMap((k, v) => [`${k}-group`, v * 2])
		const trace = { trace: Buffer.from('t') }

		await update('row', 1, trace)
		await update('row', null, trace)
		await update('row', null, trace)

		expect(forwarded.map(r => [r.headers[DELTA_OP_HEADER]?.toString(), r.key, r.value, r.timestamp])).toEqual([
			['add', 'row-group', 2, 3],
			['sub', 'row-group', 2, 3],
		])
		expect(forwarded.map(r => r.headers.trace?.toString())).toEqual(['t', 't'])
		expect(deleted).toHaveBeenCalledTimes(1)
		expect(await mapping.get('row')).toBeUndefined()
	})
})
