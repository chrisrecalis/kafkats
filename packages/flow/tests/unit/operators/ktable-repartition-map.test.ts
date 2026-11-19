import { describe, expect, it } from 'vitest'
import type { KeyValue } from '@/api/kstream.js'
import { codec, type Codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import { repartitionMapProcessor } from '@/operators/ktable-repartition-map.js'
import type { ForwardResult } from '@/processor/api.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from '../support/processor-harness.js'

const trace = { trace: Buffer.from('t') }

/** KTable.groupBy's repartition map over the updates of 'table'; `downstream` processes what it forwards. */
async function repartitionMap<KR, VR>(
	selector: (key: string, value: string) => KeyValue<KR, VR>,
	keyCodec: Codec<KR>,
	downstream?: (record: StreamRecord<KR, Change<VR>>) => ForwardResult
) {
	const out = capture<KR, Change<VR>>('out', ['select'])
	const sink = downstream && processorDef('out', ['select'], () => ({ process: downstream }))
	const harness = await ProcessorHarness.start([
		sourceDef('source', ['table']),
		processorDef('select', ['source'], repartitionMapProcessor(selector, keyCodec)),
		sink ?? out.def,
	])
	type Value = string | null
	const update = (key: Value, newValue: Value, oldValue: Value, headers: RecordHeaders = {}) =>
		harness.process('table', { key, value: { newValue, oldValue }, timestamp: 7, headers })
	const forwarded = () => out.records.map(r => [r.key, r.value.newValue, r.value.oldValue])
	return { harness, out, update, forwarded }
}

/** Groups by the first letter. */
const byLetter = (_key: string, value: string): KeyValue<string, string> => [value.slice(0, 1), value]

describe('KTable.groupBy repartition map (KS KTableRepartitionMap, 6.15)', () => {
	it('KIP-904: an update that stays in its group is one Change with both values, at its timestamp and headers', async () => {
		const { out, update, forwarded } = await repartitionMap(byLetter, codec.string())
		await update('row', 'A2', 'A1', trace)
		expect(forwarded()).toEqual([['A', 'A2', 'A1']])
		expect(out.records.map(r => [r.timestamp, r.headers])).toEqual([[7, trace]])
	})

	it('an update that changes group leaves the old group first, then joins the new one', async () => {
		const { out, update, forwarded } = await repartitionMap(byLetter, codec.string())
		await update('row', 'B1', 'A1', trace)
		expect(forwarded()).toEqual([
			['A', null, 'A1'],
			['B', 'B1', null],
		])
		expect(out.records.map(r => [r.timestamp, r.headers])).toEqual([
			[7, trace],
			[7, trace],
		])
	})

	it('a new row only joins its group, a deleted row only leaves it', async () => {
		const { update, forwarded } = await repartitionMap(byLetter, codec.string())
		await update('row', 'A1', null)
		await update('row', null, 'A1')
		expect(forwarded()).toEqual([
			['A', 'A1', null],
			['A', null, 'A1'],
		])
	})

	it('compares groups by their encoded keys: equal objects are the same group', async () => {
		const selector = (_key: string, value: string): KeyValue<{ letter: string }, string> => [
			{ letter: value.slice(0, 1) },
			value,
		]
		const { update, forwarded } = await repartitionMap(selector, codec.json<{ letter: string }>())
		await update('row', 'A2', 'A1')
		expect(forwarded()).toEqual([[{ letter: 'A' }, 'A2', 'A1']])
	})

	it('B19: a mapped pair with a null key or value, or no pair, is skipped; the other pair still counts', async () => {
		const invalid: Record<string, unknown> = { 'no-key': [null, 'v'], 'no-value': ['A', null], 'no-pair': null }
		const selector = (key: string, value: string) =>
			(value in invalid ? invalid[value] : byLetter(key, value)) as KeyValue<string, string>
		const { update, forwarded } = await repartitionMap(selector, codec.string())
		await update('row', 'no-key', 'A1')
		await update('row', 'B1', 'no-value')
		await update('row', 'no-pair', 'no-key')
		expect(forwarded()).toEqual([
			['A', null, 'A1'],
			['B', 'B1', null],
		])
	})

	it('an update without a key is dropped and counted', async () => {
		const { harness, update, forwarded } = await repartitionMap(byLetter, codec.string())
		await update(null, 'A1', null)
		expect(forwarded()).toEqual([])
		expect(harness.task.droppedCount('null-key')).toBe(1)
	})

	it('the new group is forwarded only once the old group’s record was processed downstream', async () => {
		const order: string[] = []
		const slow = async (record: StreamRecord<string, Change<string>>) => {
			order.push(`start ${record.key}`)
			await new Promise(resolve => setTimeout(resolve, 5))
			order.push(`end ${record.key}`)
		}
		const { update } = await repartitionMap(byLetter, codec.string(), slow)
		await update('row', 'B1', 'A1')
		expect(order).toEqual(['start A', 'end A', 'start B', 'end B'])
	})
})
