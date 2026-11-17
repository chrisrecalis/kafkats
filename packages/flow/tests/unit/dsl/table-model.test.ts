import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { ktableFilter } from '@/operators/ktable-filter.js'
import { KTableMapValues } from '@/operators/ktable-map-values.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { oldValues, updatesOf } from '../support/table-updates.js'

const str = codec.string()
const S = { key: str, value: str }
const joiner = (a: string | null, b: string | null) => `${a}+${b}`

/** Sends a string value with a string key at a timestamp; null is a tombstone. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp = 0) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { key: Buffer.from(k), timestamp })
}

/** Changes without their timestamps. */
const changes = <V>(updates: readonly [string, V | null, V | null, number][]) =>
	updates.map(([k, newValue, oldValue]) => [k, newValue, oldValue])

/** A parent table that cannot send old values, recording the `force` flag of every request. */
function parentWithoutOldValues(requests: boolean[]): KTableNode<string, string> {
	return {
		supplier: () => ({ process: () => undefined }),
		enableSendingOldValues: force => {
			requests.push(force)
			return false
		},
		valueGetter: { storeNames: [], create: () => ({ init: () => undefined, get: async () => null }) },
	}
}

describe('KF19: the old-value propagation contract of enableSendingOldValues (6.11)', () => {
	it('a source table and an aggregation send old values from their own stores', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', S) as KTableImpl<string, string>
		const counts = driver.input('c', S).groupByKey().count() as KTableImpl<string, number>
		expect(table.table.enableSendingOldValues(false)).toBe(true)
		expect(counts.table.enableSendingOldValues(false)).toBe(true)
		const tableUpdates = updatesOf(table)
		const countUpdates = updatesOf(counts)
		await driver.run(async ctx => {
			for (const value of ['a', 'b', null]) {
				await send(ctx, 't', 'k', value)
			}
			await send(ctx, 'c', 'k', 'x')
			await send(ctx, 'c', 'k', 'y')
		})
		expect(changes(tableUpdates)).toEqual([
			['k', 'a', null],
			['k', 'b', 'a'],
			['k', null, 'b'],
		])
		expect(oldValues(countUpdates)).toEqual([null, 1])
	})

	it('a materialized filter or mapValues sends old values from its own store and asks its parent for none', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		const tableUpdates = updatesOf(table)
		const filtered = table.filter((_k, v) => v !== 'b', { materialized: { storeName: 'kept' } }) as KTableImpl<
			string,
			string
		>
		const mapped = table.mapValues(v => v.toUpperCase(), {
			materialized: { storeName: 'upper', value: str },
		}) as KTableImpl<string, string>
		expect(filtered.table.enableSendingOldValues(true)).toBe(true)
		expect(mapped.table.enableSendingOldValues(true)).toBe(true)
		const filteredUpdates = updatesOf(filtered)
		const mappedUpdates = updatesOf(mapped)
		await driver.run(async ctx => {
			for (const value of ['a', 'b', 'c']) {
				await send(ctx, 't', 'k', value)
			}
		})
		expect(oldValues(tableUpdates)).toEqual([null, null, null])
		expect(changes(filteredUpdates)).toEqual([
			['k', 'a', null],
			['k', null, 'a'],
			['k', 'c', null],
		])
		expect(changes(mappedUpdates)).toEqual([
			['k', 'A', null],
			['k', 'B', 'A'],
			['k', 'C', 'B'],
		])
	})

	it('a filter or mapValues that is not materialized passes the request on and answers with its parent', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		const tableUpdates = updatesOf(table)
		const mapped = table.mapValues(v => v.toUpperCase()) as KTableImpl<string, string>
		expect(mapped.table.enableSendingOldValues(true)).toBe(true)
		const mappedUpdates = updatesOf(mapped)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a')
			await send(ctx, 't', 'k', 'b')
		})
		expect(oldValues(tableUpdates)).toEqual([null, 'a'])
		expect(changes(mappedUpdates)).toEqual([
			['k', 'A', null],
			['k', 'B', 'A'],
		])

		// A parent that cannot send them: the answer is no, and the flag is passed on as given.
		const requests: boolean[] = []
		const withoutOldValues = new KTableMapValues(parentWithoutOldValues(requests), (_k, v: string) => v)
		expect(withoutOldValues.enableSendingOldValues(true)).toBe(false)
		expect(withoutOldValues.enableSendingOldValues(false)).toBe(false)
		expect(requests).toEqual([true, false])
	})

	it('a filter asks its parent for old values without forcing it, at build time', () => {
		const requests: boolean[] = []
		const parent = parentWithoutOldValues(requests)
		ktableFilter(parent, () => true, false)
		ktableFilter(parent, () => true, true, 'materialized')
		expect(requests).toEqual([false])
	})
})

describe.each(GUARANTEES)('W20: derived tables are join sides (6.11; %s)', (_guarantee, config) => {
	it('a filtered and a mapped table join; updates of either side read the other through its getter', async () => {
		const driver = new TestDriver(config)
		const kept = driver.table('t', S).filter((_k, v) => v !== 'hidden')
		const upper = driver.table('u', S).mapValues(v => v.toUpperCase())
		kept.outerJoin(upper, joiner).toStream().to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 1)
			await send(ctx, 'u', 'k', 'w', 2)
			await send(ctx, 't', 'k', 'hidden', 3)
			await send(ctx, 'u', 'k', 'x', 4)
			await send(ctx, 't', 'k', 'v2', 5)
			expect(ctx.producer.messagesFor('out').map(m => `${m.value?.toString()}@${m.timestamp}`)).toEqual([
				'v+null@1',
				'v+W@2',
				'null+W@3',
				'null+X@4',
				'v2+X@5',
			])
		})
	})

	it('an aggregation and a materialized mapValues are join sides read from their stores', async () => {
		const driver = new TestDriver(config)
		const counts = driver
			.input('c', S)
			.groupByKey()
			.count()
			.mapValues(count => String(count))
		const labels = driver
			.table('t', S)
			.mapValues(v => `<${v}>`, { materialized: { storeName: 'labels', value: str } })
		labels.leftJoin(counts, joiner).toStream().to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 1)
			await send(ctx, 'c', 'k', 'x', 2)
			await send(ctx, 'c', 'k', 'y', 3)
			await send(ctx, 't', 'k', 'w', 4)
			expect(ctx.output('out', S).map(o => o.value)).toEqual(['<v>+null', '<v>+1', '<v>+2', '<w>+2'])
		})
	})
})
