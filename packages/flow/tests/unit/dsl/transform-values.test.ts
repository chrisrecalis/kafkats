import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { codec } from '@/codec.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import type { ForwardingDisabledContext, ValueTransformerWithKeySupplier } from '@/processor/api.js'
import type { TimestampedKeyValueStore } from '@/state.js'
import { Stores } from '@/state/store-builders.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { oldValues, updatesOf } from '../support/table-updates.js'

const str = codec.string()
const S = { key: str, value: str }

/** Sends a string value with a string key at a timestamp; null is a tombstone. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp = 0) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { key: Buffer.from(k), timestamp })
}

const records = (ctx: TestContextApi, topic = 'out') => ctx.output(topic, S).map(r => [r.key, r.value])

/** Changes without their timestamps. */
const changes = <V>(updates: readonly [string, V | null, V | null, number][]) =>
	updates.map(([k, newValue, oldValue]) => [k, newValue, oldValue])

/** Upper-cases values, counting its calls. */
function upperCase(calls: (string | null)[]): ValueTransformerWithKeySupplier<string, string, string> {
	return () => ({
		transform: (_key, value) => {
			calls.push(value)
			return value === null ? null : value.toUpperCase()
		},
	})
}

describe.each(GUARANTEES)('KTable.transformValues (6.12, KS KTableTransformValues; %s)', (_guarantee, config) => {
	it('transforms every update with its key, tombstones included; a null result deletes the key', async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.transformValues<string>(() => ({
				transform: (key, value) =>
					value === null ? `${key} deleted` : value === 'drop' ? null : `${key}=${value}`,
			}))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await ctx.send('t', Buffer.from('x'), {
				key: Buffer.from('a'),
				timestamp: 5,
				headers: { h: Buffer.from('1') },
			})
			await send(ctx, 't', 'a', 'drop', 6)
			await send(ctx, 't', 'a', null, 7)
			expect(records(ctx)).toEqual([
				['a', 'a=x'],
				['a', null],
				['a', 'a deleted'],
			])
			const out = ctx.producer.messagesFor('out')
			expect(out.map(m => m.timestamp)).toEqual([5n, 6n, 7n])
			expect(out[0]?.headers).toEqual({ h: Buffer.from('1') })
		})
	})

	it('the transformer cannot forward; it reads its stores through its context and schedules punctuators', async () => {
		const driver = new TestDriver(config)
		const counts = Stores.keyValueStore({ name: 'counts', key: str, value: codec.json<number>() })
		const contexts: ForwardingDisabledContext[] = []
		const punctuations: number[] = []
		driver
			.table('t', S)
			.transformValues<string>(
				() => {
					let context!: ForwardingDisabledContext
					return {
						init: c => {
							context = c
							contexts.push(c)
							c.schedule(10, 'stream-time', streamTime => void punctuations.push(streamTime))
						},
						transform: async (key, value) => {
							const store = context.getStateStore(counts)
							const count = ((await store.get(key)) ?? 0) + 1
							await store.put(key, count)
							return `${value}#${count}`
						},
					}
				},
				{ stores: [counts] }
			)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 0)
			await send(ctx, 't', 'k', 'b', 12)
			expect(records(ctx)).toEqual([
				['k', 'a#1'],
				['k', 'b#2'],
			])
			expect(punctuations).toEqual([0, 12])
		})
		expect(contexts.length).toBeGreaterThan(0)
		for (const context of contexts) {
			expect('forward' in context).toBe(false)
		}
	})

	it('a materialized result is stored with the input timestamps; it needs a value codec', async () => {
		const driver = new TestDriver(config)
		driver.table('t', S).transformValues(upperCase([]), { materialized: { storeName: 'upper', value: str } })
		expect(() =>
			driver.table('u', S).transformValues(upperCase([]), { materialized: { storeName: 'no-codec' } })
		).toThrow('transformValues() needs a value codec')
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 3)
			await send(ctx, 't', 'gone', 'v', 4)
			await send(ctx, 't', 'gone', null, 5)
			const store = ctx.getStateStore<TimestampedKeyValueStore<string, string>>('upper')
			expect(await store.get('k')).toEqual({ value: 'V', timestamp: 3 })
			expect(await store.get('gone')).toBeUndefined()
		})
	})

	it('KF9: a lookup of a result that is not materialized runs a transformer of its own over the parent value', async () => {
		const driver = new TestDriver(config)
		const calls = Stores.keyValueStore({ name: 'calls', key: str, value: codec.json<number>() })
		const transformed = driver.table('t', S).transformValues<string>(
			() => {
				let context!: ForwardingDisabledContext
				return {
					init: c => {
						context = c
					},
					transform: async (_key, value) => {
						const store = context.getStateStore(calls)
						await store.put(context.processorName, ((await store.get(context.processorName)) ?? 0) + 1)
						const topic = context.recordMetadata()?.topic ?? 'no record'
						return `${value ?? 'none'} by ${context.processorName} (${topic})`
					},
				}
			},
			{ name: 'tv', stores: [calls] }
		)
		transformed.toStream().to('table-out', S)
		driver
			.input('s', S)
			.join(transformed, (value, tableValue) => `${value} + ${tableValue}`, { name: 'enrich' })
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 's', 'k', 'early', 1)
			await send(ctx, 't', 'k', 'v', 2)
			await send(ctx, 's', 'k', 'late', 3)
			expect(records(ctx, 'table-out')).toEqual([['k', 'v by tv (t)']])
			expect(records(ctx)).toEqual([
				['k', 'early + none by enrich (no record)'],
				['k', 'late + v by enrich (no record)'],
			])
			const store = ctx.getStateStore(calls)
			expect([await store.get('tv'), await store.get('enrich')]).toEqual([1, 2])
		})
	})

	it('KF9: such a lookup has the timestamp of the parent value (KS KTableTransformValuesGetter)', async () => {
		const driver = new TestDriver(config)
		const upper = driver.table('r', S).transformValues(upperCase([]))
		driver
			.table('l', S)
			.join(upper, (value, other) => `${value}+${other}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'r', 'k', 'x', 200)
			await send(ctx, 'l', 'k', 'y', 100)
			expect(records(ctx)).toEqual([['k', 'y+X']])
			// A table-table join result has the max timestamp of both sides.
			expect(ctx.producer.messagesFor('out').map(m => m.timestamp)).toEqual([200n])
		})
	})

	it('KF19: old values are transformed from the parent, or read from the own store when materialized', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S)
		const tableUpdates = updatesOf(table)
		const transformedCalls: (string | null)[] = []
		const transformed = table.transformValues(upperCase(transformedCalls)) as KTableImpl<string, string>
		expect(transformed.table.enableSendingOldValues(true)).toBe(true)
		const transformedUpdates = updatesOf(transformed)
		// A transformer whose results differ per call: an old value transformed again would not be the one stored.
		const materializedCalls: (string | null)[] = []
		const materialized = table.transformValues<string>(
			() => ({
				transform: (_key, value) => `${value}#${materializedCalls.push(value)}`,
			}),
			{ materialized: { storeName: 'numbered', value: str } }
		) as KTableImpl<string, string>
		expect(materialized.table.enableSendingOldValues(true)).toBe(true)
		const materializedUpdates = updatesOf(materialized)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a')
			await send(ctx, 't', 'k', 'b')
		})
		expect(oldValues(tableUpdates)).toEqual([null, 'a'])
		expect(changes(transformedUpdates)).toEqual([
			['k', 'A', null],
			['k', 'B', 'A'],
		])
		// Each update's new and old value, tombstones included.
		expect(transformedCalls).toEqual(['a', null, 'b', 'a'])
		// The old value is the stored result; the transformer only sees new values.
		expect(changes(materializedUpdates)).toEqual([
			['k', 'a#1', null],
			['k', 'b#2', 'a#1'],
		])
		expect(materializedCalls).toEqual(['a', 'b'])
	})

	it('KF19: a groupBy downstream gets the old values it needs to retract', async () => {
		const driver = new TestDriver(config)
		driver
			.table('t', S)
			.transformValues<string>(() => ({ transform: (_key, value) => value?.split(':')[0] ?? null }))
			.groupBy((key, group) => [group, key], { key: str, value: str })
			.count()
			.toStream()
			.mapValues(count => String(count))
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a:1')
			await send(ctx, 't', 'k', 'b:2')
			expect(records(ctx)).toEqual([
				['a', '1'],
				['a', '0'],
				['b', '1'],
			])
		})
	})
})

describe('KTable.transformValues registration', () => {
	it('SI9: a supplier returning the same transformer twice is a TopologyError; stores must be registered', () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		const shared = { transform: () => null }
		expect(() => table.transformValues(() => shared)).toThrow(TopologyError)
		expect(() => table.transformValues(upperCase([]), { stores: ['missing'] })).toThrow(TopologyError)
	})
})
