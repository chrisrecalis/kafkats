import { describe, expect, it, vi } from 'vitest'
import type { KTable } from '@/api/ktable.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import type { KeyValueStore, ValueAndTimestamp } from '@/state.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { oldValues, updatesOf, type Update } from '../support/table-updates.js'

const str = codec.string()
const S = { key: str, value: str }
const joiner = (a: string | null, b: string | null) => `${a}+${b}`

type JoinKind = 'join' | 'leftJoin' | 'outerJoin'
/** The updates a join result forwards. */
type Results = Update<string, string>[]

/** Sends a string value with a string key at a timestamp; null is a tombstone. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp: number) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { key: Buffer.from(k), timestamp })
}

function joinTables(left: KTable<string, string>, kind: JoinKind, right: KTable<string, string>) {
	return kind === 'join'
		? left.join(right, joiner)
		: kind === 'leftJoin'
			? left.leftJoin(right, joiner)
			: left.outerJoin(right, joiner)
}

/** `a.<kind>(b)` of two string tables, with the updates of the result (and its old values if asked for). */
function joinAB(driver: TestDriver, kind: JoinKind, sendOldValues = false) {
	const a = driver.table('a', S)
	const b = driver.table('b', S)
	const joined = joinTables(a, kind, b)
	if (sendOldValues) {
		;(joined as KTableImpl<string, string>).table.enableSendingOldValues(true)
	}
	return { joined, updates: updatesOf(joined) }
}

/** Updates of the tables 'a' and 'b': `[topic, key, value, timestamp]`. */
type Input = [topic: 'a' | 'b', key: string, value: string | null, timestamp: number]

async function sendAll(ctx: TestContextApi, inputs: readonly Input[]): Promise<void> {
	for (const [topic, k, value, timestamp] of inputs) {
		await send(ctx, topic, k, value, timestamp)
	}
}

describe.each(GUARANTEES)('table-table join results (ksdsl§10.1; %s)', (_guarantee, config) => {
	it.each<[JoinKind, Input[], Results]>([
		[
			'join',
			[
				['a', 'k', 'x', 10], // no b value: nothing
				['b', 'k', 'y', 5],
				['a', 'k', 'x2', 7],
				['b', 'k', null, 20], // a has k: a tombstone
				['a', 'k', null, 30], // b has no k: nothing
				['b', 'j', 'z', 1], // a has no j: nothing
				['a', 'j', null, 2], // b has j: a tombstone, though a never had j (no old values)
			],
			[
				['k', 'x+y', null, 10],
				['k', 'x2+y', null, 7],
				['k', null, null, 20],
				['j', null, null, 2],
			],
		],
		[
			'leftJoin',
			[
				['a', 'k', 'x', 10],
				['b', 'k', 'y', 5],
				['b', 'k', null, 20], // the left value remains: joined with null, not a tombstone
				['a', 'k', null, 30], // a result existed (its old value tells): a tombstone
				['a', 'k', null, 31], // none existed: nothing
				['b', 'j', 'z', 1], // no left value: nothing
				['b', 'j', null, 2],
			],
			[
				['k', 'x+null', null, 10],
				['k', 'x+y', null, 10],
				['k', 'x+null', null, 20],
				['k', null, null, 30],
			],
		],
		[
			'outerJoin',
			[
				['a', 'k', 'x', 10],
				['b', 'k', 'y', 5],
				['a', 'k', null, 20], // the right value remains
				['b', 'k', null, 30], // the last value of k goes: a tombstone at the update's timestamp
				['b', 'k', null, 31], // nothing existed: nothing
				['a', 'j', null, 1],
				['b', 'j', 'z', 2],
			],
			[
				['k', 'x+null', null, 10],
				['k', 'x+y', null, 10],
				['k', 'null+y', null, 20],
				['k', null, null, 30],
				['j', 'null+z', null, 2],
			],
		],
	])(
		'%s: results at the later timestamp of both values, deletes per Kafka Streams',
		async (kind, inputs, expected) => {
			const driver = new TestDriver(config)
			const { joined, updates } = joinAB(driver, kind)
			joined.toStream().to('out', S)
			await driver.run(ctx => sendAll(ctx, inputs))
			expect(updates).toEqual(expected)
		}
	)

	it.each<[JoinKind, Results]>([
		[
			'join',
			[
				['k', 'x+y', null, 2],
				['k', 'x2+y', 'x+y', 3],
				['k', null, 'x2+y', 4],
			],
		],
		[
			'leftJoin',
			[
				['k', 'x+null', null, 1],
				['k', 'x+y', 'x+null', 2],
				['k', 'x2+y', 'x+y', 3],
				['k', 'x2+null', 'x2+y', 4],
			],
		],
		[
			'outerJoin',
			[
				['k', 'x+null', null, 1],
				['k', 'x+y', 'x+null', 2],
				['k', 'x2+y', 'x+y', 3],
				['k', 'x2+null', 'x2+y', 4],
			],
		],
	])('%s: old results join the old value with the other side, once requested', async (kind, expected) => {
		const driver = new TestDriver(config)
		const { updates } = joinAB(driver, kind, true)
		await driver.run(ctx =>
			sendAll(ctx, [
				['a', 'k', 'x', 1],
				['b', 'k', 'y', 2],
				['a', 'k', 'x2', 3],
				['b', 'k', null, 4],
			])
		)
		expect(updates).toEqual(expected)
	})

	it.each<[JoinKind, Results]>([
		['join', [['k', 'v:x+y', null, 20]]],
		[
			'leftJoin',
			[
				['k', 'v:x+y', null, 20],
				['m', 'v:x+null', null, 1],
			],
		],
		[
			'outerJoin',
			[
				['k', 'v:x+y', null, 20],
				['m', 'v:x+null', null, 1],
				['n', 'v:null+y', null, 3],
			],
		],
	])(
		'%s: a result that is not materialized is looked up by joining both sides (ksdsl§10.3)',
		async (kind, expected) => {
			const driver = new TestDriver(config)
			const { joined } = joinAB(driver, kind)
			// Updates of 's' at 0 read the result through its getter: they take the max timestamp of the values it joins.
			const lookups = updatesOf(driver.table('s', S).join(joined, (v, ab) => `${v}:${ab}`))
			await driver.run(async ctx => {
				await sendAll(ctx, [
					['a', 'k', 'x', 10],
					['b', 'k', 'y', 20],
					['a', 'm', 'x', 1],
					['b', 'n', 'y', 3],
				])
				for (const k of ['k', 'm', 'n', 'z']) {
					await send(ctx, 's', k, 'v', 0)
				}
			})
			expect(lookups).toEqual(expected)
		}
	)
})

describe('table-table joins', () => {
	it('B18: a materialized result is stored at its timestamp, deleted by a null result and logged', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.join(driver.table('b', S), (x, y) => (x === 'none' ? (null as unknown as string) : `${x}+${y}`), {
				materialized: { storeName: 'ab', value: str },
			})
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			const stored = () => ctx.getStateStore<KeyValueStore<string, ValueAndTimestamp<string>>>('ab').get('k')
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 5)
			expect(await stored()).toEqual({ value: 'x+y', timestamp: 5 })
			await send(ctx, 'a', 'k', 'none', 7)
			expect(await stored()).toBeUndefined()
			expect(ctx.output('out', S).map(o => o.value)).toEqual(['x+y', null])
		})
		expect([...internals(driver.flow).build().internalTopics.keys()]).toContain('test-app-ab-changelog')
	})

	it.each<[JoinKind, boolean, boolean]>([
		['join', false, false],
		['leftJoin', true, false],
		['outerJoin', true, true],
	])('KF19: %s asks old values of the left table: %s, of the right table: %s', async (kind, left, right) => {
		const driver = new TestDriver()
		const a = driver.table('a', S)
		const b = driver.table('b', S)
		const aUpdates = updatesOf(a)
		const bUpdates = updatesOf(b)
		joinTables(a, kind, b)
		await driver.run(ctx =>
			sendAll(ctx, [
				['a', 'k', 'x', 1],
				['a', 'k', 'x2', 2],
				['b', 'k', 'y', 3],
				['b', 'k', 'y2', 4],
			])
		)
		expect(oldValues(aUpdates)).toEqual([null, left ? 'x' : null])
		expect(oldValues(bUpdates)).toEqual([null, right ? 'y' : null])
	})

	it('KF19: old values asked of a result are forced on both inputs', async () => {
		const driver = new TestDriver()
		const a = driver.table('a', S)
		const b = driver.table('b', S)
		const aUpdates = updatesOf(a)
		const bUpdates = updatesOf(b)
		const joined = a.join(b, joiner) as KTableImpl<string, string>
		expect(joined.table.enableSendingOldValues(false)).toBe(true)
		await driver.run(ctx =>
			sendAll(ctx, [
				['a', 'k', 'x', 1],
				['a', 'k', 'x2', 2],
				['b', 'k', 'y', 3],
				['b', 'k', 'y2', 4],
			])
		)
		expect(oldValues(aUpdates)).toEqual([null, 'x'])
		expect(oldValues(bUpdates)).toEqual([null, 'y'])
	})

	it('KF19: a groupBy of a join result moves rows between groups (join -> groupBy)', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.join(driver.table('b', S), (group, label) => `${group}:${label}`)
			.groupBy((k, v) => [v.split(':')[0]!, k] as [string, string], S)
			.count()
			.toStream()
			.mapValues(count => String(count))
			.to('out', S)
		await driver.run(async ctx => {
			for (const k of ['k1', 'k2', 'k3']) {
				await send(ctx, 'b', k, 'B', 1)
			}
			await send(ctx, 'a', 'k1', 'g1', 2)
			await send(ctx, 'a', 'k2', 'g1', 3)
			await send(ctx, 'a', 'k3', 'g2', 4)
			await send(ctx, 'a', 'k1', 'g2', 5) // g1 = 1, g2 = 2
			await send(ctx, 'b', 'k2', 'C', 6) // stays in g1
			await send(ctx, 'a', 'k3', 'g1', 7) // g1 = 2, g2 = 1
			const last = new Map(ctx.output('out', S).map(o => [o.key, o.value]))
			expect(Object.fromEntries(last)).toEqual({ g1: '2', g2: '1' })
		})
	})

	it('accepts the deprecated Joined options for their name only, with a warning', () => {
		const warn = vi.spyOn(process, 'emitWarning').mockImplementation(() => undefined)
		try {
			const driver = new TestDriver()
			const a = driver.table('a', S)
			a.join(driver.table('b', S), joiner, { name: 'plain' })
			expect(warn).not.toHaveBeenCalled()
			a.leftJoin(driver.table('c', S), joiner, { name: 'legacy', changelog: false })
			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining(
					'KTable.join(other, joiner, Joined) is deprecated: pass { name, materialized }'
				),
				'DeprecationWarning'
			)
			expect(internals(driver.flow).build().internalTopics.size).toBe(3)
		} finally {
			warn.mockRestore()
		}
	})
})
