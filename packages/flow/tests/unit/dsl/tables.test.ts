import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec, type Codec } from '@/codec.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import type { Change } from '@/operators/change.js'
import { ktableFilter } from '@/operators/ktable-filter.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import type { StreamRecord } from '@/processor/record.js'
import type { KeyValueStore, ValueAndTimestamp } from '@/state.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from '../support/processor-harness.js'

const str = codec.string()
const S = { key: str, value: str }
/** The number codec these tests pass explicitly: an 8-byte little-endian double. */
const num: Codec<number> = {
	encode: n => {
		const encoded = Buffer.alloc(8)
		encoded.writeDoubleLE(n, 0)
		return encoded
	},
	decode: encoded => encoded.readDoubleLE(0),
}
const key = (k: string) => ({ key: Buffer.from(k) })
const client = { clientId: 'app', brokers: ['localhost:9092'] }

type Stored<V> = KeyValueStore<string, ValueAndTimestamp<V>>

/** Sends a string value with a string key at a timestamp; null is a tombstone. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp = 0) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { ...key(k), timestamp })
}

const values = (ctx: TestContextApi, topic = 'out') => ctx.output(topic, S).map(o => o.value)

/** The changes a table forwards, collected by a processor under its node. */
function changesOf<K, V>(table: unknown): Change<V>[] {
	const impl = table as KTableImpl<K, V>
	const changes: Change<V>[] = []
	impl.ctx.processor('CHANGES', undefined, [impl.node], () => ({
		process: (record: StreamRecord<K, Change<V>>) => void changes.push(record.value),
	}))
	return changes
}

describe('table-table joins (copied from flow.test.ts)', () => {
	type User = { name: string }
	type Account = { balance: number }

	/** users ⋈ accounts with the given kind, every update collected as `{ key, value }`. */
	function usersJoinAccounts(kind: 'join' | 'leftJoin' | 'outerJoin') {
		const driver = new TestDriver()
		const emitted: Array<{ key: string | null; value: unknown }> = []
		const users = driver.table('users', { key: str, value: codec.json<User>() })
		const accounts = driver.table('accounts', { key: str, value: codec.json<Account>() })
		const joiner = (user: User | null, account: Account | null) =>
			kind === 'join'
				? { name: user!.name, balance: account!.balance }
				: { name: user?.name ?? null, balance: account?.balance ?? null }
		const joined =
			kind === 'join'
				? users.join(accounts, joiner)
				: kind === 'leftJoin'
					? users.leftJoin(accounts, joiner)
					: users.outerJoin(accounts, joiner)
		joined.toStream().peek((k, value) => emitted.push({ key: k, value }))
		return { driver, emitted }
	}

	it('inner joins two tables by key', async () => {
		const { driver, emitted } = usersJoinAccounts('join')
		await driver.run(async ({ send }) => {
			// Add user without account - no join result
			await send('users', { name: 'Alice' }, key('user1'))
			expect(emitted).toHaveLength(0)

			// Add account for user1 - now we get a join result
			await send('accounts', { balance: 100 }, key('user1'))
			expect(emitted).toEqual([{ key: 'user1', value: { name: 'Alice', balance: 100 } }])

			// Update user - join result with existing account
			await send('users', { name: 'Alice Updated' }, key('user1'))
			expect(emitted[1]).toEqual({ key: 'user1', value: { name: 'Alice Updated', balance: 100 } })
			expect(emitted).toHaveLength(2)
		})
	})

	it('does not emit a tombstone when deleting a key the other table never matched', async () => {
		const { driver, emitted } = usersJoinAccounts('join')
		await driver.run(async ({ send }) => {
			// Establish a real join result for user1 (both sides present).
			await send('users', { name: 'Alice' }, key('user1'))
			await send('accounts', { balance: 100 }, key('user1'))
			expect(emitted).toEqual([{ key: 'user1', value: { name: 'Alice', balance: 100 } }])

			// Deleting a user the accounts table never matched: nothing to retract.
			emitted.length = 0
			await send('users', null, key('ghost'))
			expect(emitted).toHaveLength(0)

			// Deleting user1, whose account row exists, DOES retract the prior join result.
			await send('users', null, key('user1'))
			expect(emitted).toEqual([{ key: 'user1', value: null }])
		})
	})

	it('inner join: a delete forwards a tombstone whenever the other table has the key (KS)', async () => {
		const { driver, emitted } = usersJoinAccounts('join')
		await driver.run(async ({ send }) => {
			// Accounts has 'k' but users never did: an inner join asks for no old values, so it cannot tell.
			await send('accounts', { balance: 50 }, key('k'))
			await send('users', null, key('k'))
			expect(emitted).toEqual([{ key: 'k', value: null }])

			// A real result, then a duplicate delete: each delete forwards a tombstone.
			emitted.length = 0
			await send('users', { name: 'Ann' }, key('u'))
			await send('accounts', { balance: 10 }, key('u'))
			expect(emitted).toEqual([{ key: 'u', value: { name: 'Ann', balance: 10 } }])

			emitted.length = 0
			await send('users', null, key('u'))
			await send('users', null, key('u'))
			expect(emitted).toEqual([
				{ key: 'u', value: null },
				{ key: 'u', value: null },
			])
		})
	})

	it('left join: no tombstone when the left row never existed', async () => {
		const { driver, emitted } = usersJoinAccounts('leftJoin')
		await driver.run(async ({ send }) => {
			await send('users', null, key('ghost'))
			expect(emitted).toHaveLength(0)

			// A real left row emits (left, null); deleting it retracts.
			await send('users', { name: 'Bob' }, key('b'))
			expect(emitted).toEqual([{ key: 'b', value: { name: 'Bob', balance: null } }])

			emitted.length = 0
			await send('users', null, key('b'))
			expect(emitted).toEqual([{ key: 'b', value: null }])
		})
	})

	it('outer join: no tombstone when neither side ever existed', async () => {
		const { driver, emitted } = usersJoinAccounts('outerJoin')
		await driver.run(async ({ send }) => {
			await send('users', null, key('ghost'))
			expect(emitted).toHaveLength(0)

			// A real left-only result emits; deleting the only side retracts.
			await send('users', { name: 'Cara' }, key('c'))
			expect(emitted).toEqual([{ key: 'c', value: { name: 'Cara', balance: null } }])

			emitted.length = 0
			await send('users', null, key('c'))
			expect(emitted).toEqual([{ key: 'c', value: null }])
		})
	})

	it('inner join: a right-side delete forwards a tombstone whenever the left table has the key (KS)', async () => {
		const { driver, emitted } = usersJoinAccounts('join')
		await driver.run(async ({ send }) => {
			// A users row exists but accounts never did: the delete still forwards a tombstone.
			await send('users', { name: 'Ghost' }, key('ghost'))
			await send('accounts', null, key('ghost'))
			expect(emitted).toEqual([{ key: 'ghost', value: null }])

			// Real result, then a right-side delete retracts it; a duplicate right delete forwards another tombstone.
			emitted.length = 0
			await send('users', { name: 'Ann' }, key('u'))
			await send('accounts', { balance: 10 }, key('u'))
			expect(emitted).toEqual([{ key: 'u', value: { name: 'Ann', balance: 10 } }])

			emitted.length = 0
			await send('accounts', null, key('u'))
			await send('accounts', null, key('u'))
			expect(emitted).toEqual([
				{ key: 'u', value: null },
				{ key: 'u', value: null },
			])
		})
	})

	it('outer join: right-side deletes retract precisely', async () => {
		const { driver, emitted } = usersJoinAccounts('outerJoin')
		await driver.run(async ({ send }) => {
			await send('accounts', null, key('ghost'))
			expect(emitted).toHaveLength(0)

			// A right-only result emits; deleting the only (right) side retracts.
			await send('accounts', { balance: 5 }, key('a'))
			expect(emitted).toEqual([{ key: 'a', value: { name: null, balance: 5 } }])

			emitted.length = 0
			await send('accounts', null, key('a'))
			expect(emitted).toEqual([{ key: 'a', value: null }])
		})
	})

	it('left joins tables - left side always emits', async () => {
		const { driver, emitted } = usersJoinAccounts('leftJoin')
		await driver.run(async ({ send }) => {
			// Add user without account - left join still emits
			await send('users', { name: 'Alice' }, key('user1'))
			expect(emitted).toEqual([{ key: 'user1', value: { name: 'Alice', balance: null } }])

			// Add account - join now includes balance
			await send('accounts', { balance: 100 }, key('user1'))
			expect(emitted[1]).toEqual({ key: 'user1', value: { name: 'Alice', balance: 100 } })
		})
	})

	it('outer joins tables - both sides emit', async () => {
		const { driver, emitted } = usersJoinAccounts('outerJoin')
		await driver.run(async ({ send }) => {
			await send('users', { name: 'Alice' }, key('user1'))
			expect(emitted).toEqual([{ key: 'user1', value: { name: 'Alice', balance: null } }])

			await send('accounts', { balance: 200 }, key('user2'))
			expect(emitted[1]).toEqual({ key: 'user2', value: { name: null, balance: 200 } })

			await send('users', { name: 'Bob' }, key('user2'))
			expect(emitted[2]).toEqual({ key: 'user2', value: { name: 'Bob', balance: 200 } })
			expect(emitted).toHaveLength(3)
		})
	})

	it('left join: deleting the right row re-emits joiner(left, null), not a tombstone', async () => {
		const { driver, emitted } = usersJoinAccounts('leftJoin')
		await driver.run(async ({ send }) => {
			await send('users', { name: 'Alice' }, key('user1'))
			await send('accounts', { balance: 100 }, key('user1'))
			// The left row still exists, so the result remains, re-emitted with a null balance.
			await send('accounts', null, key('user1'))
		})
		expect(emitted).toEqual([
			{ key: 'user1', value: { name: 'Alice', balance: null } },
			{ key: 'user1', value: { name: 'Alice', balance: 100 } },
			{ key: 'user1', value: { name: 'Alice', balance: null } },
		])
	})

	it('outer join: deleting one side keeps the result from the other; deleting both tombstones', async () => {
		const { driver, emitted } = usersJoinAccounts('outerJoin')
		await driver.run(async ({ send }) => {
			await send('users', { name: 'Alice' }, key('user1'))
			await send('accounts', { balance: 100 }, key('user1'))

			// Delete the LEFT row: the result remains as joiner(null, account).
			await send('users', null, key('user1'))
			expect(emitted[2]).toEqual({ key: 'user1', value: { name: null, balance: 100 } })

			// Re-add the left, then delete the RIGHT row: the result remains as joiner(user, null).
			await send('users', { name: 'Alice2' }, key('user1'))
			expect(emitted[3]).toEqual({ key: 'user1', value: { name: 'Alice2', balance: 100 } })
			await send('accounts', null, key('user1'))
			expect(emitted[4]).toEqual({ key: 'user1', value: { name: 'Alice2', balance: null } })

			// Now delete the left too: both sides gone -> a genuine tombstone.
			await send('users', null, key('user1'))
			expect(emitted[5]).toEqual({ key: 'user1', value: null })
			expect(emitted).toHaveLength(6)
		})
	})

	it('left join: right-side records/tombstones with no left row emit nothing', async () => {
		const { driver, emitted } = usersJoinAccounts('leftJoin')
		await driver.run(async ({ send }) => {
			await send('accounts', { balance: 100 }, key('user1'))
			await send('accounts', null, key('user1'))
		})
		expect(emitted).toHaveLength(0)
	})
})

describe('table aggregations (KGroupedTable) (copied from flow.test.ts)', () => {
	type Item = { category: string; name: string }
	type Order = { region: string; amount: number }
	type Stats = { total: number; count: number }
	type Priced = { category: string; price: number }

	function countByCategory() {
		const driver = new TestDriver()
		const results: Array<{ key: string | null; count: number | null }> = []
		driver
			.table('items', { key: str, value: codec.json<Item>() })
			.groupBy((_key, value) => [value.category, value] as const, { key: str })
			.count()
			.toStream()
			.peek((k, count) => results.push({ key: k, count }))
		return { driver, results }
	}

	function statsByRegion() {
		const driver = new TestDriver()
		const results: Array<{ key: string | null; stats: Stats | null }> = []
		driver
			.table('orders', { key: str, value: codec.json<Order>() })
			.groupBy((_key, value) => [value.region, value] as const, { key: str })
			.aggregate<Stats>(
				() => ({ total: 0, count: 0 }),
				(_key, value, agg) => ({ total: agg.total + value.amount, count: agg.count + 1 }),
				(_key, value, agg) => ({ total: agg.total - value.amount, count: agg.count - 1 }),
				{ value: codec.json<Stats>() }
			)
			.toStream()
			.peek((k, stats) => results.push({ key: k, stats }))
		return { driver, results }
	}

	it('counts by grouped key using table.groupBy().count()', async () => {
		const { driver, results } = countByCategory()
		await driver.run(async ({ send }) => {
			await send('items', { category: 'books', name: 'Book 1' }, key('item1'))
			await send('items', { category: 'electronics', name: 'Phone' }, key('item2'))
			await send('items', { category: 'books', name: 'Book 2' }, key('item3'))
		})
		expect(results).toEqual([
			{ key: 'books', count: 1 },
			{ key: 'electronics', count: 1 },
			{ key: 'books', count: 2 },
		])
	})

	it('aggregates by grouped key using table.groupBy().aggregate()', async () => {
		const { driver, results } = statsByRegion()
		await driver.run(async ({ send }) => {
			await send('orders', { region: 'US', amount: 100 }, key('o1'))
			await send('orders', { region: 'EU', amount: 200 }, key('o2'))
			await send('orders', { region: 'US', amount: 150 }, key('o3'))
		})
		expect(results).toEqual([
			{ key: 'US', stats: { total: 100, count: 1 } },
			{ key: 'EU', stats: { total: 200, count: 1 } },
			{ key: 'US', stats: { total: 250, count: 2 } },
		])
	})

	it('aggregate keeps the aggregate of a group that becomes empty (KS, BC11): no tombstone', async () => {
		const { driver, results } = statsByRegion()
		await driver.run(async ({ send }) => {
			await send('orders', { region: 'US', amount: 100 }, key('o1'))
			expect(results.at(-1)).toEqual({ key: 'US', stats: { total: 100, count: 1 } })

			// The group is now empty: the subtractor's result is kept, as Kafka Streams does.
			await send('orders', null, key('o1'))
			expect(results.at(-1)).toEqual({ key: 'US', stats: { total: 0, count: 0 } })
		})
	})

	it('retracts counts when grouped key changes', async () => {
		const { driver, results } = countByCategory()
		await driver.run(async ({ send }) => {
			await send('items', { category: 'books', name: 'Book 1' }, key('item1'))
			expect(results).toEqual([{ key: 'books', count: 1 }])

			// Leaves books (count 1 -> 0, kept), then joins electronics (count -> 1).
			await send('items', { category: 'electronics', name: 'Book 1' }, key('item1'))
		})
		expect(results).toEqual([
			{ key: 'books', count: 1 },
			{ key: 'books', count: 0 },
			{ key: 'electronics', count: 1 },
		])
	})

	it('retracts counts on source tombstones', async () => {
		const { driver, results } = countByCategory()
		await driver.run(async ({ send }) => {
			await send('items', { category: 'books', name: 'Book 1' }, key('item1'))
			await send('items', { category: 'books', name: 'Book 2' }, key('item2'))
			expect(results.filter(r => r.key === 'books' && r.count === 2)).toHaveLength(1)

			// Delete item1 (tombstone) - count should go from 2 to 1
			await send('items', null, key('item1'))
		})
		expect(results.filter(r => r.key === 'books').at(-1)).toEqual({ key: 'books', count: 1 })
	})

	it('retracts aggregate values when grouped key changes', async () => {
		const { driver, results } = statsByRegion()
		await driver.run(async ({ send }) => {
			await send('orders', { region: 'US', amount: 100 }, key('o1'))
			expect(results.at(-1)).toEqual({ key: 'US', stats: { total: 100, count: 1 } })

			// Move order o1 from US to EU: US keeps its emptied aggregate, EU gets $100.
			await send('orders', { region: 'EU', amount: 100 }, key('o1'))
		})
		expect(results.slice(1)).toEqual([
			{ key: 'US', stats: { total: 0, count: 0 } },
			{ key: 'EU', stats: { total: 100, count: 1 } },
		])
	})

	it('reduces with adder and subtractor for table groupBy', async () => {
		const driver = new TestDriver()
		const totals: number[] = []
		driver
			.table('items', { key: str, value: codec.json<Priced>() })
			.groupBy((_key, value) => [value.category, value] as const, { key: str, value: codec.json<Priced>() })
			.reduce(
				(agg, value) => ({ ...agg, price: agg.price + value.price }),
				(agg, value) => ({ ...agg, price: agg.price - value.price })
			)
			.toStream()
			.peek((_k, total) => total !== null && totals.push(total.price))
		await driver.run(async ({ send }) => {
			await send('items', { category: 'books', price: 10 }, key('item1'))
			await send('items', { category: 'books', price: 20 }, key('item2'))
			expect(totals.at(-1)).toBe(30)

			// sub(30, 10) -> 20, add(20, 15) -> 35
			await send('items', { category: 'books', price: 15 }, key('item1'))
			expect(totals.at(-1)).toBe(35)
		})
	})

	it('a null result of an exactly-once table groupBy reduce deletes the group with a tombstone', async () => {
		const driver = new TestDriver({ processingGuarantee: 'exactly_once' })
		const emitted: Array<{ key: string | null; value: Priced | null }> = []
		driver
			.table('items', { key: str, value: codec.json<Priced>() })
			.groupBy((_key, value) => [value.category, value] as const, { key: str, value: codec.json<Priced>() })
			.reduce(
				(agg, value) => ({ ...agg, price: agg.price + value.price }),
				// Only a null result deletes a group's aggregate (KS); an empty group keeps its aggregate.
				(agg, value) => {
					const price = agg.price - value.price
					return price === 0 ? (null as unknown as Priced) : { ...agg, price }
				}
			)
			.toStream()
			.peek((k, value) => emitted.push({ key: k, value }))
		await driver.run(async ({ send }) => {
			await send('items', { category: 'books', price: 10 }, key('item1'))
			expect(emitted.at(-1)).toEqual({ key: 'books', value: { category: 'books', price: 10 } })

			// Delete item1 (source tombstone) -> subtractor returns null -> delete and tombstone.
			await send('items', null, key('item1'))
			expect(emitted.at(-1)).toEqual({ key: 'books', value: null })

			// Re-add to 'books': must restart from the new value, not aggregate onto a stale null.
			await send('items', { category: 'books', price: 5 }, key('item2'))
			expect(emitted.at(-1)).toEqual({ key: 'books', value: { category: 'books', price: 5 } })
		})
	})
})

describe('tables (copied from operator-semantics*.test.ts)', () => {
	it('KTable.filter emits a tombstone when a previously-passing key stops passing', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.filter((_k, v) => v === 'keep')
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'keep', 1)
			await send(ctx, 't', 'k', 'drop', 2)
			expect(values(ctx)).toEqual(['keep', null])
		})
	})

	it('KTable.filter does not invoke the predicate with a null value (tombstone)', async () => {
		const driver = new TestDriver()
		const seen: Array<string | null> = []
		driver
			.table('t', S)
			.filter((_k, v) => {
				seen.push(v)
				return v !== null && v.length > 0
			})
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 1)
			await send(ctx, 't', 'k', null, 2)
			expect(seen).not.toContain(null)
		})
	})

	it('KTable.filter then groupBy().count() un-counts rows that stop passing the filter', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.filter((_k, v) => v === 'keep')
			.groupBy((_k, v) => [v, v] as [string, string], S)
			.count()
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'keep', 1)
			await send(ctx, 't', 'k', 'drop', 2)
			// The emptied group keeps its count of 0 (KS, BC11).
			expect(ctx.output('out', { key: str, value: num }).map(o => o.value)).toEqual([1, 0])
		})
	})

	it('KTable.mapValues does not turn a tombstone into a non-null record', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.mapValues(v => (v === null ? 'NULLMAPPED' : v.toUpperCase()))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 1)
			await send(ctx, 't', 'k', null, 2)
			expect(values(ctx)).toEqual(['A', null])
		})
	})

	it('filtered table can be joined with another table (un-skipped: every table has a value getter)', async () => {
		const driver = new TestDriver()
		const a = driver.table('a', S).filter((_k, v) => v !== null)
		const b = driver.table('b', S)
		a.join(b, (x, y) => `${x}+${y}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			expect(values(ctx)).toEqual(['x+y'])
		})
	})

	it('inner table-table join retracts on right tombstone and on update', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.join(driver.table('b', S), (x, y) => `${x}+${y}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			await send(ctx, 'b', 'k', 'z', 3)
			await send(ctx, 'b', 'k', null, 4)
			await send(ctx, 'a', 'k', 'x2', 5)
			expect(values(ctx)).toEqual(['x+y', 'x+z', null])
		})
	})

	it('table-table outer join', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.outerJoin(driver.table('b', S), (x, y) => `${x}|${y}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			await send(ctx, 'a', 'k', null, 3)
			await send(ctx, 'b', 'k', null, 4)
			expect(values(ctx)).toEqual(['x|null', 'x|y', 'null|y', null])
		})
	})

	it('KGroupedTable count: key change moves between groups, tombstone subtracts', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.groupBy((_k, v) => [v, v] as [string, string], S)
			.count()
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k1', 'A', 1)
			await send(ctx, 't', 'k2', 'A', 2)
			await send(ctx, 't', 'k1', 'B', 3)
			await send(ctx, 't', 'k2', null, 4)
			const out = ctx.output('out', { key: str, value: num }).map(o => [o.key, o.value])
			const last = (k: string) => out.filter(o => o[0] === k).pop()?.[1]
			expect(last('A')).toBe(0)
			expect(last('B')).toBe(1)
		})
	})

	it('KGroupedTable reduce non-commutative / non-inverse subtractor uses adder+subtractor', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.groupBy((_k, v) => ['g', v] as [string, string], S)
			// adder concatenates; subtractor removes the value
			.reduce(
				(agg, v) => agg + v,
				(agg, v) => agg.replace(v, ''),
				{ key: str, value: str }
			)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k1', 'a', 1)
			await send(ctx, 't', 'k2', 'b', 2)
			await send(ctx, 't', 'k1', 'c', 3) // k1: a -> c
			// Kafka: adder(sub(agg,a), c): "ab" -> "b" -> "bc"
			expect(values(ctx).at(-1)).toBe('bc')
		})
	})
})

describe('table filter, filterNot and mapValues', () => {
	it('filterNot keeps the values the predicate rejects; mapValuesWithKey gets the key; null deletes', async () => {
		const driver = new TestDriver()
		const t = driver.table('t', S)
		t.filterNot((_k, v) => v.startsWith('x'))
			.toStream()
			.to('kept', S)
		t.mapValuesWithKey((k, v) => (v === 'gone' ? null : `${k}:${v}`))
			.toStream()
			.to('mapped', S)
		// A function declaring more parameters gets the value alone (0.13 called mapValues so too).
		t.mapValues(JSON.stringify).toStream().to('stringified', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'a', 'x1')
			await send(ctx, 't', 'a', 'v')
			await send(ctx, 't', 'a', 'gone')
			expect(values(ctx, 'kept')).toEqual(['v', 'gone'])
			expect(values(ctx, 'mapped')).toEqual(['a:x1', 'a:v', null])
			expect(values(ctx, 'stringified')).toEqual(['"x1"', '"v"', '"gone"'])
		})
	})

	it('asks its parent for old values: no tombstone for a key that never matched, one when it stops matching', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.filter((_k, v) => v.startsWith('keep'))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'drop')
			await send(ctx, 't', 'k', 'keep')
			await send(ctx, 't', 'k', 'keep2')
			await send(ctx, 't', 'k', 'drop')
			await send(ctx, 't', 'k', 'drop2')
			await send(ctx, 't', 'k', null)
			expect(values(ctx)).toEqual(['keep', 'keep2', null])
		})
	})

	it('sends old values on request: mapped from the parent, filtered through the predicate', async () => {
		const driver = new TestDriver()
		const mapped = driver.table('t', S).mapValues(v => v.toUpperCase()) as KTableImpl<string, string>
		const filtered = mapped.filter((_k, v) => v !== 'B') as KTableImpl<string, string>
		expect(mapped.table.enableSendingOldValues(false)).toBe(true)
		const mappedChanges = changesOf<string, string>(mapped)
		const filteredChanges = changesOf<string, string>(filtered)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a')
			await send(ctx, 't', 'k', 'b')
			await send(ctx, 't', 'k', null)
		})
		expect(mappedChanges).toEqual([
			{ newValue: 'A', oldValue: null },
			{ newValue: 'B', oldValue: 'A' },
			{ newValue: null, oldValue: 'B' },
		])
		expect(filteredChanges).toEqual([
			{ newValue: 'A', oldValue: null },
			{ newValue: null, oldValue: 'A' },
		])
	})

	it('a materialized filter or mapValues stores the derived table; its Materialized codecs are the format', async () => {
		const driver = new TestDriver()
		const t = driver.table('t', S)
		t.filter((_k, v) => v !== 'no', { materialized: { storeName: 'kept' } })
			.toStream()
			.to('kept', S)
		const lengths = t.mapValues(v => v.length, {
			name: 'lengths',
			materialized: { storeName: 'lengths', value: num },
		}) as KTableImpl<string, number>
		lengths.toStream().to('lengths')
		// A materialized table sends old values from its own store.
		expect(lengths.table.enableSendingOldValues(false)).toBe(true)
		const changes = changesOf<string, number>(lengths)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'yes', 5)
			expect(await ctx.getStateStore<Stored<string>>('kept').get('k')).toEqual({ value: 'yes', timestamp: 5 })
			expect(await ctx.getStateStore<Stored<number>>('lengths').get('k')).toEqual({ value: 3, timestamp: 5 })
			await send(ctx, 't', 'k', 'no', 6)
			await send(ctx, 't', 'k', 'no', 7)
			expect(await ctx.getStateStore<Stored<string>>('kept').get('k')).toBeUndefined()
			expect(values(ctx, 'kept')).toEqual(['yes', null])
			expect(ctx.output('lengths', { key: str, value: num }).map(o => o.value)).toEqual([3, 2, 2])
		})
		expect(changes).toEqual([
			{ newValue: 3, oldValue: null },
			{ newValue: 2, oldValue: 3 },
			{ newValue: 2, oldValue: 2 },
		])
		const topology = internals(driver.flow).build()
		expect(topology.subtopologyOf('t')!.nodes.map(n => n.name)).toContain('lengths')
		expect([...topology.internalTopics.keys()]).toEqual(
			expect.arrayContaining(['test-app-kept-changelog', 'test-app-lengths-changelog'])
		)
	})

	it('a materialized mapValues needs a value codec for its store: a TopologyError names the operator', () => {
		const driver = new TestDriver()
		const t = driver.table('t', S)
		expect(() => t.mapValues(v => v.length, { materialized: { storeName: 'lengths' } })).toThrow(TopologyError)
		expect(() => t.mapValues(v => v.length, { materialized: { storeName: 'lengths' } })).toThrow(
			'mapValues() needs a value codec'
		)
	})

	it('without old values from its parent, a filter forwards a tombstone for every update it rejects (KS)', async () => {
		const parent: KTableNode<string, string> = {
			supplier: () => ({ process: () => undefined }),
			enableSendingOldValues: () => false,
			valueGetter: { storeNames: [], create: () => ({ init: () => undefined, get: async () => null }) },
		}
		const filter = ktableFilter(parent, (_k, v) => v === 'keep', false)
		const out = capture<string, Change<string>>('out', ['filter'])
		const harness = await ProcessorHarness.start([
			sourceDef('source', ['t']),
			processorDef('filter', ['source'], filter.supplier),
			out.def,
		])
		for (const newValue of ['drop', 'keep', 'drop', null]) {
			await harness.process('t', { key: 'k', value: { newValue, oldValue: null }, timestamp: 0 })
		}
		expect(out.records.map(r => r.value.newValue)).toEqual([null, 'keep', null, null])
	})

	it('a derived table is a join side: its value getter recomputes from the parent', async () => {
		const driver = new TestDriver()
		const derived = driver
			.table('t', S)
			.filter((_k, v) => v !== 'hidden')
			.mapValues(v => v.toUpperCase())
		// Updates of 's' read the derived table through its getter.
		driver
			.table('s', S)
			.leftJoin(derived, (v, tv) => `${v}+${tv}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v')
			await send(ctx, 's', 'k', 'a')
			await send(ctx, 't', 'k', 'hidden')
			await send(ctx, 's', 'k', 'b')
			expect(values(ctx)).toEqual(['a+V', 'a+null', 'b+null'])
		})
	})
})

describe('table-table joins', () => {
	/** Every record a table forwards. */
	function recordsOf<K, V>(table: unknown): StreamRecord<K, Change<V>>[] {
		const impl = table as KTableImpl<K, V>
		const records: StreamRecord<K, Change<V>>[] = []
		impl.ctx.processor('RECORDS', undefined, [impl.node], () => ({
			process: (record: StreamRecord<K, Change<V>>) => void records.push(record),
		}))
		return records
	}

	it('results carry the max timestamp of the joined values and the headers of the update', async () => {
		const driver = new TestDriver()
		const joined = driver.table('a', S).join(driver.table('b', S), (x, y) => `${x}+${y}`)
		const records = recordsOf<string, string>(joined)
		await driver.run(async ctx => {
			await ctx.send('a', Buffer.from('x'), { ...key('k'), timestamp: 10, headers: { h: Buffer.from('a') } })
			await ctx.send('b', Buffer.from('y'), { ...key('k'), timestamp: 5, headers: { h: Buffer.from('b') } })
			await ctx.send('a', Buffer.from('x2'), { ...key('k'), timestamp: 7 })
		})
		expect(records.map(r => [r.value.newValue, r.timestamp, r.headers.h?.toString()])).toEqual([
			['x+y', 10, 'b'],
			['x2+y', 7, undefined],
		])
	})

	it('a joiner returning null deletes the result: it is forwarded as a tombstone (KS)', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.join(driver.table('b', S), (x, y) => (x === 'none' ? (null as unknown as string) : `${x}+${y}`))
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'b', 'k', 'y')
			await send(ctx, 'a', 'k', 'none')
			expect(values(ctx)).toEqual([null])
			await send(ctx, 'a', 'k', 'x')
			await send(ctx, 'a', 'k', 'none')
			expect(values(ctx)).toEqual([null, 'x+y', null])
		})
	})

	it('a self-join forwards one result per side for every update', async () => {
		const driver = new TestDriver()
		const t = driver.table('t', S)
		t.join(t, (x, y) => `${x}=${y}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v')
			expect(values(ctx)).toEqual(['v=v', 'v=v'])
		})
	})

	it('sends old results on request, joined from the old values of both sides', async () => {
		const driver = new TestDriver()
		const joined = driver.table('a', S).leftJoin(driver.table('b', S), (x, y) => `${x}+${y}`) as KTableImpl<
			string,
			string
		>
		expect(joined.table.enableSendingOldValues(false)).toBe(true)
		const changes = changesOf<string, string>(joined)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x')
			await send(ctx, 'b', 'k', 'y')
			await send(ctx, 'a', 'k', null)
		})
		expect(changes).toEqual([
			{ newValue: 'x+null', oldValue: null },
			{ newValue: 'x+y', oldValue: 'x+null' },
			{ newValue: null, oldValue: 'x+y' },
		])
	})

	it('a materialized result is stored and is a join side; a result that is not is a lookup side', async () => {
		const driver = new TestDriver()
		const ab = driver
			.table('a', S)
			.join(driver.table('b', S), (x, y) => `${x}+${y}`, { materialized: { storeName: 'ab', value: str } })
		ab.join(driver.table('c', S), (xy, z) => `${xy}+${z}`)
			.toStream()
			.to('abc', S)
		const bc = driver.table('b', { ...S, name: 'b-again' }).outerJoin(driver.table('c', S), (y, z) => `${y}|${z}`)
		// Updates of 's' read the (not materialized) outer join result through its getter.
		driver
			.table('s', S)
			.join(bc, (v, yz) => `${v}:${yz}`)
			.toStream()
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			await send(ctx, 'c', 'k', 'z', 3)
			expect(await ctx.getStateStore<Stored<string>>('ab').get('k')).toEqual({ value: 'x+y', timestamp: 2 })
			expect(values(ctx, 'abc')).toEqual(['x+y+z'])
			await send(ctx, 's', 'k', 'v')
			await send(ctx, 's', 'missing', 'w')
			expect(values(ctx)).toEqual(['v:y|z'])
		})
	})

	it('puts both tables in one subtopology (co-partitioned); a named join names its nodes', () => {
		const app = flow({ applicationId: 'app', client })
		app.table('a', S).join(app.table('b', S), (x, y) => x + y, { name: 'ab' })
		const topology = internals(app).build()
		const join = topology.subtopologyOf('a')!
		expect(join).toBe(topology.subtopologyOf('b'))
		expect(join.nodes.map(n => n.name).filter(n => n.startsWith('ab'))).toEqual([
			'ab-join-this',
			'ab-join-other',
			'ab',
		])
		expect(topology.copartitionGroups.map(group => [...group].sort())).toEqual([['a', 'b']])
	})

	it('accepts the deprecated Joined options for their name; another app’s table is a TopologyError', () => {
		const app = flow({ applicationId: 'app', client })
		const a = app.table('a', S)
		const foreign = flow({ applicationId: 'other', client }).table('x', S)
		expect(() => a.join(foreign, (x, y) => x + y)).toThrow('join() needs a table of the same app')
		a.leftJoin(app.table('b', S), (x, y) => x + y, { name: 'legacy', value: str })
		expect(
			internals(app)
				.build()
				.subtopologyOf('a')!
				.nodes.map(n => n.name)
		).toContain('legacy-join-this')
	})

	it('gives the same results under at-least-once, exactly-once per record and exactly-once on stop', async () => {
		const runs: (TestDriverConfig | undefined)[] = [
			undefined,
			{ processingGuarantee: 'exactly_once' },
			{ processingGuarantee: 'exactly_once', commitEvery: 'stop' },
		]
		const results: unknown[] = []
		for (const config of runs) {
			const driver = new TestDriver(config)
			driver
				.table('a', S)
				.outerJoin(driver.table('b', S), (x, y) => `${x}|${y}`)
				.toStream()
				.to('out', S)
			await driver.run(async ctx => {
				for (const [topic, k, v] of [
					['a', 'k', 'x'],
					['b', 'k', 'y'],
					['a', 'k', null],
					['b', 'j', 'z'],
					['b', 'k', null],
				] as const) {
					await send(ctx, topic, k, v)
				}
				results.push(values(ctx))
			})
		}
		expect(results[0]).toEqual(['x|null', 'x|y', 'null|y', 'null|z', null])
		expect(results[1]).toEqual(results[0])
		expect(results[2]).toEqual(results[0])
	})
})

describe('KTable.groupBy and KGroupedTable', () => {
	const counts = (ctx: TestContextApi) => ctx.output('out', { key: str, value: num }).map(o => [o.key, o.value])
	/** Kafka Streams' Change encoding flags (KS ChangedSerializer). */
	const [OLD_ONLY, NEW_ONLY, BOTH] = [0, 1, 2]
	const bytes = (...parts: (string | number)[]) =>
		Buffer.concat(parts.map(part => (typeof part === 'string' ? Buffer.from(part) : Buffer.of(part))))

	it('writes one Change per update in the Kafka Streams format, two when the group changes; keeps headers', async () => {
		const driver = new TestDriver()
		driver
			.table('t', S)
			.groupBy((_k, v) => [v.slice(0, 1), v] as [string, string], { ...S, name: 'by-letter' })
			.count()
			.toStream()
			.to('out', { key: str, value: num })
		const trace = (t: string) => ({ trace: Buffer.from(t) })
		await driver.run(async ctx => {
			// The n-th update carries the header trace = n.
			for (const [i, value] of ['A1', 'A2', 'B1', null].entries()) {
				const headers = trace(`${i + 1}`)
				await ctx.send('t', value === null ? null : Buffer.from(value), { ...key('k'), headers })
			}
			const changes = ctx.producer.messages.filter(m => m.topic === 'test-app-by-letter-repartition')
			expect(changes.map(m => [m.key?.toString(), m.value, m.headers])).toEqual([
				['A', bytes('A1', NEW_ONLY), trace('1')],
				// KIP-904: the row stays in its group, one record with both values (varint length 2 -> 0x04).
				['A', bytes(0x04, 'A2', 'A1', BOTH), trace('2')],
				['A', bytes('A2', OLD_ONLY), trace('3')],
				['B', bytes('B1', NEW_ONLY), trace('3')],
				['B', bytes('B1', OLD_ONLY), trace('4')],
			])
			expect(counts(ctx)).toEqual([
				['A', 1],
				['A', 1],
				['A', 0],
				['B', 1],
				['B', 0],
			])
			const results = ctx.producer.messages.filter(m => m.topic === 'out')
			expect(results.map(m => m.headers)).toEqual(['1', '2', '3', '3', '4'].map(trace))
		})
	})

	it('KF6: the aggregations of a named grouping share one repartition topic; B4: another operation may not', async () => {
		const driver = new TestDriver()
		const grouped = driver.table('t', S).groupBy((_k, v) => [v, v] as [string, string], { ...S, name: 'g' })
		grouped.count().toStream().to('out', { key: str, value: num })
		grouped
			.reduce(
				(agg, v) => agg + v,
				(agg, v) => agg.replace(v, '')
			)
			.toStream()
			.to('reduced', S)
		const rekeyed = driver.input('u', S).selectKey((_k, v) => v)
		expect(() => rekeyed.toTable({ ...S, name: 'g' })).toThrow("The repartition topic name 'g' is already used")
		await driver.run(async ctx => {
			await send(ctx, 't', 'k1', 'A')
			await send(ctx, 't', 'k2', 'A')
			expect(counts(ctx)).toEqual([
				['A', 1],
				['A', 2],
			])
			expect(values(ctx, 'reduced')).toEqual(['A', 'AA'])
			const sinks = ctx.producer.messages.filter(m => m.topic === 'test-app-g-repartition')
			expect(sinks).toHaveLength(2)
		})
	})

	it('each aggregation of an unnamed grouping has its own repartition topic, named after its store if named', () => {
		const app = flow({ applicationId: 'app', client })
		const grouped = app.table('t', S).groupBy((_k, v) => [v, v] as [string, string], S)
		grouped.count({ storeName: 'counts' })
		grouped.count()
		const topology = internals(app).build()
		expect([...topology.internalTopics.keys()].sort()).toEqual([
			'app-counts-changelog',
			'app-counts-repartition',
			'app-repartition-0-repartition',
			'app-table-count-store-1-changelog',
			'app-table-t-0-changelog',
		])
	})

	it('B20, B21 retired: an aggregation has one store, logged unless its changelog is off; groupBy has none', () => {
		const app = flow({ applicationId: 'app', client })
		const grouped = app.table('t', S).groupBy((_k, v) => [v, v] as [string, string], { ...S, name: 'g' })
		const agg = () => ''
		grouped.aggregate(
			agg,
			(_k, v, a) => a + v,
			(_k, v, a) => a.replace(v, ''),
			{ value: str, storeName: 'kept' }
		)
		grouped.aggregate(
			agg,
			(_k, v, a) => a + v,
			(_k, v, a) => a.replace(v, ''),
			{
				value: str,
				storeName: 'quiet',
				changelog: false,
			}
		)
		const topology = internals(app).build()
		const aggregation = topology.subtopologyOf('app-g-repartition')!
		expect(aggregation.stores.map(s => s.name)).toEqual(['kept', 'quiet'])
		expect([...aggregation.changelogTopics.keys()]).toEqual(['kept'])
		expect(topology.subtopologyOf('t')!.stores.map(s => s.name)).toEqual(['table-t-0'])
	})

	it('the selector may change the value type (Grouped.value); groupBy and aggregate need their codecs', async () => {
		const driver = new TestDriver()
		const t = driver.table('t', S)
		expect(() => t.groupBy((_k, v) => [v, v.length])).toThrow('groupBy() needs a key codec')
		const grouped = t.groupBy((k, v) => ['all', v.length] as const, { key: str, value: num })
		expect(() =>
			// @ts-expect-error: the aggregate's codec is required
			grouped.aggregate(
				() => 0,
				(_k, v, sum) => sum + v,
				(_k, v, sum) => sum - v
			)
		).toThrow('aggregate() needs a codec for its aggregates')
		grouped
			.reduce(
				(sum, v) => sum + v,
				(sum, v) => sum - v,
				{ storeName: 'lengths' }
			)
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ctx => {
			await send(ctx, 't', 'a', 'xx', 3)
			await send(ctx, 't', 'b', 'yyy', 4)
			await send(ctx, 't', 'a', 'x', 5)
			// The update of 'a' stays in its group: one change, subtracting 2 and adding 1 (KIP-904).
			expect(counts(ctx)).toEqual([
				['all', 2],
				['all', 5],
				['all', 4],
			])
			expect(await ctx.getStateStore<Stored<number>>('lengths').get('all')).toEqual({ value: 4, timestamp: 5 })
		})
	})

	it('groups the results of a join: the forced old values retract each row from its previous group', async () => {
		const driver = new TestDriver()
		driver
			.table('a', S)
			.leftJoin(driver.table('b', S), (x, y) => y ?? x)
			.groupBy((_k, v) => [v, v] as [string, string], S)
			.count()
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'X')
			await send(ctx, 'b', 'k', 'Y')
			await send(ctx, 'b', 'k', null)
			expect(counts(ctx)).toEqual([
				['X', 1],
				['X', 0],
				['Y', 1],
				['Y', 0],
				['X', 1],
			])
		})
	})

	it('gives the same results under at-least-once, exactly-once per record and exactly-once on stop', async () => {
		const runs: (TestDriverConfig | undefined)[] = [
			undefined,
			{ processingGuarantee: 'exactly_once' },
			{ processingGuarantee: 'exactly_once', commitEvery: 'stop' },
		]
		const results: unknown[] = []
		for (const config of runs) {
			const driver = new TestDriver(config)
			driver
				.table('t', S)
				.groupBy((_k, v) => [v, v] as [string, string], S)
				.count()
				.toStream()
				.to('out', { key: str, value: num })
			await driver.run(async ctx => {
				for (const [k, v] of [
					['k1', 'A'],
					['k2', 'A'],
					['k1', 'B'],
					['k2', null],
					['k3', 'B'],
				] as const) {
					await send(ctx, 't', k, v)
				}
				results.push(counts(ctx))
			})
		}
		expect(results[0]).toEqual([
			['A', 1],
			['A', 2],
			['A', 1],
			['B', 1],
			['A', 0],
			['B', 2],
		])
		expect(results[1]).toEqual(results[0])
		expect(results[2]).toEqual(results[0])
	})
})
