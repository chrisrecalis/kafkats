import { describe, expect, it } from 'vitest'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { JoinWindows, TimeWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'

const str = codec.string()
const S = { key: str, value: str }
const key = (k: string) => ({ key: Buffer.from(k) })
const client = { clientId: 'app', brokers: ['localhost:9092'] }

/** Sends a string value with a string key at a timestamp. */
function send(ctx: TestContextApi, topic: string, k: string, value: string | null, timestamp: number, partition = 0) {
	return ctx.send(topic, value === null ? null : Buffer.from(value), { ...key(k), timestamp, partition })
}

const values = (ctx: TestContextApi, topic = 'out') => ctx.output(topic, S).map(o => o.value)

describe('stream-table joins (copied from flow.test.ts)', () => {
	it('joins stream records with table lookups (inner join)', async () => {
		const driver = new TestDriver()

		type Event = { userId: string; action: string }
		type User = { name: string; tier: string }
		type Enriched = { userId: string; action: string; userName: string; userTier: string }

		const results: Enriched[] = []

		const users = driver.table('users', { key: codec.string(), value: codec.json<User>() })

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.join(users, (event, user) => ({
				userId: event.userId,
				action: event.action,
				userName: user.name,
				userTier: user.tier,
			}))
			.peek((_key, value) => {
				if (value === null) {
					throw new Error('unexpected tombstone in stream-table join result')
				}
				results.push(value)
			})

		await driver.run(async ({ send }) => {
			// First, populate the table with user data
			await send('users', { name: 'Alice', tier: 'gold' }, key('user1'))
			await send('users', { name: 'Bob', tier: 'silver' }, key('user2'))

			// Now send events - only matching users should be joined
			await send('events', { userId: 'user1', action: 'click' }, key('user1'))
			await send('events', { userId: 'user2', action: 'view' }, key('user2'))
			await send('events', { userId: 'user3', action: 'buy' }, key('user3'))
		})

		// Only user1 and user2 events should be joined (inner join)
		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ userId: 'user1', action: 'click', userName: 'Alice', userTier: 'gold' })
		expect(results[1]).toEqual({ userId: 'user2', action: 'view', userName: 'Bob', userTier: 'silver' })
	})

	it('left joins stream records with table lookups', async () => {
		const driver = new TestDriver()

		type Event = { action: string }
		type User = { name: string }
		type Result = { action: string; userName: string | null }

		const results: Result[] = []

		const users = driver.table('users', { key: codec.string(), value: codec.json<User>() })

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.leftJoin(users, (event, user) => ({
				action: event.action,
				userName: user?.name ?? null,
			}))
			.peek((_key, value) => {
				if (value === null) {
					throw new Error('unexpected tombstone in stream-table leftJoin result')
				}
				results.push(value)
			})

		await driver.run(async ({ send }) => {
			// Populate table with one user
			await send('users', { name: 'Alice' }, key('user1'))

			// Send events for both existing and non-existing users
			await send('events', { action: 'click' }, key('user1'))
			await send('events', { action: 'view' }, key('user2'))
		})

		// Left join should include all events
		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ action: 'click', userName: 'Alice' })
		expect(results[1]).toEqual({ action: 'view', userName: null })
	})

	it('updates table state and joins reflect latest values', async () => {
		const driver = new TestDriver()

		type User = { score: number }
		type Event = { action: string }
		type Result = { action: string; score: number }

		const results: Result[] = []

		const users = driver.table('users', { key: codec.string(), value: codec.json<User>() })

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.join(users, (event, user) => ({
				action: event.action,
				score: user.score,
			}))
			.peek((_key, value) => {
				if (value === null) {
					throw new Error('unexpected tombstone in stream-table join result')
				}
				results.push(value)
			})

		await driver.run(async ({ send }) => {
			// Insert initial user score
			await send('users', { score: 100 }, key('user1'))

			// Event joins with score=100
			await send('events', { action: 'a' }, key('user1'))

			// Update user score
			await send('users', { score: 200 }, key('user1'))

			// Event now joins with score=200
			await send('events', { action: 'b' }, key('user1'))
		})

		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ action: 'a', score: 100 })
		expect(results[1]).toEqual({ action: 'b', score: 200 })
	})
})

describe('stream-stream joins (copied from flow.test.ts)', () => {
	it('joins streams within a time window (inner join)', async () => {
		const driver = new TestDriver()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string; ad: string }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = driver.input('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = driver.input('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.join(
				impressions,
				(click, impression) => ({
					page: click.page,
					ad: impression.ad,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				if (key === null || value === null) {
					throw new Error('unexpected tombstone in stream-stream join result')
				}
				results.push({ key, value })
			})

		await driver.run(async ({ send }) => {
			// Impression at t=1000ms
			await send('impressions', { ad: 'banner1' }, { ...key('user1'), timestamp: 1000n })

			// Click at t=2000ms (within 5s window) - should join
			await send('clicks', { page: 'home' }, { ...key('user1'), timestamp: 2000n })

			// Click at t=10000ms (outside 5s window from impression) - no join from click side
			await send('clicks', { page: 'about' }, { ...key('user1'), timestamp: 10000n })
		})

		// Exactly one join: click at t=2000ms joins with impression at t=1000ms (within 5s window)
		// Click at t=10000ms should NOT join (outside window from impression at t=1000ms)
		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: 'banner1' } })
	})

	it('left joins streams - left side always emits', async () => {
		const driver = new TestDriver()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string; ad: string | null }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = driver.input('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = driver.input('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.leftJoin(
				impressions,
				(click, impression) => ({
					page: click.page,
					ad: impression?.ad ?? null,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				if (key === null || value === null) {
					throw new Error('unexpected tombstone in stream-stream leftJoin result')
				}
				results.push({ key, value })
			})

		await driver.run(async ({ send }) => {
			// Click without matching impression - should emit with null
			await send('clicks', { page: 'home' }, { ...key('user1'), timestamp: 1000n })

			expect(results).toHaveLength(1)
			expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: null } })

			// Add impression, then click - should join
			await send('impressions', { ad: 'banner1' }, { ...key('user2'), timestamp: 2000n })
			await send('clicks', { page: 'products' }, { ...key('user2'), timestamp: 2500n })
		})

		// Exactly 2 results: click without match (null ad) + click with matching impression
		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user2', value: { page: 'products', ad: 'banner1' } })
	})

	it('outer joins streams - both sides emit', async () => {
		const driver = new TestDriver()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string | null; ad: string | null }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = driver.input('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = driver.input('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.outerJoin(
				impressions,
				(click, impression) => ({
					page: click?.page ?? null,
					ad: impression?.ad ?? null,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				if (key === null || value === null) {
					throw new Error('unexpected tombstone in stream-stream outerJoin result')
				}
				results.push({ key, value })
			})

		await driver.run(async ({ send }) => {
			// Click alone - should emit with null ad
			await send('clicks', { page: 'home' }, { ...key('user1'), timestamp: 1000n })

			expect(results).toHaveLength(1)
			expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: null } })

			// Impression alone - should emit with null page
			await send('impressions', { ad: 'banner1' }, { ...key('user2'), timestamp: 2000n })
		})

		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user2', value: { page: null, ad: 'banner1' } })
	})
})

describe('joins (copied from operator-semantics*.test.ts)', () => {
	it('two records with same key + same timestamp on one side both join (no overwrite in window store)', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		l.join(r, (a, b) => `${a}+${b}`, JoinWindows.of(1000)).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L1', 100)
			await send(ctx, 'l', 'k', 'L2', 100) // same key + timestamp as L1
			await send(ctx, 'r', 'k', 'R1', 100)
			// Kafka Streams stores both (retainDuplicates) -> 2 results
			expect(values(ctx).sort()).toEqual(['L1+R1', 'L2+R1'])
		})
	})

	it('same-key same-timestamp duplicates via DSL both join', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		l.join(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L1', 100)
			await send(ctx, 'l', 'k', 'L2', 100)
			await send(ctx, 'r', 'k', 'R1', 100)
			expect(values(ctx).sort()).toEqual(['L1+R1', 'L2+R1'])
		})
	})

	it('window is inclusive at +/- window size and exclusive beyond', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		l.join(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'Rlo', 4000)
			await send(ctx, 'r', 'k', 'Rhi', 6000)
			await send(ctx, 'r', 'k', 'Rout1', 3999)
			await send(ctx, 'r', 'k', 'Rout2', 6001)
			expect(values(ctx)).toEqual(['L+Rlo', 'L+Rhi'])
		})
	})

	it('stream-table join sees table tombstone', async () => {
		const driver = new TestDriver()
		driver
			.input('s', S)
			.join(driver.table('t', S), (a, b) => `${a}+${b}`)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 1)
			await send(ctx, 's', 'k', 'a', 2)
			await send(ctx, 't', 'k', null, 3)
			await send(ctx, 's', 'k', 'b', 4)
			expect(values(ctx)).toEqual(['a+v'])
		})
	})

	it('stream can be joined with an aggregated table (groupByKey().count())', async () => {
		const driver = new TestDriver()
		const counts = driver.input('a', S).groupByKey().count()
		driver
			.input('b', S)
			.join(counts, (v, c) => `${v}:${c}`)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			expect(values(ctx)).toEqual(['y:1'])
		})
	})
})

describe('join wiring (copied from flow.test.ts)', () => {
	it('repartitions a re-keyed stream after a global-table join before aggregating it', () => {
		const app = flow({ applicationId: 'app', client })
		const regions = app.globalTable('regions', { key: codec.string(), value: codec.string() })

		// The join needs no repartitioning (every instance holds the whole table), but the
		// records still sit in the partition of their old key.
		app.stream('events', { key: codec.string(), value: codec.json<{ region: string }>() })
			.selectKey((_key, value) => value!.region)
			.join(regions, (event, name) => ({ ...event, name }))
			.groupByKey({ key: codec.string(), value: codec.json() })
			.count({ storeName: 'per-region' })

		const topology = internals(app).build()
		const perRegion = topology.subtopologies.find(s => s.stores.some(store => store.name === 'per-region'))
		expect([...perRegion!.sourceTopics]).toEqual(['app-per-region-repartition'])
	})

	it('BC10: joins the results of a windowed aggregation with a table without repartitioning them', () => {
		const app = flow({ applicationId: 'app', client })
		type Window = { key: string; window: { start: number; end: number } }
		// The table's topic must be partitioned by base key, as windowed sinks write (6.9).
		const labels = app.table('labels', { key: codec.json<Window>(), value: codec.string() })

		app.stream('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeWithNoGrace('10s'))
			.count({ storeName: 'windows' })
			.mapValues(count => count ?? 0)
			.filter(() => true)
			.toStream()
			.join(labels, (count, label) => `${label}: ${count}`, {
				key: codec.json<Window>(),
				value: codec.json<number>(),
			})

		const topology = internals(app).build()
		expect([...topology.internalTopics.keys()].filter(topic => topic.endsWith('-repartition'))).toEqual([])
		expect(topology.copartitionGroups.map(group => [...group].sort())).toContainEqual(['events', 'labels'])
	})
})

describe('stream-table joins', () => {
	it('passes the key to a joiner that declares three parameters', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		driver
			.input('s', S)
			.join(table, (k: string, v: string, tv: string) => `${k}:${v}+${tv}`)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'T', 1)
			await send(ctx, 's', 'k', 'S', 2)
			expect(values(ctx)).toEqual(['k:S+T'])
		})
	})

	it('only stream records produce results, with their key, timestamp and headers', async () => {
		const driver = new TestDriver()
		driver
			.input('s', S)
			.leftJoin(driver.table('t', S), (v, tv) => `${v}+${tv}`)
			.to('out', S)
		await driver.run(async ctx => {
			await ctx.send('s', Buffer.from('S'), { ...key('k'), timestamp: 10, headers: { h: Buffer.from('s') } })
			await send(ctx, 't', 'k', 'T', 20)
			await send(ctx, 't', 'k', 'U', 30)
			const out = ctx.producer.messagesFor('out')
			expect(out.map(m => [m.value?.toString(), m.timestamp, m.headers?.h?.toString()])).toEqual([
				['S+null', 10n, 's'],
			])
		})
	})

	it('drops stream records with a null value; a left join joins a null key with null (KIP-962)', async () => {
		const driver = new TestDriver()
		driver
			.input('s', S)
			.leftJoin(driver.table('t', S), (v, tv) => `${v}+${tv}`)
			.to('out', S)
		await driver.run(async ctx => {
			await ctx.send('s', Buffer.from('S'), { timestamp: 1 })
			await send(ctx, 's', 'k', null, 2)
			expect(values(ctx)).toEqual(['S+null'])
		})
	})

	it('repartitions a re-keyed stream (named by Joined.name) into the partitions of the table', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', S)
		driver
			.input('s', S)
			.selectKey((_k, v) => v!)
			.join(table, (v, tv) => `${v}+${tv}`, { name: 'enrich', key: str })
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'x', 'T', 1)
			await send(ctx, 's', 'k', 'x', 2)
			expect(ctx.output('test-app-enrich-repartition', S)).toEqual([
				{ topic: 'test-app-enrich-repartition', key: 'x', value: 'x', partition: undefined },
			])
			expect(values(ctx)).toEqual(['x+T'])
		})
		const topology = internals(driver.flow).build()
		expect(topology.copartitionGroups.map(group => [...group].sort())).toEqual([
			['t', 'test-app-enrich-repartition'],
		])
	})

	it('a table of another app is a TopologyError', () => {
		const driver = new TestDriver()
		const foreign = flow({ applicationId: 'other', client }).table('t', S)
		expect(() => driver.input('s', S).join(foreign, (v, tv) => v + tv)).toThrow(
			'join() needs a table of the same app'
		)
	})
})

describe('stream-global table joins', () => {
	it('looks up the key a selector picks; a left join joins records without a value with null', async () => {
		const driver = new TestDriver()
		const regions = driver.globalTable('regions', S)
		const events = driver.input('events', { key: str, value: codec.json<{ region: string }>() })
		events
			.join(
				regions,
				(_k, v) => v.region,
				(v, name) => `${v.region}=${name}`
			)
			.to('inner', S)
		events
			.leftJoin(
				regions,
				(_k, v) => v.region,
				(k: string | null, _v: { region: string }, name: string | null) => `${k}:${name}`
			)
			.to('left', S)
		await driver.run(async ctx => {
			await send(ctx, 'regions', 'eu', 'Europe', 1)
			await ctx.send('events', { region: 'eu' }, { ...key('e1'), timestamp: 2 })
			await ctx.send('events', { region: 'us' }, { ...key('e2'), timestamp: 3 })
			await ctx.send('events', { region: 'eu' }, { timestamp: 4 })
			expect(values(ctx, 'inner')).toEqual(['eu=Europe', 'eu=Europe'])
			expect(values(ctx, 'left')).toEqual(['e1:Europe', 'e2:null', 'null:Europe'])
		})
	})

	it('the deprecated two-argument form looks up the record key; a left join joins null keys with null', async () => {
		const driver = new TestDriver()
		const users = driver.globalTable('users', S)
		driver
			.input('events', S)
			.leftJoin(users, (v, name) => `${v}+${name}`)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'users', 'u1', 'Alice', 1)
			await send(ctx, 'events', 'u1', 'click', 2)
			await send(ctx, 'events', 'u2', 'view', 3)
			await ctx.send('events', Buffer.from('nokey'), { timestamp: 4 })
			await send(ctx, 'events', 'u1', null, 5)
			expect(values(ctx)).toEqual(['click+Alice', 'view+null', 'nokey+null'])
		})
	})

	it('does not repartition: a re-keyed stream stays flagged, so a later key-based operation repartitions', () => {
		const app = flow({ applicationId: 'app', client })
		const regions = app.globalTable('regions', S)
		app.stream('events', S)
			.selectKey((_k, v) => v)
			.join(
				regions,
				(k: string | null) => k,
				(v, name) => `${v}=${name}`
			)
			.toTable({ key: str, value: str, storeName: 'per-region' })
		const topology = internals(app).build()
		expect([...topology.internalTopics.keys()]).toEqual([
			'app-repartition-0-repartition',
			'app-per-region-changelog',
		])
		expect(topology.subtopologyOf('events')!.stores).toEqual([])
		expect(topology.subtopologyOf('app-repartition-0-repartition')!.stores.map(s => s.name)).toEqual(['per-region'])
	})
})
