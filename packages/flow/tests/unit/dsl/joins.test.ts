import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { DslContext } from '@/dsl/context.js'
import { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import { decodeCommitMetadata } from '@/processor/metadata.js'
import type { WindowStore } from '@/state.js'
import type { TaskStateManager } from '@/state/state-manager.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { APP, TaskHarness } from '../support/task-harness.js'

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
})

describe('stream-stream joins', () => {
	/** `l.<kind>(r)` with a 1 s window, results written to 'out' as `<left>+<right>`. */
	function windowJoin(kind: 'join' | 'leftJoin' | 'outerJoin', config?: TestDriverConfig): TestDriver {
		const driver = new TestDriver(config)
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		const joiner = (a: string | null, b: string | null) => `${a}+${b}`
		const windows = JoinWindows.of(1000)
		const joined =
			kind === 'join'
				? l.join(r, joiner, windows)
				: kind === 'leftJoin'
					? l.leftJoin(r, joiner, windows)
					: l.outerJoin(r, joiner, windows)
		joined.to('out', S)
		return driver
	}

	it('results keep the key, timestamp and headers of the record that produced them', async () => {
		const driver = windowJoin('leftJoin')
		await driver.run(async ctx => {
			await ctx.send('l', Buffer.from('L'), { ...key('k'), timestamp: 100, headers: { h: Buffer.from('l') } })
			await ctx.send('r', Buffer.from('R'), { ...key('k'), timestamp: 300, headers: { h: Buffer.from('r') } })
			const out = ctx.producer.messagesFor('out')
			expect(
				out.map(m => [m.key?.toString(), m.value?.toString(), m.timestamp, m.headers?.h?.toString()])
			).toEqual([
				['k', 'L+null', 100n, 'l'],
				['k', 'L+R', 300n, 'r'],
			])
		})
	})

	it('left join: the right side emits matches only; outer join: both sides emit unmatched records at once', async () => {
		const left = windowJoin('leftJoin')
		await left.run(async ctx => {
			await send(ctx, 'r', 'k', 'R', 100)
			await send(ctx, 'l', 'k', 'L', 200)
			expect(values(ctx)).toEqual(['L+R'])
		})
		const outer = windowJoin('outerJoin')
		await outer.run(async ctx => {
			await send(ctx, 'r', 'k', 'R', 100)
			await send(ctx, 'l', 'k', 'L', 200)
			await send(ctx, 'l', 'k', 'X', 5000)
			expect(values(ctx)).toEqual(['null+R', 'L+R', 'X+null'])
		})
	})

	it('joins every pair of matching records, each once, in timestamp order of the other side', async () => {
		const driver = windowJoin('join')
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L2', 200)
			await send(ctx, 'l', 'k', 'L1', 100)
			await send(ctx, 'l', 'other', 'O', 150)
			await send(ctx, 'r', 'k', 'R1', 150)
			await send(ctx, 'r', 'k', 'R2', 160)
			expect(values(ctx)).toEqual(['L1+R1', 'L2+R1', 'L1+R2', 'L2+R2'])
		})
	})

	it('drops records with a null key or a null value on both sides', async () => {
		const driver = windowJoin('outerJoin')
		await driver.run(async ctx => {
			await ctx.send('l', Buffer.from('L'), { timestamp: 100 })
			await send(ctx, 'l', 'k', null, 100)
			await ctx.send('r', Buffer.from('R'), { timestamp: 100 })
			await send(ctx, 'r', 'k', null, 100)
			expect(values(ctx)).toEqual([])
		})
	})

	it('a self-join matches each record with itself once', async () => {
		const driver = new TestDriver()
		const s = driver.input('s', S)
		s.join(s, (a, b) => `${a}+${b}`, JoinWindows.of(1000)).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 's', 'k', 'A', 100)
			expect(values(ctx)).toEqual(['A+A'])
			await send(ctx, 's', 'k', 'B', 200)
			expect(values(ctx)).toEqual(['A+A', 'B+A', 'A+B', 'B+B'])
		})
	})

	it('JoinWindows.before/after bound the window of the left side; the right side sees it mirrored', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		const windows = JoinWindows.ofTimeDifferenceWithNoGrace(1000).before(0)
		l.join(r, (a, b) => `${a}+${b}`, windows).to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'early', 4999)
			await send(ctx, 'r', 'k', 'same', 5000)
			await send(ctx, 'r', 'k', 'last', 6000)
			await send(ctx, 'r', 'k', 'late', 6001)
			await send(ctx, 'l', 'k', 'L2', 3999)
			expect(values(ctx)).toEqual(['L+same', 'L+last', 'L2+early'])
		})
	})

	it('Joined.within takes the size of TimeWindows and the gap of SessionWindows as a symmetric window', async () => {
		for (const within of [TimeWindows.of(1000), SessionWindows.withInactivityGap(1000)]) {
			const driver = new TestDriver()
			const l = driver.input('l', S)
			const r = driver.input('r', S)
			l.join(r, (a, b) => `${a}+${b}`, { within }).to('out', S)
			await driver.run(async ctx => {
				await send(ctx, 'l', 'k', 'L', 5000)
				await send(ctx, 'r', 'k', 'in', 4000)
				await send(ctx, 'r', 'k', 'out', 6001)
				expect(values(ctx)).toEqual(['L+in'])
			})
		}
	})

	it('a join without a window, or with SlidingWindows, is a TopologyError', () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S)
		const joiner = (a: string | null, b: string | null) => `${a}+${b}`
		expect(() => l.join(r, joiner, {})).toThrow(TopologyError)
		expect(() => l.join(r, joiner, {})).toThrow('join() with a stream needs JoinWindows as its third argument')
		expect(() => l.leftJoin(r, joiner, { within: SlidingWindows.ofTimeDifferenceWithNoGrace(10) })).toThrow(
			'leftJoin() with a stream cannot use SlidingWindows'
		)
	})

	it('gives the same results under at-least-once, exactly-once per record and exactly-once on stop', async () => {
		const runs: (TestDriverConfig | undefined)[] = [
			undefined,
			{ processingGuarantee: 'exactly_once' },
			{ processingGuarantee: 'exactly_once', commitEvery: 'stop' },
		]
		const results: unknown[] = []
		for (const config of runs) {
			const driver = windowJoin('outerJoin', config)
			await driver.run(async ctx => {
				await send(ctx, 'l', 'k', 'L1', 100)
				await send(ctx, 'l', 'k', 'L2', 100)
				await send(ctx, 'r', 'k', 'R1', 1100)
				await send(ctx, 'r', 'k', 'R2', 50)
				await send(ctx, 'l', 'k', 'L3', 3000)
				results.push(values(ctx))
			})
		}
		expect(results[0]).toEqual(['L1+null', 'L2+null', 'L1+R1', 'L2+R1', 'L1+R2', 'L2+R2', 'L3+null'])
		expect(results[1]).toEqual(results[0])
		expect(results[2]).toEqual(results[0])
	})

	it('B2: the StreamJoined codecs are used when no side is repartitioned', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', S).mapValues(v => ({ v }))
		const r = driver.input('r', S)
		l.join(r, (a, b) => `${a.v}+${b}`, JoinWindows.of(1000), { value: codec.json<{ v: string | null }>() }).to(
			'out',
			S
		)
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 100)
			await send(ctx, 'r', 'k', 'R', 100)
			expect(values(ctx)).toEqual(['L+R'])
		})
	})

	it('needs codecs for the buffers: a value changed upstream without StreamJoined.value is a TopologyError', () => {
		const driver = new TestDriver()
		const l = driver.input('l', S)
		const r = driver.input('r', S).mapValues(v => v)
		expect(() => l.join(r, (a, b) => a + b, JoinWindows.of(1000))).toThrow(
			'join() of the other stream needs a value codec'
		)
	})

	it('B22: an invalid other stream throws before anything is registered', () => {
		const app = flow({ applicationId: 'app', client })
		const foreign = flow({ applicationId: 'other', client }).stream('x')
		const rekeyed = app.stream('a', S).selectKey((_k, v) => v)
		expect(() => rekeyed.join(foreign as never, (a, b) => a + b, JoinWindows.of(1000))).toThrow(
			'join() needs a stream of the same app'
		)
		const topology = internals(app).build()
		expect(topology.internalTopics.size).toBe(0)
		expect(topology.subtopologies.flatMap(s => s.stores)).toEqual([])
	})

	it('repartitions a re-keyed side before joining; a named join names its topics, nodes and stores', async () => {
		const app = flow({ applicationId: 'app', client })
		const l = app.stream('l', S).selectKey((_k, v) => v!)
		const r = app.stream('r', S)
		l.join(r, (a, b) => a + b, JoinWindows.of(1000), { name: 'pairs', key: str })
		const topology = internals(app).build()
		expect([...topology.internalTopics.keys()].sort()).toEqual([
			'app-pairs-left-repartition',
			'app-pairs-other-join-store-changelog',
			'app-pairs-this-join-store-changelog',
		])
		const join = topology.subtopologyOf('app-pairs-left-repartition')!
		expect(join).toBe(topology.subtopologyOf('r'))
		expect(join.nodes.map(n => n.name).filter(n => n.startsWith('pairs'))).toEqual([
			'pairs-this-join',
			'pairs-other-join',
			'pairs-merge',
		])
		expect(join.stores.map(s => s.name)).toEqual(['pairs-this-join-store', 'pairs-other-join-store'])
		expect(topology.copartitionGroups.map(group => [...group].sort())).toEqual([
			['app-pairs-left-repartition', 'r'],
		])
	})

	it('names the join nodes of left and outer joins as Kafka Streams does', () => {
		const app = flow({ applicationId: 'app', client })
		const [a, b] = ['a', 'b'].map(t => app.stream(t, S))
		const joiner = (x: string | null, y: string | null) => `${x}${y}`
		a!.leftJoin(b!, joiner, JoinWindows.of(1000), { name: 'l' })
		a!.outerJoin(b!, joiner, JoinWindows.of(1000), { name: 'o' })
		a!.leftJoin(b!, joiner, JoinWindows.of(1000))
		a!.outerJoin(b!, joiner, JoinWindows.of(1000))
		const [join] = internals(app).build().subtopologies
		expect(join!.nodes.map(n => n.name.replace(/-\d+$/, '')).filter(n => n !== 'KSTREAM-SOURCE')).toEqual([
			'l-this-join',
			'l-outer-other-join',
			'l-merge',
			'o-outer-this-join',
			'o-outer-other-join',
			'o-merge',
			'KSTREAM-JOINTHIS',
			'KSTREAM-OUTEROTHER',
			'KSTREAM-MERGE',
			'KSTREAM-OUTERTHIS',
			'KSTREAM-OUTEROTHER',
			'KSTREAM-MERGE',
		])
	})

	it('StreamJoined.storeName names the buffers; unnamed buffers keep their generated names', () => {
		const app = flow({ applicationId: 'app', client })
		const [a, b, c] = ['a', 'b', 'c'].map(t => app.stream(t, S))
		a!.join(b!, (x, y) => x + y, JoinWindows.of(1000), { storeName: 'ab' })
		a!.leftJoin(c!, (x, y) => x + y, JoinWindows.of(1000))
		a!.outerJoin(c!, (x, y) => `${x}${y}`, JoinWindows.of(1000))
		const stores = internals(app)
			.build()
			.subtopologies.flatMap(s => s.stores.map(store => store.name))
		expect(stores).toEqual([
			'ab-this-join-store',
			'ab-other-join-store',
			'stream-left-join-left-0',
			'stream-left-join-right-1',
			'stream-outer-join-left-2',
			'stream-outer-join-right-3',
		])
	})

	it('buffers are window stores logged to changelogs with window retention (twice the join window)', () => {
		const app = flow({ applicationId: 'app', client })
		app.stream('l', S).join(app.stream('r', S), (a, b) => a + b, { within: TimeWindows.of('10s') })
		const [join] = internals(app).build().subtopologies
		expect(join!.stores.map(s => [s.name, s.kind, s.retentionMs, s.windowSizeMs])).toEqual([
			['stream-join-left-0', 'window', 20_000, 10_000],
			['stream-join-right-1', 'window', 20_000, 10_000],
		])
		for (const store of join!.stores) {
			expect(store.logging).toMatchObject({ enabled: true, topic: `app-${store.name}-changelog` })
			const { configs } = store.logging as { configs: Record<string, string> }
			expect(configs['cleanup.policy']).toBe('delete,compact')
			expect(configs['retention.ms']).toBe('20000')
		}
		const noLog = flow({ applicationId: 'app', client })
		noLog.stream('l', S).join(noLog.stream('r', S), (a, b) => a + b, JoinWindows.of(10), { changelog: false })
		const [unlogged] = internals(noLog).build().subtopologies
		expect(unlogged!.stores.map(s => s.logging.enabled)).toEqual([false, false])
	})

	it('the sides share one clock, committed under the left buffer name, which expires both buffers', async () => {
		const driver = windowJoin('join')
		await driver.run(async ctx => {
			await send(ctx, 'r', 'k', 'R', 1_000_000)
			await send(ctx, 'l', 'k', 'L', 1_000)
			const committed = ctx.consumer.committed.get('l:0')!
			expect(decodeCommitMetadata(committed.metadata).metadata).toEqual(
				new Map([['stream-join-left-0', 1_000_000]])
			)
			// By the clock the right side advanced, L is past retention as soon as it was buffered.
			const entries = (store: string) =>
				ctx.getStateStore<WindowStore<string, string>>(store).approximateNumEntries()
			expect(await entries('stream-join-left-0')).toBe(0)
			expect(await entries('stream-join-right-1')).toBe(1)
		})
	})

	for (const eos of [false, true]) {
		it(`${eos ? 'exactly-once' : 'at-least-once'}: a task re-created from its commits keeps its buffers and seeds the clock`, async () => {
			const base = 1_000_000_000
			const h = new TaskHarness({
				eos,
				build: builder => {
					const ctx = new DslContext(builder, { applicationId: APP })
					const left: KStream<string, string> = ctx.stream('l', S)
					const right: KStream<string, string> = ctx.stream('r', S)
					left.join(right, (a, b) => `${a}+${b}`, JoinWindows.of(1000)).to('out', S)
				},
			})
			const first = await h.start(h.task())
			h.enqueue(first, 'l', [
				{ key: 'k', value: 'L', timestamp: base },
				{ key: 'z', value: 'Z', timestamp: base + 50_000 },
			])
			h.enqueue(first, 'r', [{ key: 'y', value: 'Y', timestamp: base }])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			const restarted = await h.restart()
			h.enqueue(restarted, 'r', [{ key: 'k', value: 'R', timestamp: base + 500 }])
			await h.process(restarted)
			await h.commit()
			// R joins L, which was buffered before the restart.
			expect(h.output('out').map(o => o.value)).toEqual(['L+R'])
			// The right side's first expiry then runs by the clock seeded from the commits (base + 50 s), not by R's
			// timestamp: only Z, the newest record, is left.
			const { stateManager } = restarted as unknown as { stateManager: TaskStateManager }
			const entries = (store: string) =>
				(stateManager.store(store) as WindowStore<string, string>).approximateNumEntries()
			expect(await entries('stream-join-left-0')).toBe(1)
			expect(await entries('stream-join-right-1')).toBe(0)
		})
	}
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

	it('drops stream records with a null key or a null value, also in a left join', async () => {
		const driver = new TestDriver()
		driver
			.input('s', S)
			.leftJoin(driver.table('t', S), (v, tv) => `${v}+${tv}`)
			.to('out', S)
		await driver.run(async ctx => {
			await ctx.send('s', Buffer.from('S'), { timestamp: 1 })
			await send(ctx, 's', 'k', null, 2)
			expect(values(ctx)).toEqual([])
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

	it('the deprecated two-argument form looks up the record key; null keys and values are dropped', async () => {
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
			expect(values(ctx)).toEqual(['click+Alice', 'view+null'])
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
