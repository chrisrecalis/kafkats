import { describe, expect, it } from 'vitest'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'

const str = codec.string()
const S = { key: str, value: str }
const regionIds = codec.json<number>()
const client = { clientId: 'app', brokers: ['localhost:9092'] }

type Event = { region: number | null }
const byRegion = (_key: string | null, event: Event) => event.region
/** A joiner that shows the record key: `<key>@<region>=<region name>`. */
const keyJoiner = (key: string | null, event: Event, name: string | null) => `${key}@${event.region}=${name}`

/** Sends a string value (null: a tombstone) with a string key or none. */
function send(
	ctx: TestContextApi,
	topic: string,
	k: string | null,
	value: string | null,
	timestamp = 0,
	partition = 0
) {
	const key = k === null ? null : Buffer.from(k)
	return ctx.send(topic, value === null ? null : Buffer.from(value), { key, timestamp, partition })
}

/** Sets (null: deletes) the name of a region. */
function region(ctx: TestContextApi, id: number, name: string | null) {
	return ctx.send('regions', name === null ? null : Buffer.from(name), { keyValue: id, keyCodec: regionIds })
}

/** Sends an event (null: a tombstone) with a string key or none. */
function event(ctx: TestContextApi, k: string | null, value: Event | null, timestamp = 0) {
	return ctx.send('events', value, { key: k === null ? null : Buffer.from(k), timestamp })
}

/** Events keyed by name, and a global table of region names keyed by region id. */
function eventsAndRegions(driver: TestDriver) {
	const regions = driver.globalTable('regions', { key: regionIds, value: str })
	const events = driver.input('events', { key: str, value: codec.json<Event>() })
	return { events, regions }
}

const values = (ctx: TestContextApi, topic: string) => ctx.output(topic, S).map(o => o.value)

describe.each(GUARANTEES)('stream-global table join results (6.19; %s)', (_guarantee, config) => {
	it('looks up the key the selector picks; leftJoinWithKey gives the joiner the record key', async () => {
		const driver = new TestDriver(config)
		const { events, regions } = eventsAndRegions(driver)
		events.join(regions, byRegion, (value, name) => `${value.region}=${name}`).to('inner', S)
		events.leftJoinWithKey(regions, byRegion, keyJoiner).to('left', S)
		await driver.run(async ctx => {
			await region(ctx, 1, 'Europe')
			await event(ctx, 'a', { region: 1 })
			await event(ctx, 'b', { region: 2 })
			expect(values(ctx, 'inner')).toEqual(['1=Europe'])
			expect(values(ctx, 'left')).toEqual(['a@1=Europe', 'b@2=null'])
		})
	})

	it('KIP-962: a left join joins a record without a selected key with null; an inner join drops it', async () => {
		const driver = new TestDriver(config)
		const { events, regions } = eventsAndRegions(driver)
		const selected: (string | null)[] = []
		const selector = (key: string | null, value: Event) => {
			selected.push(key)
			return byRegion(key, value)
		}
		events.joinWithKey(regions, selector, keyJoiner).to('inner', S)
		events.leftJoinWithKey(regions, selector, keyJoiner).to('left', S)
		await driver.run(async ctx => {
			await region(ctx, 1, 'Europe')
			await event(ctx, null, { region: 1 })
			await event(ctx, 'a', { region: null })
			// A selector reading a field the JSON lacks returns undefined: no key either.
			await ctx.send('events', {}, { key: Buffer.from('b') })
			await event(ctx, 'c', null)
			// A record without a key joins on the key its selector picks.
			expect(values(ctx, 'inner')).toEqual(['null@1=Europe'])
			expect(values(ctx, 'left')).toEqual(['null@1=Europe', 'a@null=null', 'b@undefined=null'])
			// Null values are dropped before the selector sees them.
			expect(selected).toEqual([null, null, 'a', 'a', 'b', 'b'])
		})
	})

	it('results carry the record key, timestamp and headers; table updates produce none', async () => {
		const driver = new TestDriver(config)
		const { events, regions } = eventsAndRegions(driver)
		events.joinWithKey(regions, byRegion, keyJoiner).to('inner', S)
		events.leftJoinWithKey(regions, byRegion, keyJoiner).to('left', S)
		await driver.run(async ctx => {
			await ctx.send(
				'events',
				{ region: 1 },
				{ key: Buffer.from('a'), timestamp: 10, headers: { h: Buffer.from('x') } }
			)
			await region(ctx, 1, 'Europe')
			// Not synchronized with stream time: an older record sees the newer row.
			await event(ctx, 'b', { region: 1 }, 5)
			await region(ctx, 1, null)
			await event(ctx, 'c', { region: 1 }, 20)
			const sent = (topic: string) =>
				ctx.producer.messagesFor(topic).map(m => [m.value?.toString(), m.timestamp, m.headers?.h?.toString()])
			expect(sent('inner')).toEqual([['b@1=Europe', 5n, undefined]])
			expect(sent('left')).toEqual([
				['a@1=null', 10n, 'x'],
				['b@1=Europe', 5n, undefined],
				['c@1=null', 20n, undefined],
			])
		})
	})

	it('DL21: the deprecated two-argument form looks up the record key; only a left join joins null keys', async () => {
		const driver = new TestDriver(config)
		const users = driver.globalTable('users', S)
		const events = driver.input('events', S)
		events.join(users, (value, name) => `${value}+${name}`).to('inner', S)
		events.leftJoin(users, (value, name) => `${value}+${name}`).to('left', S)
		await driver.run(async ctx => {
			await send(ctx, 'users', 'u1', 'Alice')
			await send(ctx, 'events', 'u1', 'click')
			await send(ctx, 'events', 'u2', 'view')
			await send(ctx, 'events', null, 'nokey')
			await send(ctx, 'events', 'u1', null)
			expect(values(ctx, 'inner')).toEqual(['click+Alice'])
			expect(values(ctx, 'left')).toEqual(['click+Alice', 'view+null', 'nokey+null'])
		})
	})
})

describe('stream-global table join wiring (6.19)', () => {
	it('needs no co-partitioning: every task looks up the whole table, whatever the partition counts', async () => {
		const driver = new TestDriver({ partitions: { events: 2, regions: 3 } })
		const regions = driver.globalTable('regions', S)
		driver
			.input('events', S)
			.join(
				regions,
				(_key, value) => value,
				(value, name) => `${value}=${name}`
			)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'regions', 'eu', 'Europe', 0, 2)
			await send(ctx, 'regions', 'us', 'America', 0, 1)
			await send(ctx, 'events', 'a', 'us', 0, 0)
			await send(ctx, 'events', 'b', 'eu', 0, 1)
			expect(values(ctx, 'out')).toEqual(['us=America', 'eu=Europe'])
		})
		expect(internals(driver.flow).build().copartitionGroups).toEqual([])
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

	it('KF12: a repartition topic before a left join keeps null keys, and the selector still finds the row', async () => {
		const driver = new TestDriver()
		const regions = driver.globalTable('regions', S)
		driver
			.input('events', S)
			.selectKey((_key, value) => (value === 'eu' ? null : value))
			.repartition({ name: 'by-region', key: str })
			.leftJoinWithKey(
				regions,
				(_key, value) => value,
				(key, value, name) => `${key}:${value}=${name}`
			)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'regions', 'eu', 'Europe')
			await send(ctx, 'events', 'a', 'eu')
			expect(ctx.output('test-app-by-region-repartition', S).map(r => r.key)).toEqual([null])
			expect(values(ctx, 'out')).toEqual(['null:eu=Europe'])
		})
	})

	it('names the join node; a global table of another app is a TopologyError', () => {
		const app = flow({ applicationId: 'app', client })
		const regions = app.globalTable('regions', S)
		const events = app.stream('events', S)
		const joiner = (value: string, name: string | null) => `${value}=${name}`
		events.join(regions, (_key, value) => value, joiner, { name: 'enrich' })
		events.leftJoin(regions, (_key, value) => value, joiner)
		const nodes = internals(app).build().subtopologyOf('events')!.nodes
		expect(nodes.map(node => node.name)).toEqual([
			'KSTREAM-SOURCE-0000000002',
			'enrich',
			'KSTREAM-LEFTJOIN-0000000003',
		])

		const foreign = flow({ applicationId: 'other', client }).globalTable('regions', S)
		expect(() => events.leftJoin(foreign, joiner)).toThrow('leftJoin() needs a global table of the same app')
		// A key-aware join's errors name the method called.
		const keyed = (key: string | null, value: string, name: string | null) => `${key}:${value}=${name}`
		expect(() => events.joinWithKey(foreign, (_key, value) => value, keyed)).toThrow(
			'joinWithKey() needs a global table of the same app'
		)
	})
})
