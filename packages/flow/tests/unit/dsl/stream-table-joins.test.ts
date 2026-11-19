import { describe, expect, it } from 'vitest'
import type { KTable } from '@/api/ktable.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import type { SinkDef } from '@/topology/nodes.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'

const str = codec.string()
const S = { key: str, value: str }
const joiner = (v: string, tv: string | null) => `${v}+${tv}`
/** A joiner that shows the key: `<key>:<value>+<table value>`. */
const keyJoiner = (k: string | null, v: string, tv: string | null) => `${k}:${v}+${tv}`
const client = { clientId: 'app', brokers: ['localhost:9092'] }

/** Sends a string value at a timestamp, with a string key or none. */
function send(ctx: TestContextApi, topic: string, k: string | null, value: string | null, timestamp = 0) {
	const key = k === null ? null : Buffer.from(k)
	return ctx.send(topic, value === null ? null : Buffer.from(value), { key, timestamp })
}

const values = (ctx: TestContextApi, topic: string) => ctx.output(topic, S).map(o => o.value)
const keys = (ctx: TestContextApi, topic: string) => ctx.output(topic, S).map(o => o.key)

/** The null-key marks of the repartition sinks of an app (4.1.6), by topic. */
function nullKeyMarks(app: ReturnType<typeof flow>): Record<string, SinkDef['nullKeys']> {
	const marks: Record<string, SinkDef['nullKeys']> = {}
	const nodes = internals(app)
		.build()
		.subtopologies.flatMap(s => s.nodes)
	for (const node of nodes) {
		if (node.kind === 'sink' && node.internal === 'repartition') marks[node.topic as string] = node.nullKeys
	}
	return marks
}

describe.each(GUARANTEES)('stream-table join results (6.14; %s)', (_guarantee, config) => {
	it('KIP-962: a left join joins a null key with null; an inner join drops it; both drop null values', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S)
		const stream = driver.input('s', S)
		stream.joinWithKey(table, keyJoiner).to('inner', S)
		stream.leftJoinWithKey(table, keyJoiner).to('left', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'T')
			await send(ctx, 's', 'k', 'V')
			await send(ctx, 's', null, 'N')
			await send(ctx, 's', 'k', null)
			await send(ctx, 's', 'missing', 'M')
			expect(values(ctx, 'inner')).toEqual(['k:V+T'])
			expect(values(ctx, 'left')).toEqual(['k:V+T', 'null:N+null', 'missing:M+null'])
		})
	})

	it('KF12: a repartition before a left join keeps null keys; one before an inner join drops them', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S)
		const rekeyed = driver.input('s', S).selectKey((_k, v) => (v === 'none' ? (null as unknown as string) : v!))
		rekeyed.leftJoin(table, joiner, { name: 'left', key: str }).to('left', S)
		rekeyed.join(table, joiner, { name: 'inner', key: str }).to('inner', S)
		await driver.run(async ctx => {
			await send(ctx, 't', 'x', 'T')
			await send(ctx, 's', 'a', 'x')
			await send(ctx, 's', 'b', 'none')
			expect(keys(ctx, 'test-app-left-repartition')).toEqual(['x', null])
			expect(keys(ctx, 'test-app-inner-repartition')).toEqual(['x'])
			expect(values(ctx, 'left')).toEqual(['x+T', 'none+null'])
			expect(values(ctx, 'inner')).toEqual(['x+T'])
		})
	})

	it('W20: looks up any table: derived ones through their getters, materialized ones in their stores', async () => {
		const driver = new TestDriver(config)
		const table = driver.table('t', S)
		const lookups: Record<string, KTable<string, string>> = {
			filtered: table.filter((_k, v) => v !== 'hidden'),
			mapped: table.mapValues(v => v.toUpperCase()),
			joined: table.join(driver.table('u', S), (a, b) => `${a}&${b}`),
			materialized: table.mapValues(v => `${v}!`, { materialized: { storeName: 'bang', value: str } }),
			counted: driver
				.input('c', S)
				.groupByKey()
				.count()
				.mapValues(count => String(count)),
		}
		const stream = driver.input('s', S)
		for (const [topic, lookup] of Object.entries(lookups)) {
			stream.leftJoin(lookup, joiner).to(topic, S)
		}
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v')
			await send(ctx, 'u', 'k', 'w')
			await send(ctx, 'c', 'k', 'x')
			await send(ctx, 'c', 'k', 'y')
			await send(ctx, 's', 'k', 'S')
			await send(ctx, 't', 'k', 'hidden')
			await send(ctx, 's', 'k', 'S2')
			expect(Object.fromEntries(Object.keys(lookups).map(topic => [topic, values(ctx, topic)]))).toEqual({
				filtered: ['S+v', 'S2+null'],
				mapped: ['S+V', 'S2+HIDDEN'],
				joined: ['S+v&w', 'S2+hidden&w'],
				materialized: ['S+v!', 'S2+hidden!'],
				counted: ['S+2', 'S2+2'],
			})
		})
	})
})

describe('null keys on the repartition topics of stream-table joins (KF12)', () => {
	it('a left stream-table or global join downstream keeps them; inner joins do not', () => {
		const marks = (left: boolean) => {
			const app = flow({ applicationId: 'app', client })
			const regions = app.globalTable('regions', S)
			const rekeyed = app.stream('s', S).selectKey((_k, v) => v!)
			const joined = rekeyed.join(app.table('t', S), joiner, { name: 'enrich', key: str })
			const direct = { name: 'direct', key: str }
			if (left) {
				joined.leftJoin(regions, (_k, v) => v, joiner)
				rekeyed.leftJoin(app.table('u', S), joiner, direct)
			} else {
				joined.join(regions, (_k, v) => v, joiner)
				rekeyed.join(app.table('u', S), joiner, direct)
			}
			return nullKeyMarks(app)
		}
		expect(marks(true)).toEqual({ 'app-enrich-repartition': 'keep', 'app-direct-repartition': 'keep' })
		expect(marks(false)).toEqual({ 'app-enrich-repartition': 'drop', 'app-direct-repartition': 'drop' })
	})
})
