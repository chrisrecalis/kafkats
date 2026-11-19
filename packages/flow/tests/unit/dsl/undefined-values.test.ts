/**
 * Undefined is no value, as null is (Kafka Streams has only null): a mapper, transformer or joiner that returns
 * undefined deletes on a table, joins nothing and fails nothing, wherever the value goes next. Every topology runs twice,
 * once with callbacks returning undefined and once with them returning null, and must write the same records.
 */
import { describe, expect, it } from 'vitest'
import type { KTable } from '@/api/ktable.js'
import { codec } from '@/codec.js'
import { JoinWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'

type Row = { a?: string }
type Absent = null | undefined

const str = codec.string()
const S = { key: str, value: str }
const rows = { key: str, value: codec.json<Row>() }
const num = codec.json<number>()

const sendRow = (ctx: TestContextApi, topic: string, key: string, row: Row, timestamp = 0) =>
	ctx.send(topic, Buffer.from(JSON.stringify(row)), { key: Buffer.from(key), timestamp })
const sendString = (ctx: TestContextApi, topic: string, key: string, value: string, timestamp = 0) =>
	ctx.send(topic, Buffer.from(value), { key: Buffer.from(key), timestamp })

/** What was written to 'out', as `key=value` (`key=null` for a tombstone); fails if a record failed processing. */
async function outputs(
	build: (driver: TestDriver, absent: Absent) => void,
	script: (ctx: TestContextApi) => Promise<void>
): Promise<string[][]> {
	const results: string[][] = []
	for (const absent of [undefined, null]) {
		const driver = new TestDriver()
		build(driver, absent)
		await driver.run(async ctx => {
			await script(ctx)
			results.push(
				ctx.producer
					.messagesFor('out')
					.map(({ key, value }) => `${key?.toString()}=${value === null ? null : value.toString()}`)
			)
		})
	}
	expect(results[0]).toEqual(results[1])
	return results
}

describe('undefined is no value, as null is', () => {
	it('toTable(): a mapped stream value of undefined deletes the key', async () => {
		const [written] = await outputs(
			(driver, absent) =>
				driver
					.input('in', rows)
					.mapValues(row => row?.a ?? absent)
					.toTable({ value: str })
					.toStream()
					.to('out', S),
			async ctx => {
				await sendRow(ctx, 'in', 'k', { a: 'x' })
				await sendRow(ctx, 'in', 'k', {})
			}
		)
		expect(written).toEqual(['k=x', 'k=null'])
	})

	it('KTable.mapValues(), materialized or not, and transformValues(): undefined deletes the key', async () => {
		for (const op of ['mapValues', 'materialized', 'transformValues'] as const) {
			const [written] = await outputs(
				(driver, absent) => {
					const table = driver.table('t', rows)
					const mapped =
						op === 'mapValues'
							? table.mapValues(row => row.a ?? absent)
							: op === 'materialized'
								? table.mapValues(row => row.a ?? absent, {
										materialized: { storeName: 'm', value: str },
									})
								: table.transformValues(() => ({ transform: (_key, row) => row?.a ?? absent }))
					mapped.toStream().to('out', S)
				},
				async ctx => {
					await sendRow(ctx, 't', 'k', { a: 'x' })
					await sendRow(ctx, 't', 'k', {})
				}
			)
			expect(written, op).toEqual(['k=x', 'k=null'])
		}
	})

	it('table-table joins: a side mapped to undefined has no value, a joiner returning undefined deletes', async () => {
		const [written] = await outputs(
			(driver, absent) => {
				const left = driver.table('l', rows).mapValues(row => row.a ?? absent)
				const right = driver.table('r', S)
				left.join(right, (a, b) => `${a}+${b}`)
					.toStream()
					.to('out', S)
				left.leftJoin(right, (a, b) => (b === 'none' ? absent : `${a}+${b}`), { name: 'left' })
					.toStream()
					.to('out', S)
			},
			async ctx => {
				await sendString(ctx, 'r', 'k', 'x')
				await sendRow(ctx, 'l', 'k', {})
				await sendRow(ctx, 'l', 'k', { a: 'y' })
				await sendString(ctx, 'r', 'k', 'none')
			}
		)
		expect(written).toEqual(['k=null', 'k=null', 'k=y+x', 'k=y+x', 'k=y+none', 'k=null'])
	})

	it('groupBy() aggregations do not count a row mapped to undefined', async () => {
		const [written] = await outputs(
			(driver, absent) =>
				driver
					.table('t', rows)
					.mapValues(row => row.a ?? absent)
					.groupBy((key, value) => [value, key], S)
					.count()
					.toStream()
					.to('out', { key: str, value: num }),
			async ctx => {
				await sendRow(ctx, 't', 'k', { a: 'g' })
				await sendRow(ctx, 't', 'k', {})
			}
		)
		expect(written).toEqual(['g=1', 'g=0'])
	})

	it('a stream-table join looks up a table-table join or transformValues() whose callback returned undefined: no value', async () => {
		for (const op of ['join', 'transformValues'] as const) {
			const [written] = await outputs(
				(driver, absent) => {
					const left = driver.table('l', S)
					const lookUp = <V>(table: KTable<string, V>) =>
						driver
							.input('in', S)
							.join(table, (s, t) => `${s}:${t}`)
							.to('out', S)
					// Not materialized: the stream-table join reads them through their value getters.
					if (op === 'join') {
						lookUp(left.leftJoin(driver.table('r', S), (a, b) => (b === 'none' ? absent : `${a}+${b}`)))
					} else {
						lookUp(
							left.transformValues(() => ({
								transform: (_key, value) => (value === 'y' ? absent : value),
							}))
						)
					}
				},
				async ctx => {
					await sendString(ctx, 'l', 'k', 'y')
					await sendString(ctx, 'r', 'k', 'none')
					await sendString(ctx, 'l', 'j', 'z')
					await sendString(ctx, 'r', 'j', 'w')
					await sendString(ctx, 'in', 'k', 's')
					await sendString(ctx, 'in', 'j', 's')
				}
			)
			expect(written, op).toEqual([op === 'join' ? 'j=s:z+w' : 'j=s:z'])
		}
	})

	it('transformValues() and a table-table join feeding groupBy(): a row that was or became undefined is no row', async () => {
		for (const op of ['transformValues', 'join'] as const) {
			const [written] = await outputs(
				(driver, absent) => {
					const table = driver.table('t', S)
					const derived =
						op === 'transformValues'
							? table.transformValues(() => ({
									transform: (_key, value) => (value === 'x' ? absent : value),
								}))
							: table.leftJoin(driver.table('r', S), (a, b) => (a === 'x' ? absent : `${a}${b ?? ''}`))
					// groupBy() asks for old values: the old row is transformed (joined) again, to undefined here.
					derived
						.groupBy((key, value) => [(value as string).toUpperCase(), key], S)
						.count()
						.toStream()
						.to('out', { key: str, value: num })
				},
				async ctx => {
					await sendString(ctx, 't', 'k', 'x')
					await sendString(ctx, 't', 'k', 'g')
				}
			)
			expect(written, op).toEqual(['G=1'])
		}
	})

	it('stream-table and stream-stream joins drop a stream record mapped to undefined', async () => {
		const [written] = await outputs(
			(driver, absent) => {
				const stream = driver.input('in', rows).mapValues(row => row?.a ?? absent)
				stream.join(driver.table('t', S), (a, b) => `${a}+${b}`).to('out', S)
				stream.leftJoin(driver.table('t2', S), (a, b) => `${a}+${b ?? 'none'}`).to('out', S)
				stream
					.leftJoin(driver.input('other', S), (a, b) => `${a}*${b ?? 'none'}`, JoinWindows.of(10), {
						value: str,
						otherValue: str,
					})
					.to('out', S)
			},
			async ctx => {
				await sendString(ctx, 't', 'k', 'x')
				await sendString(ctx, 'other', 'k', 'o')
				await sendRow(ctx, 'in', 'k', {})
				await sendRow(ctx, 'in', 'k', { a: 'y' })
			}
		)
		expect(written).toEqual(['k=y+x', 'k=y+none', 'k=y*o'])
	})
})
