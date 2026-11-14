import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import { createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'
import { injectCommitFailures, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

describe('global tables and worker lifecycle', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-lc-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	it('global table: every lookup hits on 2 instances, pre-existing rows are loaded first, updates and tombstones propagate', async () => {
		const P = 4
		const table = uniqueTopicName('r2-gt-table')
		const events = uniqueTopicName('r2-gt-events')
		const output = uniqueTopicName('r2-gt-out')
		const applicationId = uniqueTopicName('r2-gt-app')
		await createTopics(admin, [{ name: table, partitions: P }, { name: events, partitions: P }, { name: output }])
		const ROWS = 300
		await producer.send(
			table,
			Array.from({ length: ROWS }, (_, i) => ({ key: `t${i}`, value: Buffer.from(`row${i}`) }))
		)
		// events live in a different partition than the table row with the same key
		await producer.send(
			events,
			Array.from({ length: ROWS }, (_, i) => ({
				key: `t${i}`,
				value: Buffer.from(`ev${i}`),
				partition: (i + 1) % P,
			}))
		)
		const build = (): FlowApp => {
			const app = flow({
				applicationId,
				client: newClient('r2-gt-app'),
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
			})
			const t = app.globalTable(table, { key: stringCodec, value: stringCodec })
			app.stream(events, { key: stringCodec, value: stringCodec })
				.join(t, (ev, row) => `${ev}+${row}`)
				.to(output, { key: stringCodec, value: stringCodec })
			return app
		}
		const a1 = build()
		const a2 = build()
		try {
			await Promise.all([a1.start(), a2.start()])
			let out = await readAllCommitted(admin, output)
			for (const deadline = Date.now() + 25_000; out.length < ROWS && Date.now() < deadline;) {
				await sleep(500)
				out = await readAllCommitted(admin, output, 800)
			}
			const got = new Map(out.map(m => [m.key!.toString(), m.value!.toString()]))
			expect(out).toHaveLength(ROWS)
			for (let i = 0; i < ROWS; i += 1) expect(got.get(`t${i}`)).toBe(`ev${i}+row${i}`)

			// update + tombstone after startup
			await producer.send(table, [
				{ key: 't0', value: Buffer.from('row0-v2') },
				{ key: 't1', value: null },
			])
			await sleep(2_000)
			await producer.send(events, [
				{ key: 't0', value: Buffer.from('again0'), partition: 3 },
				{ key: 't1', value: Buffer.from('again1'), partition: 2 },
				{ key: 't2', value: Buffer.from('again2'), partition: 1 },
			])
			await sleep(2_500)
			out = await readAllCommitted(admin, output)
			const later = out
				.slice(ROWS)
				.map(m => `${m.key!.toString()}=${m.value!.toString()}`)
				.sort()
			expect(later).toEqual(['t0=again0+row0-v2', 't2=again2+row2'])
		} finally {
			await a1.close().catch(() => {})
			await a2.close().catch(() => {})
		}
	}, 80_000)

	it('close() while a worker is in restart backoff returns promptly and leaves no extra handles', async () => {
		const input = uniqueTopicName('r2-lc-in')
		const output = uniqueTopicName('r2-lc-out')
		await createTopics(admin, [{ name: input }, { name: output }])
		await producer.send(input, [{ key: 'a', value: numberCodec.encode(1) }])
		const handlesBefore = (process as unknown as { _getActiveHandles(): unknown[] })._getActiveHandles().length
		for (let round = 0; round < 3; round += 1) {
			const client = newClient('r2-lc-app')
			injectCommitFailures(client, () => true) // every commit fails -> endless restarts
			const app = flow({
				applicationId: uniqueTopicName('r2-lc-app'),
				client,
				processingGuarantee: 'exactly_once',
				commitIntervalMs: 50,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
			})
			app.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'c', key: stringCodec, value: numberCodec })
				.toStream()
				.to(output, { key: stringCodec, value: numberCodec })
			await app.start()
			await sleep(2_500) // several failed commits -> worker is in backoff
			const t0 = Date.now()
			await app.close()
			expect(Date.now() - t0).toBeLessThan(5_000)
			expect(app.state()).toBe('STOPPED')
			await client.disconnect()
		}
		await sleep(500)
		const handlesAfter = (process as unknown as { _getActiveHandles(): unknown[] })._getActiveHandles().length
		expect(handlesAfter - handlesBefore).toBeLessThanOrEqual(2)
		expect(await readAllCommitted(admin, output, 800)).toHaveLength(0)
	}, 60_000)
})

describe('start() retry', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-rs-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	for (const owned of [true, false]) {
		it(`start() can be retried after it failed because the source topic did not exist (${owned ? 'owned' : 'shared'} client)`, async () => {
			const input = uniqueTopicName('r2-rs-in')
			const output = uniqueTopicName('r2-rs-out')
			await createTopics(admin, [{ name: output }])
			const shared = owned ? undefined : newClient('r2-rs-app')
			const app = flow({
				applicationId: uniqueTopicName('r2-rs-app'),
				client: shared ?? { brokers: requireKafkaBrokers(), clientId: uniqueTopicName('r2-rs-owned') },
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
			})
			app.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'c', key: stringCodec, value: numberCodec })
				.toStream()
				.to(output, { key: stringCodec, value: numberCodec })
			try {
				await expect(app.start()).rejects.toThrow(/not found|does not exist|topic/i)
				expect(app.state()).toBe('ERROR')
				await createTopics(admin, [{ name: input }])
				await producer.send(input, [{ key: 'a', value: numberCodec.encode(1) }])
				await app.start()
				await sleep(2_500)
				const out = await readAllCommitted(admin, output)
				expect(out.map(m => `${m.key!.toString()}=${numberCodec.decode(m.value!)}`)).toEqual(['a=1'])
				// BUG (F6): doStart() never resets currentState, so a successful retry still reports ERROR.
				expect(app.state()).toBe('RUNNING')
			} finally {
				await app.close().catch(() => {})
				await shared?.disconnect().catch(() => {})
			}
		}, 40_000)
	}
})
