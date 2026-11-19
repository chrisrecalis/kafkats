import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { ErrorCode, KafkaError, type KafkaClient, type Producer } from '@kafkats/client'
import { codec, FailedProcessingError, flow, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	requireKafkaBrokers,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

describe('global tables and app lifecycle', () => {
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

	it('global table: a row that cannot be decoded after startup stops the app and its stream processing', async () => {
		const table = uniqueTopicName('r2-gt-bad-table')
		const events = uniqueTopicName('r2-gt-bad-events')
		const output = uniqueTopicName('r2-gt-bad-out')
		await createTopics(admin, [{ name: table }, { name: events }, { name: output }])
		await producer.send(table, { key: 'k', value: Buffer.from('1') })

		const app = flow({
			applicationId: uniqueTopicName('r2-gt-bad-app'),
			client: newClient('r2-gt-bad-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const rows = app.globalTable(table, { key: stringCodec, value: numberCodec })
		app.stream(events, { key: stringCodec, value: stringCodec })
			.join(rows, (event, row) => `${event}+${row}`)
			.to(output, { key: stringCodec, value: stringCodec })
		const joined = async (): Promise<string[]> =>
			(await readAllCommitted(admin, output, 800)).map(message => message.value!.toString())
		try {
			await app.start()
			await producer.send(events, { key: 'k', value: Buffer.from('before') })
			for (const deadline = Date.now() + 10_000; (await joined()).length < 1 && Date.now() < deadline;) {
				await sleep(200)
			}
			expect(await joined()).toEqual(['before+1'])

			await producer.send(table, { key: 'k', value: Buffer.from('not json') })
			for (const deadline = Date.now() + 10_000; app.state() !== 'ERROR' && Date.now() < deadline;) {
				await sleep(100)
			}
			expect(app.state()).toBe('ERROR')
			// The deserialization handler's 'fail' (the default) wraps the codec's error (4.6.3).
			expect(app.getError()).toBeInstanceOf(FailedProcessingError)
			expect(app.getError()?.cause).toBeInstanceOf(SyntaxError)

			// The table no longer advances, so nothing more may be joined against it.
			await producer.send(events, { key: 'k', value: Buffer.from('after') })
			await sleep(3_000)
			expect(await joined()).toEqual(['before+1'])
		} finally {
			await app.close().catch(() => {})
		}
	}, 40_000)

	it('global table: a row that cannot be decoded in the last batch of the initial load fails start()', async () => {
		const table = uniqueTopicName('r2-gt-bad-load-table')
		const events = uniqueTopicName('r2-gt-bad-load-events')
		const output = uniqueTopicName('r2-gt-bad-load-out')
		await createTopics(admin, [{ name: table }, { name: events }, { name: output }])
		await producer.send(table, [
			{ key: 'a', value: Buffer.from('1') },
			{ key: 'b', value: Buffer.from('not json') },
		])

		const app = flow({
			applicationId: uniqueTopicName('r2-gt-bad-load-app'),
			client: newClient('r2-gt-bad-load-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const rows = app.globalTable(table, { key: stringCodec, value: numberCodec })
		app.stream(events, { key: stringCodec, value: stringCodec })
			.join(rows, (event, row) => `${event}+${row}`)
			.to(output, { key: stringCodec, value: stringCodec })
		try {
			await expect(app.start()).rejects.toBeInstanceOf(FailedProcessingError)
			expect(app.getError()?.cause).toBeInstanceOf(SyntaxError)
			expect(app.state()).toBe('ERROR')
		} finally {
			await app.close().catch(() => {})
		}
	}, 40_000)

	it('a fatal error while restoring a task assigned anew puts the app in ERROR instead of restarting forever', async () => {
		const input = uniqueTopicName('r2-fatal-restore-in')
		const output = uniqueTopicName('r2-fatal-restore-out')
		const applicationId = uniqueTopicName('r2-fatal-restore-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: output }])
		// Once armed, every restore consumer of the first instance (groupId `<appId>-restore-<taskId>-<n>`, DL14)
		// fails with an error that no restart can fix.
		const denied = new KafkaError('not authorized to read the changelog', ErrorCode.TopicAuthorizationFailed)
		let armed = false
		let restores = 0
		const firstClient = newClient('r2-fatal-restore-app')
		const createConsumer = firstClient.consumer.bind(firstClient)
		firstClient.consumer = config => {
			const consumer = createConsumer(config)
			if (armed && config.groupId.startsWith(`${applicationId}-restore-`)) {
				restores += 1
				consumer.runBatch = () => Promise.reject(denied)
			}
			return consumer
		}
		const build = (client: KafkaClient): FlowApp => {
			const app = flow({
				applicationId,
				client,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50, sessionTimeoutMs: 6_000 },
				producer: { lingerMs: 0 },
			})
			app.stream(input, { key: stringCodec, value: stringCodec })
				.groupByKey()
				.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(output, { key: stringCodec, value: numberCodec })
			return app
		}
		const first = build(firstClient)
		const second = build(newClient('r2-fatal-restore-app'))
		try {
			await first.start()
			// The second instance joins and takes one of the two tasks; both count a record per partition, so the
			// changelog of the second instance's task moves past what the first one has of it.
			await second.start()
			await producer.send(input, [
				{ key: 'a', value: Buffer.from('x'), partition: 0 },
				{ key: 'b', value: Buffer.from('x'), partition: 1 },
			])
			for (const deadline = Date.now() + 20_000; Date.now() < deadline;) {
				if ((await readAllCommitted(admin, output, 500)).length >= 2) break
			}
			expect(await readAllCommitted(admin, output, 500)).toHaveLength(2)

			// The second instance leaves: the first one gets its task anew and must restore it.
			armed = true
			await second.close()
			for (const deadline = Date.now() + 20_000; first.state() !== 'ERROR' && Date.now() < deadline;) {
				await sleep(100)
			}
			expect(restores).toBe(1)
			expect(first.state()).toBe('ERROR')
			expect(first.getError()).toBe(denied)
		} finally {
			await first.close().catch(() => {})
			await second.close().catch(() => {})
			await firstClient.disconnect().catch(() => {})
		}
	}, 60_000)

	it('close() during the recovery backoff returns promptly and leaves no extra handles', async () => {
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
			await sleep(2_500) // several failed commits -> the app is in its recovery backoff
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

	it('at-least-once: close() stops inside a batch and the next start resumes after what was acknowledged', async () => {
		const input = uniqueTopicName('r2-midbatch-in')
		const applicationId = uniqueTopicName('r2-midbatch-app')
		await createTopics(admin, [{ name: input }])
		const N = 300
		await producer.send(
			input,
			Array.from({ length: N }, (_, i) => ({ key: 'k', value: Buffer.from(String(i)) }))
		)
		const run = (seen: number[]): FlowApp => {
			const app = flow({
				applicationId,
				client: newClient('r2-midbatch-app'),
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			})
			app.stream(input, { key: stringCodec, value: stringCodec }).peek((_, value) => {
				seen.push(Number(value))
				// A slow processor: the whole batch takes several seconds.
				for (const until = Date.now() + 10; Date.now() < until;) {
					// busy
				}
			})
			return app
		}

		const first: number[] = []
		const a1 = run(first)
		await a1.start()
		for (const deadline = Date.now() + 20_000; first.length < 20 && Date.now() < deadline;) await sleep(20)
		await a1.close()
		expect(first.length).toBeGreaterThanOrEqual(20)
		// It did not work through the rest of the batch first.
		expect(first.length).toBeLessThan(N)

		const second: number[] = []
		const a2 = run(second)
		try {
			await a2.start()
			for (const deadline = Date.now() + 30_000; !second.includes(N - 1) && Date.now() < deadline;)
				await sleep(50)
		} finally {
			await a2.close()
		}
		// Nothing skipped, and what the first run acknowledged is not processed again.
		expect(second[0]).toBeLessThanOrEqual(first.length)
		expect(second[0]).toBeGreaterThan(0)
		expect(new Set([...first, ...second]).size).toBe(N)
	})

	it('at-least-once: the tasks of one instance process concurrently and count every record', async () => {
		const P = 4
		const input = uniqueTopicName('r2-conc-in')
		const output = uniqueTopicName('r2-conc-out')
		const applicationId = uniqueTopicName('r2-conc-app')
		await createTopics(admin, [
			{ name: input, partitions: P },
			{ name: output, partitions: P },
		])
		const PER_KEY = 200
		await producer.send(
			input,
			Array.from({ length: P * PER_KEY }, (_, i) => ({
				key: `k${i % P}`,
				value: Buffer.from('x'),
				partition: i % P,
			}))
		)
		const app = flow({
			applicationId,
			client: newClient('r2-conc-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
		})
		app.stream(input, { key: stringCodec, value: stringCodec })
			.groupByKey()
			.count({ key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			const latest = new Map<string, number>()
			for (const deadline = Date.now() + 30_000; Date.now() < deadline;) {
				for (const m of await readAllCommitted(admin, output)) {
					latest.set(m.key!.toString(), JSON.parse(m.value!.toString()) as number)
				}
				if (latest.size === P && [...latest.values()].every(count => count === PER_KEY)) break
				await sleep(300)
			}
			expect([...latest.entries()].sort()).toEqual(Array.from({ length: P }, (_, i) => [`k${i}`, PER_KEY]))
		} finally {
			await app.close()
		}
	})

	it('at-least-once: the records of one task (two topics, one store) are processed one at a time', async () => {
		const left = uniqueTopicName('r2-conc-merge-a')
		const right = uniqueTopicName('r2-conc-merge-b')
		const output = uniqueTopicName('r2-conc-merge-out')
		await createTopics(admin, [{ name: left }, { name: right }, { name: output }])
		// Both topics have the same key in partition 0: one count, kept in state partition 0.
		const PER_TOPIC = 300
		for (const topic of [left, right]) {
			await producer.send(
				topic,
				Array.from({ length: PER_TOPIC }, () => ({ key: 'k', value: Buffer.from('x') }))
			)
		}
		const app = flow({
			applicationId: uniqueTopicName('r2-conc-merge-app'),
			client: newClient('r2-conc-merge-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
		})
		app.stream(left, { key: stringCodec, value: stringCodec })
			.merge(app.stream(right, { key: stringCodec, value: stringCodec }))
			.groupByKey()
			.count({ key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			let counts: number[] = []
			for (const deadline = Date.now() + 20_000; counts.length < 2 * PER_TOPIC && Date.now() < deadline;) {
				await sleep(300)
				counts = (await readAllCommitted(admin, output, 800)).map(m => numberCodec.decode(m.value!))
			}
			// Every record adds one: no update is lost to a concurrent read of the old count.
			expect(counts).toEqual(Array.from({ length: 2 * PER_TOPIC }, (_, i) => i + 1))
		} finally {
			await app.close()
		}
	}, 60_000)
})
