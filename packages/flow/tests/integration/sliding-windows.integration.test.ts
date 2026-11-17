/**
 * Sliding-window aggregation (6.6, KS `KStreamSlidingWindowAggregate`) against the broker under exactly-once, with a
 * restart in the middle and aborted transactions: the committed results equal those of a failure-free run, which the
 * TestDriver computes from the same records (Kafka Streams' TopologyTestDriver as the oracle). Out-of-order records
 * within the grace period update the windows they belong to; records later than that are dropped.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import {
	codec,
	flow,
	SlidingWindows,
	WindowedCodecs,
	type EmitStrategy,
	type FlowApp,
	type KStream,
} from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'
import {
	createTopics,
	fetchCommitted,
	injectCommitFailures,
	lcg,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const list = codec.json<string[]>()
const BASE = 1_700_000_000_000
const DIFFERENCE = 1_000
const GRACE = 500
const windowedKey = WindowedCodecs.timeWindowed(str, DIFFERENCE)
/** Key of the records that only move stream time; their windows are ignored. */
const TICK = 'tick'

type Input = { key: string; value: string; timestamp: number }

/** The values of the records of each window, sorted (whatever order they came in), written to `output`. */
function topology(stream: KStream<string, string>, output: string, emit: EmitStrategy): void {
	stream
		.groupByKey()
		.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(DIFFERENCE, GRACE))
		.aggregate(
			() => [] as string[],
			(_key, value, values) => [...values, value].sort(),
			{ storeName: 'sliding', value: list, emit }
		)
		.toStream()
		.to(output, { value: list })
}

/** A result: `key [start, end] = values @ timestamp`. */
function describeResult(key: Buffer, value: Buffer | null, timestamp: bigint): string {
	const { key: base, window } = windowedKey.decode(key)
	const values = value === null ? 'null' : list.decode(value).join(',')
	return `${base} [${window.start - BASE}, ${window.end - BASE}] = ${values} @ ${Number(timestamp) - BASE}`
}

/**
 * Two keys, records about 150 ms apart with up to 2 s of jitter: 24 of the 40 come out of order, 5 of them later than
 * the grace period allows (dropped).
 */
function workload(): Input[] {
	const rnd = lcg(42)
	return Array.from({ length: 40 }, (_, i) => ({
		key: i % 3 === 0 ? 'b' : 'a',
		value: `v${i}`,
		timestamp: BASE + 2_000 + 150 * i + Math.floor(rnd() * 2_600) - 2_000,
	}))
}

/** The records later than the grace period: KS drops a record when its window `[t - d, t]` closed. */
function late(records: Input[]): Input[] {
	let streamTime = -1
	return records.filter(({ timestamp }) => {
		streamTime = Math.max(streamTime, timestamp)
		return timestamp + DIFFERENCE < streamTime - GRACE
	})
}

/** What a failure-free run produces from `records` (and then `ticks`), in order. */
async function oracle(records: Input[], emit: EmitStrategy, ticks: Input[] = []): Promise<string[]> {
	const driver = new TestDriver()
	topology(driver.input('in', { key: str, value: str }), 'out', emit)
	const results: string[] = []
	await driver.run(async ctx => {
		for (const { key, value, timestamp } of [...records, ...ticks]) {
			await ctx.send('in', value, { key: Buffer.from(key), codec: str, timestamp })
		}
		for (const { key, value, timestamp } of ctx.producer.messagesFor('out')) {
			results.push(describeResult(key!, value, timestamp!))
		}
	})
	return results
}

describe('sliding windows under exactly-once', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('sliding-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	async function run(emit: EmitStrategy, until: (results: string[]) => boolean): Promise<string[]> {
		const input = uniqueTopicName('sliding-in')
		const output = uniqueTopicName('sliding-out')
		const applicationId = uniqueTopicName('sliding-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		let commits = 0
		const start = async (): Promise<FlowApp> => {
			const client = newClient('sliding-app')
			injectCommitFailures(client, () => ++commits % 4 === 2 && commits <= 24)
			const app = flow({
				applicationId,
				client,
				processingGuarantee: 'exactly_once',
				commitIntervalMs: 100,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
				changelog: {
					restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 },
				},
			})
			topology(app.stream(input, { key: str, value: str }), output, emit)
			await app.start()
			return app
		}
		let sent = 0
		const send = async (records: Input[]) => {
			for (const { key, value, timestamp } of records) {
				await producer.send(input, { key, value: Buffer.from(value), timestamp: new Date(timestamp) })
				sent++
				await sleep(10)
			}
		}
		const consumed = async () => {
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(200)) {
				if ((await fetchCommitted(admin, applicationId, input)).offset >= BigInt(sent)) return
			}
			throw new Error('the input was not committed in time')
		}
		const results = async () =>
			(await readAllCommitted(admin, output, 800))
				.map(m => describeResult(m.key!, m.value, m.timestamp))
				.filter(result => !result.startsWith(`${TICK} `))

		const records = workload()
		let app = await start()
		try {
			await send(records.slice(0, 20))
			await consumed()
			await app.close()
			app = await start()
			await send(records.slice(20))
			await consumed()
			// Ticks far ahead close every window; emission on window close is throttled by wall clock.
			const last = Math.max(...records.map(r => r.timestamp))
			for (let i = 0, deadline = Date.now() + 30_000; Date.now() < deadline; i++) {
				if (until(await results())) break
				await send([{ key: TICK, value: 't', timestamp: last + 10_000 + i }])
				await sleep(1_000)
			}
			expect(commits, 'commit failures were injected').toBeGreaterThanOrEqual(2)
			return await results()
		} finally {
			await app.close().catch(() => {})
		}
	}

	it("emit 'on-update': the committed results are exactly those of a failure-free run, in order", async () => {
		const want = await oracle(workload(), 'on-update')
		// The late records are in no window.
		const dropped = late(workload()).map(record => record.value)
		expect(dropped).toHaveLength(5)
		const valuesOf = (result: string) => result.split(' = ')[1]!.split(' @ ')[0]!.split(',')
		expect(want.filter(result => valuesOf(result).some(value => dropped.includes(value)))).toEqual([])
		const got = await run('on-update', results => results.length >= want.length)
		expect(got).toEqual(want)
	}, 90_000)

	it("emit 'on-window-close': each closed window once, with the final values of a failure-free run", async () => {
		const records = workload()
		const tick = { key: TICK, value: 't', timestamp: Math.max(...records.map(r => r.timestamp)) + 10_000 }
		const want = (await oracle(records, 'on-window-close', [tick])).filter(r => !r.startsWith(`${TICK} `))
		const got = await run('on-window-close', results => results.length >= want.length)
		expect([...got].sort()).toEqual([...want].sort())
	}, 90_000)
})
