/**
 * Deferred left and outer stream-stream join results against the broker (6.10; Kafka Streams' spurious result fix): a
 * record of an outer side that matched nothing waits in the join's outer store until its window and grace closed by
 * stream time, then is joined with null, exactly once; a match within its window removes it instead. The outer store
 * and both join buffers are logged, so pending records survive a restart, and under exactly-once an aborted transaction
 * neither loses nor repeats a result.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, JoinWindows, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	fetchCommitted,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const BASE = 1_700_000_000_000
/** Key of the records that only move stream time; their own results are ignored. */
const TICK = 'tick'

describe('deferred stream-stream join results', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('deferred-joins-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	interface Topics {
		left: string
		right: string
		output: string
	}

	async function topics(prefix: string, partitions: number): Promise<Topics & { applicationId: string }> {
		const names = {
			left: uniqueTopicName(`${prefix}-left`),
			right: uniqueTopicName(`${prefix}-right`),
			output: uniqueTopicName(`${prefix}-out`),
			applicationId: uniqueTopicName(`${prefix}-app`),
		}
		await createTopics(admin, [
			{ name: names.left, partitions },
			{ name: names.right, partitions },
			{ name: names.output },
		])
		return names
	}

	function build(
		applicationId: string,
		{ left, right, output }: Topics,
		kind: 'leftJoin' | 'outerJoin',
		client = newClient('deferred-joins-app')
	): FlowApp {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		const leftStream = app.stream(left, { key: str, value: str })
		const rightStream = app.stream(right, { key: str, value: str })
		const joiner = (l: string | null, r: string | null) => `${l ?? 'null'}|${r ?? 'null'}`
		const windows = JoinWindows.ofTimeDifferenceAndGrace('1s', 0)
		const joined =
			kind === 'leftJoin'
				? leftStream.leftJoin(rightStream, joiner, windows)
				: leftStream.outerJoin(rightStream, joiner, windows)
		joined.to(output, { key: str, value: str })
		return app
	}

	/** The committed results without those of tick records, as `key=value`, sorted. */
	async function results(output: string): Promise<string[]> {
		return (await readAllCommitted(admin, output, 800))
			.map(m => `${m.key!.toString()}=${m.value!.toString()}`)
			.filter(row => !row.startsWith(`${TICK}=`))
			.sort()
	}

	/**
	 * Sends a tick record at `time` to every partition of `left` until `done` holds: stream time is per task, and each
	 * task scans its closed windows at most once a second of wall clock.
	 */
	async function tickUntil(
		left: string,
		partitions: number,
		time: number,
		done: () => Promise<boolean>,
		timeoutMs = 30_000
	): Promise<void> {
		const deadline = Date.now() + timeoutMs
		for (let tick = time; Date.now() < deadline; tick++) {
			for (let partition = 0; partition < partitions; partition++) {
				await producer.send(left, { key: TICK, value: Buffer.from('t'), partition, timestamp: new Date(tick) })
			}
			await sleep(1_100)
			if (await done()) return
		}
		throw new Error('the closed windows were not emitted in time')
	}

	/** Waits until the group committed `count` records of `topic` over its partitions. */
	async function committedInput(applicationId: string, topic: string, partitions: number, count: bigint) {
		const committed = async () => {
			let sum = 0n
			for (let partition = 0; partition < partitions; partition++) {
				const { offset } = await fetchCommitted(admin, applicationId, topic, partition)
				sum += offset > 0n ? offset : 0n
			}
			return sum
		}
		for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(200)) {
			if ((await committed()) >= count) return
		}
		expect(await committed(), `${topic} committed`).toBe(count)
	}

	it('a pending left record survives a restart: a match after the restart removes it, the others are joined with null once', async () => {
		const names = await topics('deferred-restart', 1)
		const send = (topic: string, key: string, value: string, offset: number) =>
			producer.send(topic, { key, value: Buffer.from(value), timestamp: new Date(BASE + offset) })
		let app = build(names.applicationId, names, 'leftJoin')
		try {
			await app.start()
			await send(names.left, 'a', 'La', 0)
			await send(names.left, 'b', 'Lb', 0)
			await send(names.left, 'c', 'Lc', 0)
			await send(names.right, 'a', 'Ra', 500)
			for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(300)) {
				if ((await results(names.output)).includes('a=La|Ra')) break
			}
			// Lb and Lc wait: their windows end at BASE + 1000.
			expect(await results(names.output)).toEqual(['a=La|Ra'])

			await app.close()
			app = build(names.applicationId, names, 'leftJoin')
			await app.start()
			await send(names.right, 'b', 'Rb', 800)
			// Ticks only once Rb was processed: a tick processed first would close Lb's window before Rb arrives.
			for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(300)) {
				if ((await results(names.output)).includes('b=Lb|Rb')) break
			}
			await tickUntil(names.left, 1, BASE + 1_001, async () => (await results(names.output)).length >= 3)
			await sleep(1_500)
			expect(await results(names.output)).toEqual(['a=La|Ra', 'b=Lb|Rb', 'c=Lc|null'])
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)

	it('outer join under aborted transactions: every record yields exactly one result', async () => {
		const P = 2
		const names = await topics('deferred-outer', P)
		const KEYS = 30
		const client = newClient('deferred-outer-app')
		let commits = 0
		injectCommitFailures(client, () => ++commits % 4 === 2 && commits <= 30)
		const app = build(names.applicationId, names, 'outerJoin', client)
		const want: string[] = []
		try {
			await app.start()
			// Key i: a left record at 10 i; a right record 300 ms later for two thirds of the keys (a match), else a right
			// record of another key (outer: joined with null on both sides). All of them lie within the join buffers'
			// retention (2 s), so the results do not depend on the order the two topics are read in.
			for (let i = 0; i < KEYS; i++) {
				const key = `k${i}`
				const time = new Date(BASE + 10 * i)
				await producer.send(names.left, { key, value: Buffer.from(`L${i}`), timestamp: time })
				if (i % 3 !== 2) {
					await producer.send(names.right, {
						key,
						value: Buffer.from(`R${i}`),
						timestamp: new Date(BASE + 10 * i + 300),
					})
					want.push(`${key}=L${i}|R${i}`)
				} else {
					await producer.send(names.right, { key: `r${i}`, value: Buffer.from(`R${i}`), timestamp: time })
					want.push(`${key}=L${i}|null`, `r${i}=null|R${i}`)
				}
				await sleep(20)
			}
			// Ticks only once every record was processed, so that none is late.
			await committedInput(names.applicationId, names.left, P, BigInt(KEYS))
			await committedInput(names.applicationId, names.right, P, BigInt(KEYS))
			await tickUntil(
				names.left,
				P,
				BASE + 10 * KEYS + 2_000,
				async () => (await results(names.output)).length >= want.length
			)
			await sleep(1_500)
			expect(commits, 'commit failures were injected').toBeGreaterThanOrEqual(2)
			expect(await results(names.output)).toEqual(want.sort())
		} finally {
			await app.close().catch(() => {})
		}
	}, 90_000)
})
