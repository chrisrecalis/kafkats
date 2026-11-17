/**
 * Final results of time windows under exactly-once against the broker: `suppress({ until: 'window-close' })` (6.16) and
 * `emit: 'on-window-close'` (6.8) forward each window once, after stream time passed its end plus grace, with its
 * final value, across a restart in the middle of a window and across aborted transactions. The suppression buffer and
 * the window store are restored from their changelogs, and the progress of emit on window close is committed with the
 * offsets, so a window is neither lost nor emitted twice.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, TimeWindows, type FlowApp } from '../../src/index.js'
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
const num = codec.json<number>()
const BASE = 1_700_000_000_000 // a multiple of the window size
const SIZE = 10_000
const GRACE = 2_000
/** Key of the records that only move stream time. */
const TICK = 'tick'

type Operator = 'suppress' | 'emit-on-window-close'
type Disturbance = 'restart mid-window' | 'aborted transactions'

describe('final results of time windows under exactly-once', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('final-results-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	function build(applicationId: string, input: string, output: string, operator: Operator, client: KafkaClient) {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		const windowed = app
			.stream(input, { key: str, value: str })
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeAndGrace(SIZE, GRACE))
		const counts =
			operator === 'suppress'
				? windowed.count({ storeName: 'counts', value: num }).suppress({ until: 'window-close' })
				: windowed.count({ storeName: 'counts', value: num, emit: 'on-window-close' })
		counts
			.toStream()
			.selectKey(key => `${key!.key}@${key!.window.start - BASE}`)
			.to(output, { key: str, value: num })
		return app
	}

	for (const operator of ['suppress', 'emit-on-window-close'] as const) {
		for (const disturbance of ['restart mid-window', 'aborted transactions'] as const) {
			it(`${operator}: each window is emitted once with its final count (${disturbance})`, async () => {
				await scenario(operator, disturbance)
			}, 90_000)
		}
	}

	async function scenario(operator: Operator, disturbance: Disturbance): Promise<void> {
		const input = uniqueTopicName('final-in')
		const output = uniqueTopicName('final-out')
		const applicationId = uniqueTopicName('final-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		let commits = 0
		const start = () => {
			const client = newClient('final-app')
			if (disturbance === 'aborted transactions') {
				injectCommitFailures(client, () => ++commits % 3 === 2 && commits <= 30)
			}
			const app = build(applicationId, input, output, operator, client)
			return app.start().then(() => app)
		}
		const restart = async (app: FlowApp) => {
			if (disturbance !== 'restart mid-window') return app
			await app.close()
			return start()
		}
		let sent = 0
		const send = async (records: Array<[key: string, offset: number]>) => {
			await producer.send(
				input,
				records.map(([key, offset]) => ({ key, value: Buffer.from('x'), timestamp: new Date(BASE + offset) }))
			)
			sent += records.length
		}
		const consumed = async () => {
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(200)) {
				if ((await fetchCommitted(admin, applicationId, input)).offset >= BigInt(sent)) return
			}
			throw new Error(`${sent} input records were not committed in time`)
		}
		/** The committed final results: `key@windowStart` to the counts emitted for it. */
		const results = async () => {
			const emitted = new Map<string, number[]>()
			for (const m of await readAllCommitted(admin, output, 800)) {
				const key = m.key!.toString()
				emitted.set(key, [...(emitted.get(key) ?? []), num.decode(m.value!)])
			}
			return emitted
		}
		/** Moves stream time to `offset` with tick records until `keys` were emitted: emission is throttled by wall clock. */
		const tickUntilEmitted = async (offset: number, keys: string[]) => {
			for (const deadline = Date.now() + 30_000; Date.now() < deadline; offset++) {
				await send([[TICK, offset]])
				await sleep(1_100)
				const emitted = await results()
				if (keys.every(key => emitted.has(key))) return
			}
			throw new Error(`${keys.join(', ')} were not emitted in time`)
		}

		let app = await start()
		try {
			// W0 = [0, 10 s) closes at 12 s of stream time.
			await send([
				['a', 1_000],
				['a', 2_000],
				['a', 3_000],
				['b', 4_000],
			])
			await consumed()
			expect(await results(), 'W0 is open').toEqual(new Map())

			app = await restart(app)
			await send([
				['a', 6_000],
				['c', 7_000],
				['a', 11_000], // W1 = [10 s, 20 s); W0 is still open (grace)
				['b', 15_000], // closes W0
			])
			await tickUntilEmitted(15_000, ['a@0', 'b@0', 'c@0'])

			app = await restart(app)
			await send([['a', 25_000]]) // closes W1
			await tickUntilEmitted(25_000, ['a@10000', 'b@10000', `${TICK}@10000`])
			await sleep(1_500)

			const emitted = await results()
			if (disturbance === 'aborted transactions') {
				expect(commits, 'commit failures were injected').toBeGreaterThanOrEqual(2)
			}
			for (const [key, counts] of emitted) {
				expect(counts, `${key} is emitted once`).toHaveLength(1)
			}
			const finals = new Map([...emitted].filter(([key]) => !key.startsWith(`${TICK}@`)))
			expect(finals).toEqual(
				new Map([
					['a@0', [4]],
					['b@0', [1]],
					['c@0', [1]],
					['a@10000', [1]],
					['b@10000', [1]],
				])
			)
			// W2 (a at 25 s, ticks) is still open.
			expect([...emitted.keys()].filter(key => key.endsWith('@20000'))).toEqual([])
		} finally {
			await app.close().catch(() => {})
		}
	}
})
