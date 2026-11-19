/**
 * Round 3 / attack 7: stream time across a restart. The window aggregation's clock (the late-record clock) is committed
 * with the offsets and seeded from them (D5), so a record that the failure-free run drops as late (its window closed
 * more than the grace period ago) is dropped by a run with a restart in the middle too. A clock that started from 0
 * after a restart or reassignment would count it and re-emit its window.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { TimeWindows, codec, flow, type FlowApp } from '../../src/index.js'
import { createTopics, newClient, readAllCommitted, sleep, uniqueTopicName } from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()

describe('hardening: stream time survives a restart', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-st-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	const build = (applicationId: string, input: string, output: string): FlowApp => {
		const app = flow({
			applicationId,
			client: newClient('r3-st-app'),
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		app.stream(input, { key: str, value: str })
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeAndGrace('1s', '23s'))
			.count({ storeName: 'w', value: num })
			.toStream()
			.selectKey(k => `${k!.key}@${k!.window.start % 1_000_000}`)
			.to(output, { key: str, value: num })
		return app
	}

	/** An output row: the count of the window starting at `windowStart`. */
	const row = (windowStart: number, count: number) => `a@${windowStart % 1_000_000}=${count}`

	async function run(restart: boolean, lateTs: (base: number) => number[], base: number): Promise<string[]> {
		const input = uniqueTopicName('r3-st-in')
		const output = uniqueTopicName('r3-st-out')
		const applicationId = uniqueTopicName('r3-st-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		let app = build(applicationId, input, output)
		try {
			await app.start()
			const send = async (ts: number[]) => {
				for (const t of ts) {
					await producer.send(input, { key: 'a', value: Buffer.from('x'), timestamp: new Date(t) })
				}
				await sleep(1_500)
			}
			// W(base) gets 2 records, then stream time jumps 100s: W(base) closed 76s ago (its end + 23s grace)
			await send([base, base + 200, base + 100_000])
			if (restart) {
				await app.close()
				app = build(applicationId, input, output)
				await app.start()
				await sleep(1_000)
			}
			await send(lateTs(base))
			const rows = await readAllCommitted(admin, output, 1_500)
			return rows.map(r => `${r.key!.toString()}=${num.decode(r.value!)}`).sort()
		} finally {
			await app.close().catch(() => {})
		}
	}

	it('late record of a closed window: restart run == failure-free run, which drops it', async () => {
		const lateTs = (base: number) => [base + 300] // 99.7s behind stream time: W(base) closed long ago
		const base = Math.floor(Date.now() / 1000) * 1000
		const free = await run(false, lateTs, base)
		expect(free).toEqual([row(base, 1), row(base, 2), row(base + 100_000, 1)].sort())
		const restarted = await run(true, lateTs, base)
		expect(restarted).toEqual(free)
	}, 80_000)

	it('late record within the grace period: restart run == failure-free run, which counts it (guard)', async () => {
		const lateTs = (base: number) => [base + 100_000 - 5_000] // W(base + 95s) is open until base + 119s
		const base = Math.floor(Date.now() / 1000) * 1000
		const free = await run(false, lateTs, base)
		expect(free).toContain(row(base + 95_000, 1))
		const restarted = await run(true, lateTs, base)
		expect(restarted).toEqual(free)
	}, 80_000)
})
