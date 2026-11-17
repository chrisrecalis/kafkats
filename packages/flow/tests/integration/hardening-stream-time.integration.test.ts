/**
 * Round 3 / attack 7: stream time across a restart. Stream time (the late-record clock) is held in memory per source
 * partition and restarts from 0 after a restart / reassignment, so a record that the failure-free run drops as "past
 * retention" is processed (and re-emitted) by a run with a restart in the middle.
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
			.windowedBy(TimeWindows.of(1_000))
			.count({ storeName: 'w', value: num })
			.toStream()
			.selectKey(k => `${k!.key}@${k!.window.start % 1_000_000}`)
			.to(output, { key: str, value: num })
		return app
	}

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
			// W(base) gets 2 records, then stream time jumps 100s (retention is 24s), then the first window is long expired
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

	it('late record beyond retention (window long expired): restart run == failure-free run', async () => {
		const lateTs = (base: number) => [base + 300] // 99.7s behind stream time: dropped by the failure-free run
		const base = Math.floor(Date.now() / 1000) * 1000
		const free = await run(false, lateTs, base)
		const restarted = await run(true, lateTs, base)
		expect(restarted).toEqual(free)
	}, 80_000)

	it('late record within retention: restart run == failure-free run (guard)', async () => {
		const lateTs = (base: number) => [base + 100_000 - 5_000]
		const base = Math.floor(Date.now() / 1000) * 1000
		const free = await run(false, lateTs, base)
		const restarted = await run(true, lateTs, base)
		expect(restarted).toEqual(free)
	}, 80_000)
})
