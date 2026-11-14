import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, TimeWindows, type FlowApp } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { DurableMemoryProvider, injectCommitFailures, newClient, readAllCommitted, sleep } from './round2-util.js'

const stringCodec = codec.string()

describe('round2: store expiry vs aborted transactions (durable local state)', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-exp-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	// BUG (F3): BufferedWindowStore/BufferedSessionStore.expireOldWindows/expireOldSessions delete from the
	// *underlying* store immediately, outside the transaction buffer. When the transaction aborts, the
	// committed windows it expired are gone locally but still live in the changelog. With a provider that
	// keeps local state across worker restarts (checkpoint store present => no wipe, restore resumes from the
	// checkpoint) the replayed records no longer find their join partners.
	const scenario = async (inject: boolean) => {
		const left = uniqueTopicName('r2-exp-left')
		const right = uniqueTopicName('r2-exp-right')
		const output = uniqueTopicName('r2-exp-out')
		const applicationId = uniqueTopicName('r2-exp-app')
		await createTopics(admin, [{ name: left }, { name: right }, { name: output }])
		const base = 1_700_000_000_000

		const client = newClient('r2-exp-app')
		let armed = false
		let fired = 0
		injectCommitFailures(client, () => inject && armed && fired++ < 1)
		const app: FlowApp = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 300,
			stateStoreProvider: new DurableMemoryProvider(),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		app.stream(left, { key: stringCodec, value: stringCodec })
			.join(app.stream(right, { key: stringCodec, value: stringCodec }), (l, r) => `${l}|${r}`, {
				within: TimeWindows.of(1_000),
			})
			.to(output, { key: stringCodec, value: stringCodec })
		try {
			await app.start()
			// tx1 (committed): a far-ahead left record first, then the old one that must be joined later.
			await producer.send(left, [
				{ key: 'm', value: Buffer.from('L0'), timestamp: new Date(base + 599_000) },
				{ key: 'k', value: Buffer.from('L1'), timestamp: new Date(base) },
			])
			await sleep(2_500)
			armed = true
			// tx2: R1 joins L1; R2 advances right stream time by 10 minutes, which expires L1 locally.
			await producer.send(right, [
				{ key: 'k', value: Buffer.from('R1'), timestamp: new Date(base + 500) },
				{ key: 'z', value: Buffer.from('R2'), timestamp: new Date(base + 600_000) },
			])
			await sleep(8_000)
			const out = (await readAllCommitted(admin, output)).map(m => `${m.key!.toString()}=${m.value!.toString()}`)
			if (inject) expect(fired).toBeGreaterThanOrEqual(2)
			expect(out).toEqual(['k=L1|R1'])
		} finally {
			await app.close()
		}
	}

	it('control: failure-free pass joins L1 with R1', () => scenario(false), 60_000)
	it('exactly_once join after an aborted commit equals a failure-free pass', () => scenario(true), 60_000)
})
