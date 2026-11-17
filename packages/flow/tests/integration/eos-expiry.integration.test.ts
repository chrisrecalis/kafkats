import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, JoinWindows, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	DurableMemoryProvider,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const stringCodec = codec.string()

describe('store expiry vs aborted transactions (durable local state)', () => {
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

	// BUG (F3): expiry of join buffers deleted from the *underlying* store at once, outside the transaction buffer. When
	// the transaction aborted, the committed entries it expired were gone locally but still live in the changelog. With a
	// provider that keeps local state across recoveries (checkpoint store present => no wipe, restore resumes from the
	// checkpoint) the replayed records no longer found their join partners. Expiry now deletes only entries that were
	// past the retention as of the last commit, inside the transaction (6.2).
	//
	// The join buffers keep 40 s (1 s before + 1 s after + 38 s grace). tx1 commits L1 and L0, 30 s later. tx2 (failed
	// once, then replayed): R1 joins L1, then L2 moves the left buffer's clock to 65 s, past L1's retention but not L0's,
	// and starts an expiry pass (the clock moved more than a minute since the last one). The replayed R1 must find L1
	// again. L0 keeps the left buffer from being empty: a store found empty is rebuilt from its changelog, which would
	// repair a lost entry. R3, sent after tx2 committed, must not find L1: the join really expires it.
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
			commitIntervalMs: 1_000,
			stateStoreProvider: new DurableMemoryProvider(),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		app.stream(left, { key: stringCodec, value: stringCodec })
			.join(
				app.stream(right, { key: stringCodec, value: stringCodec }),
				(l, r) => `${l}|${r}`,
				JoinWindows.ofTimeDifferenceAndGrace('1s', '38s')
			)
			.to(output, { key: stringCodec, value: stringCodec })
		const writer = admin.producer({ transactionalId: uniqueTopicName('r2-exp-writer'), lingerMs: 0 })
		try {
			await app.start()
			// tx1 (committed): L1, which R1 joins later, and L0.
			await producer.send(left, [
				{ key: 'k', value: Buffer.from('L1'), timestamp: new Date(base) },
				{ key: 'm', value: Buffer.from('L0'), timestamp: new Date(base + 30_000) },
			])
			await sleep(2_500)
			armed = true
			// tx2: R1 and L2 become visible together (one producer transaction), so that one flow transaction processes
			// both, R1 first (the smaller timestamp).
			await writer.transaction(async tx => {
				await tx.send(right, { key: 'k', value: Buffer.from('R1'), timestamp: new Date(base + 500) })
				await tx.send(left, { key: 'z', value: Buffer.from('L2'), timestamp: new Date(base + 65_000) })
			})
			await sleep(6_000)
			// R3 is within L1's window, but L1 is past the left buffer's retention.
			await producer.send(right, { key: 'k', value: Buffer.from('R3'), timestamp: new Date(base + 700) })
			await sleep(3_000)
			const out = (await readAllCommitted(admin, output)).map(m => `${m.key!.toString()}=${m.value!.toString()}`)
			if (inject) expect(fired).toBeGreaterThanOrEqual(2)
			expect(out).toEqual(['k=L1|R1'])
		} finally {
			await app.close()
			await writer.disconnect()
		}
	}

	it('control: failure-free pass joins L1 with R1', () => scenario(false), 60_000)
	it('exactly_once join after an aborted commit equals a failure-free pass', () => scenario(true), 60_000)
})
