import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient } from '@kafkats/client'
import { codec, flow } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { newClient, readAllCommitted } from './eos-test-util.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

describe('exactly_once: transaction aborted by the broker (transaction timeout)', () => {
	let client: KafkaClient

	beforeAll(async () => {
		client = newClient('flow-eos-txn-timeout')
		await client.connect()
	})

	afterAll(async () => {
		await client.disconnect()
	})

	// When a transaction outlives transactionTimeoutMs (stalled broker, frozen process)
	// the broker aborts it and answers the producer with ProducerFenced / InvalidProducerEpoch.
	// The worker must not stop for good: it drops the uncommitted state, re-initializes the
	// producer and reprocesses from the committed offsets, with exact results.
	it('recovers and produces exact counts', async () => {
		const input = uniqueTopicName('flow-it-txn-timeout-in')
		const out = uniqueTopicName('flow-it-txn-timeout-out')
		await createTopics(client, [{ name: input }, { name: out }])

		let stalled = false
		const app = flow({
			applicationId: uniqueTopicName('flow-it-txn-timeout'),
			client,
			numStreamThreads: 1,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { maxWaitMs: 100 },
			producer: { transactionTimeoutMs: 2_000 },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.peek((_, v) => {
				// Freeze the process (like a long GC pause) with the transaction open until the broker
				// has aborted it (it scans for timed-out transactions every 10s by default).
				if (v === 3 && !stalled) {
					stalled = true
					Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 14_000)
				}
			})
			.groupByKey()
			.count({ key: stringCodec, value: numberCodec })
			.toStream()
			.to(out, { key: stringCodec, value: numberCodec })

		const producer = client.producer({ lingerMs: 0 })
		try {
			await app.start()
			for (let i = 1; i <= 6; i++) {
				await producer.send(input, { key: 'a', value: numberCodec.encode(i) })
			}

			const deadline = Date.now() + 60_000
			let counts: number[] = []
			while (Date.now() < deadline) {
				counts = (await readAllCommitted(client, out, 800)).map(m => Number(m.value))
				if (counts.includes(6) || app.state() === "ERROR") break
			}
			expect(app.state()).toBe('RUNNING')
			expect(stalled).toBe(true)
			expect(counts).toEqual([1, 2, 3, 4, 5, 6])
		} finally {
			await producer.disconnect()
			await app.close()
		}
	}, 120_000)
})
