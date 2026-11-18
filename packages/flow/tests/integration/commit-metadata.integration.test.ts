/**
 * Commit metadata and committed positions against the broker (D5, D6, 4.8.3, 4.8.5): committed offsets carry Kafka
 * Streams' TopicPartitionMetadata (the partition time and the processor clocks); the processor clocks are seeded from
 * it, so a restart decides lateness as the failure-free run does, under both guarantees; after an idle period the
 * exactly-once committed offset of a transactionally written input is its log end offset, past the last commit marker
 * (KIP-1094), so a restart never reads a marker again.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import { TimeWindows } from '../../src/dsl/windows.js'
import { decodeCommitMetadata } from '../../src/processor/metadata.js'
import {
	collectUnhandledRejections,
	createTopics,
	eventTimeBase,
	fetchCommitted,
	MultiTopicCollector,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const GUARANTEES = ['at_least_once', 'exactly_once'] as const
const BASE = eventTimeBase() // near the wall clock (retention), a multiple of the window size

let client: KafkaClient
let producer: Producer

async function logEnd(topic: string): Promise<bigint> {
	return (await client.admin().fetchTopicOffsets(topic, [0], 'latest')).get(0)!
}

describe('commit metadata', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('commit-metadata')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	describe.each(GUARANTEES)('%s', processingGuarantee => {
		it('committed offsets carry the partition time and the processor clock, which a restart starts from', async () => {
			const input = uniqueTopicName('metadata-in')
			const out = uniqueTopicName('metadata-out')
			const applicationId = uniqueTopicName('metadata-app')
			await createTopics(client, [{ name: input }, { name: out }])
			const output = new MultiTopicCollector({
				client,
				groupId: uniqueTopicName('metadata-reader'),
				topics: [{ topic: out, keyCodec: str, valueCodec: num }],
			})
			await output.start()
			const build = (): FlowApp => {
				const app = flow({
					applicationId,
					client,
					processingGuarantee,
					consumer: { maxWaitMs: 100 },
					producer: { lingerMs: 0 },
				})
				// A window accepts records until 23 s after its end (its grace period).
				app.stream(input, { key: str, value: str })
					.groupByKey()
					.windowedBy(TimeWindows.ofSizeAndGrace('1s', '23s'))
					.count({ storeName: 'w', value: num })
					.toStream()
					.selectKey(key => `${key!.key}@${key!.window.start - BASE}`)
					.to(out, { key: str, value: num })
				return app
			}
			const send = (timestamps: readonly number[]) =>
				producer.send(
					input,
					timestamps.map(ts => ({ key: 'a', value: Buffer.from('x'), timestamp: new Date(BASE + ts) }))
				)
			const results = () => output.getTopicMessages<string, number>(out).map(m => `${m.key}=${m.value}`)

			let app = build()
			try {
				await app.start()
				await send([0, 200, 100_000])
				await waitUntil('three counts', () => results().length >= 3, 30_000)
				await app.close()

				const { offset, metadata } = await fetchCommitted(client, applicationId, input)
				expect(offset).toBe(3n)
				const decoded = decodeCommitMetadata(metadata)
				expect(decoded.invalid).toBeUndefined()
				expect(decoded.partitionTime).toBe(BASE + 100_000)
				// The window aggregation's clock, keyed by its store: the newest record time it saw; and the store's
				// clock: the end of the newest window written to it (6.2).
				expect(new Map(decoded.metadata)).toEqual(
					new Map([
						['w', BASE + 100_000],
						['w.observed', BASE + 101_000],
					])
				)

				// The window [0, 1000) closed 76 s ago: the restarted run drops a record of it, as the failure-free
				// one would. A clock started from scratch would count it.
				app = build()
				await app.start()
				await send([300, 100_500])
				await waitUntil('the count of the later record', () => results().length >= 4, 30_000)
				await sleep(1000)
				expect(results()).toEqual(['a@0=1', 'a@0=2', 'a@100000=1', 'a@100000=2'])
			} finally {
				await app.close()
				await output.stop()
			}
		}, 90_000)
	})

	it('exactly_once: after an idle period the committed offset of a transactional input is its log end offset', async () => {
		const input = uniqueTopicName('metadata-markers-in')
		const out = uniqueTopicName('metadata-markers-out')
		const applicationId = uniqueTopicName('metadata-markers-app')
		await createTopics(client, [{ name: input }, { name: out }])
		const writer = client.producer({ transactionalId: uniqueTopicName('metadata-writer'), lingerMs: 0 })
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			consumer: { maxWaitMs: 100 },
			producer: { lingerMs: 0 },
		})
		app.stream(input, { key: str, value: str })
			.mapValues(value => value?.toUpperCase() ?? null)
			.to(out, { key: str, value: str })
		try {
			// Three transactions of five records: each ends with a commit marker.
			for (let t = 0; t < 3; t++) {
				await writer.transaction(async tx => {
					await tx.send(
						input,
						Array.from({ length: 5 }, (_, i) => ({ key: 'k', value: Buffer.from(`v${t}-${i}`) }))
					)
				})
			}
			// The broker writes the markers after the commits returned: the log ends past the last one soon.
			const end = 18n
			for (const deadline = Date.now() + 10_000; (await logEnd(input)) < end && Date.now() < deadline;) {
				await sleep(100)
			}
			expect(await logEnd(input)).toBe(end)
			await app.start()
			for (const deadline = Date.now() + 30_000; Date.now() < deadline; await sleep(200)) {
				if ((await readAllCommitted(client, out, 500)).length >= 15) break
			}
			expect(await readAllCommitted(client, out, 500)).toHaveLength(15)
			// Idle: the queue is empty and the consumer's position moved past the last marker, which is committed.
			await sleep(2000)
			expect((await fetchCommitted(client, applicationId, input)).offset).toBe(end)
		} finally {
			await app.close()
			await writer.disconnect()
		}
	}, 60_000)
})
