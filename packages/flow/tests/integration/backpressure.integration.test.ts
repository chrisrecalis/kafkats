/**
 * Backpressure across rebalances (D3, 4.5.1, 4.5.6; runtime§6.30, DL7d): with `bufferedRecordsPerPartition: 5`, a
 * partition whose queue fills up is paused and resumed once its task worked it off. A second instance joins and leaves
 * mid-run under exactly-once: the counts stay exact, and no partition stays paused: each one is committed further
 * within 10 s after each rebalance.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import {
	clientConfig,
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const PARTITIONS = 4
const KEYS = 8
/** Records per key written before the start: every queue overflows at once. */
const BACKLOG = 25

let admin: KafkaClient
let producer: Producer

/** One record per key; key `k<i>` lives on partition i % PARTITIONS. */
async function round(input: string): Promise<void> {
	await producer.send(
		input,
		Array.from({ length: KEYS }, (_, i) => ({ key: `k${i}`, value: Buffer.from('x'), partition: i % PARTITIONS }))
	)
}

async function committedOffsets(groupId: string, input: string): Promise<bigint[]> {
	return Promise.all(
		Array.from({ length: PARTITIONS }, async (_, p) => (await fetchCommitted(admin, groupId, input, p)).offset)
	)
}

/** Every partition's committed offset moves past where it is now within `timeoutMs` (records keep coming). */
async function everyPartitionProgresses(groupId: string, input: string, timeoutMs: number): Promise<void> {
	const from = await committedOffsets(groupId, input)
	let now = from
	for (const deadline = Date.now() + timeoutMs; Date.now() < deadline; await sleep(250)) {
		now = await committedOffsets(groupId, input)
		if (now.every((offset, p) => offset > from[p]!)) return
	}
	expect(
		now.map((offset, p) => offset > from[p]!),
		`committed offsets ${from.join(', ')} -> ${now.join(', ')}`
	).toEqual(now.map(() => true))
}

describe('backpressure', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		admin = newClient('backpressure-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
		expect(unhandled).toEqual([])
	})

	it('exactly_once with 5 buffered records per partition: an instance joins and leaves, counts stay exact', async () => {
		const input = uniqueTopicName('backpressure-in')
		const out = uniqueTopicName('backpressure-out')
		const applicationId = uniqueTopicName('backpressure-app')
		await createTopics(admin, [{ name: input, partitions: PARTITIONS }, { name: out }])
		const build = (): FlowApp => {
			const app = flow({
				applicationId,
				client: clientConfig('backpressure-app'),
				processingGuarantee: 'exactly_once',
				bufferedRecordsPerPartition: 5,
				consumer: { maxWaitMs: 100 },
				producer: { lingerMs: 0 },
			})
			app.stream(input, { key: str, value: str })
				.groupByKey()
				.count({ storeName: 'counts', key: str, value: num })
				.toStream()
				.to(out, { key: str, value: num })
			return app
		}
		let rounds = 0
		for (; rounds < BACKLOG; rounds++) await round(input)
		let pumping = true
		const pump = (async () => {
			for (; pumping; rounds++) {
				await round(input)
				await sleep(100)
			}
		})()

		const first = build()
		const second = build()
		try {
			await first.start()
			await everyPartitionProgresses(applicationId, input, 10_000)
			await second.start()
			await everyPartitionProgresses(applicationId, input, 10_000)
			await second.close()
			await everyPartitionProgresses(applicationId, input, 10_000)
		} finally {
			pumping = false
			await pump
			await second.close()
		}
		try {
			const want = Array.from({ length: rounds }, (_, c) => c + 1)
			let counts = new Map<string, number[]>()
			for (const deadline = Date.now() + 30_000; Date.now() < deadline; await sleep(250)) {
				counts = new Map()
				for (const m of await readAllCommitted(admin, out, 800)) {
					const key = m.key!.toString()
					counts.set(key, [...(counts.get(key) ?? []), num.decode(m.value!)])
				}
				if (counts.size === KEYS && [...counts.values()].every(seq => seq.length >= rounds)) break
			}
			for (let i = 0; i < KEYS; i++) expect(counts.get(`k${i}`), `k${i}`).toEqual(want)
		} finally {
			await first.close()
		}
	}, 120_000)
})
