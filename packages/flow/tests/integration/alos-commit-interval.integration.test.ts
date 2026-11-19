/**
 * At-least-once offsets are committed by flow (D6, BC3, 4.8.1, 4.11; runtime§6.4): every `commitIntervalMs` at a
 * barrier between slices, never by the client's auto-commit; close() commits what was processed (each queue's head)
 * and leaves what was only fetched for the next run.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import {
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	newClient,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()

let client: KafkaClient
let producer: Producer

/** A stateless app over `input` (one partition) that records what it processed; `busyMs` per record. */
function recording(applicationId: string, input: string, commitIntervalMs: number, busyMs = 0) {
	const seen: number[] = []
	const app: FlowApp = flow({ applicationId, client, commitIntervalMs, consumer: { maxWaitMs: 100 } })
	app.stream(input, { key: str, value: str }).peek((_key, value) => {
		seen.push(Number(value))
		for (const until = Date.now() + busyMs; Date.now() < until;) {
			// a slow processor
		}
	})
	return { app, seen }
}

async function produce(input: string, from: number, to: number): Promise<void> {
	await producer.send(
		input,
		Array.from({ length: to - from }, (_, i) => ({ key: 'k', value: Buffer.from(String(from + i)) }))
	)
}

const committedOffset = async (groupId: string, topic: string): Promise<bigint> =>
	(await fetchCommitted(client, groupId, topic)).offset

describe('at-least-once commit interval', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('alos-interval')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	it('commits the processed offsets every commitIntervalMs', async () => {
		const input = uniqueTopicName('alos-interval-in')
		const applicationId = uniqueTopicName('alos-interval')
		await createTopics(client, [{ name: input }])
		const { app, seen } = recording(applicationId, input, 1000)
		try {
			await app.start()
			for (const end of [10, 20, 30]) {
				await produce(input, end - 10, end)
				await waitUntil(`${end} processed`, () => seen.length >= end, 15_000)
				const processedAt = Date.now()
				for (const deadline = processedAt + 5000; Date.now() < deadline; await sleep(100)) {
					if ((await committedOffset(applicationId, input)) === BigInt(end)) break
				}
				expect(await committedOffset(applicationId, input)).toBe(BigInt(end))
				// Within about one interval (plus the barrier and the request), sooner than an auto-commit (5 s).
				expect(Date.now() - processedAt).toBeLessThan(4000)
			}
		} finally {
			await app.close()
		}
	}, 60_000)

	it('commits nothing between intervals (no client auto-commit); close() commits what was processed', async () => {
		const input = uniqueTopicName('alos-long-interval-in')
		const applicationId = uniqueTopicName('alos-long-interval')
		await createTopics(client, [{ name: input }])
		const { app, seen } = recording(applicationId, input, 60_000)
		try {
			await app.start()
			await produce(input, 0, 10)
			await waitUntil('10 processed', () => seen.length >= 10, 15_000)
			// Longer than the client's auto-commit interval (5 s): only the start position is committed.
			await sleep(6000)
			expect(await committedOffset(applicationId, input)).toBeLessThanOrEqual(0n)
		} finally {
			await app.close()
		}
		expect(await committedOffset(applicationId, input)).toBe(10n)
	}, 60_000)

	it('close() in the middle of a fetched batch commits the processed records only; the next run goes on from there', async () => {
		const input = uniqueTopicName('alos-close-in')
		const applicationId = uniqueTopicName('alos-close')
		await createTopics(client, [{ name: input }])
		const N = 300
		await produce(input, 0, N)
		const first = recording(applicationId, input, 60_000, 10)
		await first.app.start()
		await waitUntil('20 processed', () => first.seen.length >= 20, 20_000)
		await first.app.close()
		expect(first.seen.length).toBeLessThan(N)
		expect(await committedOffset(applicationId, input)).toBe(BigInt(first.seen.length))

		const second = recording(applicationId, input, 60_000)
		try {
			await second.app.start()
			await waitUntil('the rest processed', () => second.seen.length >= N - first.seen.length, 20_000)
		} finally {
			await second.app.close()
		}
		// Exactly the records the first run did not process, each once: nothing skipped, nothing repeated.
		expect([...first.seen, ...second.seen]).toEqual(Array.from({ length: N }, (_, i) => i))
	}, 60_000)
})
