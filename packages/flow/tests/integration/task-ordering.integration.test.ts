/**
 * Record order inside a task (D4, 4.5.3, 4.5.4): the records of a task's partitions are processed in timestamp order,
 * the smallest head timestamp first, also across topics written before the start (Kafka Streams' KIP-695 readiness: a
 * task waits until every partition with data on the broker has records buffered). With `maxTaskIdleMs > 0`, a task
 * whose other partition is empty waits that long for a late producer before it processes out of order.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { string, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import {
	collectUnhandledRejections,
	createTopics,
	MultiTopicCollector,
	newClient,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const strings = { key: string(), value: string() }
const BASE = 1_700_000_000_000

let client: KafkaClient
let producer: Producer

/** `left` merged with `right` into `out` (one task: both topics have one partition); values are timestamps. */
function merging(left: string, right: string, out: string, maxTaskIdleMs: number): FlowApp {
	const app = flow({
		applicationId: uniqueTopicName('ordering-app'),
		client,
		maxTaskIdleMs,
		consumer: { maxWaitMs: 100 },
	})
	app.stream(left, strings).merge(app.stream(right, strings)).to(out, strings)
	return app
}

async function produce(topic: string, timestamps: readonly number[]): Promise<void> {
	await producer.send(
		topic,
		timestamps.map(ts => ({ key: 'k', value: Buffer.from(String(ts)), timestamp: new Date(ts) }))
	)
}

/** Topics of one test: two inputs with one partition each and the output, plus a reader of the output. */
async function topics(
	prefix: string
): Promise<{ left: string; right: string; out: string; output: MultiTopicCollector }> {
	const [left, right, out] = ['left', 'right', 'out'].map(name => uniqueTopicName(`${prefix}-${name}`)) as [
		string,
		string,
		string,
	]
	await createTopics(client, [{ name: left }, { name: right }, { name: out }])
	const output = new MultiTopicCollector({
		client,
		groupId: uniqueTopicName(`${prefix}-reader`),
		topics: [{ topic: out, valueCodec: string() }],
	})
	await output.start()
	return { left, right, out, output }
}

function values(output: MultiTopicCollector, out: string): number[] {
	return output.getTopicMessages<string, string>(out).map(m => Number(m.value))
}

describe('task ordering', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('task-ordering')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	it('merges two topics written before the start in timestamp order, not in the order they were written', async () => {
		const { left, right, out, output } = await topics('ordering-before')
		const N = 50
		// All of `left` is written before all of `right`, but their timestamps interleave.
		await produce(
			left,
			Array.from({ length: N }, (_, i) => BASE + 2 * i)
		)
		await produce(
			right,
			Array.from({ length: N }, (_, i) => BASE + 2 * i + 1)
		)
		const app = merging(left, right, out, 0)
		try {
			await app.start()
			await waitUntil('every record merged', () => values(output, out).length >= 2 * N, 30_000)
			expect(values(output, out)).toEqual(Array.from({ length: 2 * N }, (_, i) => BASE + i))
		} finally {
			await app.close()
			await output.stop()
		}
	}, 60_000)

	it('maxTaskIdleMs > 0: a record of a late producer of the other topic is processed first if it is older', async () => {
		const { left, right, out, output } = await topics('ordering-idle')
		const app = merging(left, right, out, 5000)
		try {
			await app.start()
			await sleep(1000)
			await produce(left, [BASE + 2000])
			// `right` is empty and caught up: the task idles for maxTaskIdleMs instead of processing `left` now.
			await sleep(1000)
			expect(values(output, out)).toEqual([])
			await produce(right, [BASE + 1000])
			await waitUntil('both records', () => values(output, out).length >= 2, 15_000)
			expect(values(output, out)).toEqual([BASE + 1000, BASE + 2000])
		} finally {
			await app.close()
			await output.stop()
		}
	}, 60_000)

	it('maxTaskIdleMs 0: what is buffered is processed at once, a later older record of the other topic after it', async () => {
		const { left, right, out, output } = await topics('ordering-no-idle')
		const app = merging(left, right, out, 0)
		try {
			await app.start()
			await sleep(1000)
			await produce(left, [BASE + 2000])
			await waitUntil('the record of left', () => values(output, out).length >= 1, 15_000)
			await produce(right, [BASE + 1000])
			await waitUntil('both records', () => values(output, out).length >= 2, 15_000)
			expect(values(output, out)).toEqual([BASE + 2000, BASE + 1000])
		} finally {
			await app.close()
			await output.stop()
		}
	}, 60_000)

	it('maxTaskIdleMs > 0: without a late record, the task processes after idling maxTaskIdleMs', async () => {
		const { left, right, out, output } = await topics('ordering-idle-bound')
		const idle = 3000
		const app = merging(left, right, out, idle)
		try {
			await app.start()
			await sleep(1000)
			const written = Date.now()
			await produce(left, [BASE])
			await waitUntil('the record of left', () => values(output, out).length >= 1, 15_000)
			const waited = Date.now() - written
			expect(waited).toBeGreaterThanOrEqual(idle - 500)
			expect(waited).toBeLessThan(idle + 5000)
		} finally {
			await app.close()
			await output.stop()
		}
	}, 60_000)
})
