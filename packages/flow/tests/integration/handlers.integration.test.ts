/**
 * The production exception handler against the broker (3.14, 4.6.5; DL7c): a sink record the broker rejects (3 MB,
 * above its message size limit) is dropped under at-least-once when the handler answers 'continue', and the records
 * after it are processed and committed. Under exactly-once a failed send poisons the transaction: the app stops
 * (ERROR) whatever the handler answers, and nothing of that transaction is committed.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '../../src/api/app.js'
import { FailedProcessingError } from '../../src/api/errors.js'
import type { ProductionExceptionHandler } from '../../src/api/handlers.js'
import { flow } from '../../src/app/flow.js'
import {
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()

let client: KafkaClient
let producer: Producer

/** `input` copied to `out`; the value 'huge' becomes 3 MB at the sink. */
function copying(applicationId: string, input: string, out: string, config: Partial<FlowConfig>): FlowApp {
	const app = flow({ applicationId, client, consumer: { maxWaitMs: 100 }, producer: { lingerMs: 0 }, ...config })
	app.stream(input, { key: str, value: str }).to(out, {
		key: str,
		value: { encode: v => (v === 'huge' ? Buffer.alloc(3_000_000) : str.encode(v)), decode: str.decode },
	})
	return app
}

async function produce(input: string, values: readonly string[]): Promise<void> {
	await producer.send(
		input,
		values.map(value => ({ key: 'k', value: Buffer.from(value) }))
	)
}

describe('production exception handler', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('handlers')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	it("at_least_once, 'continue': the rejected record is dropped, the later ones are processed and committed", async () => {
		const input = uniqueTopicName('handlers-alos-in')
		const out = uniqueTopicName('handlers-alos-out')
		const applicationId = uniqueTopicName('handlers-alos')
		await createTopics(client, [{ name: input }, { name: out }])
		const calls: { topic: string; offset: bigint; origin: string }[] = []
		const handler: ProductionExceptionHandler = (context, record, _error, origin) => {
			calls.push({ topic: record.topic, offset: context.offset, origin })
			return 'continue'
		}
		const app = copying(applicationId, input, out, { productionExceptionHandler: handler, commitIntervalMs: 200 })
		try {
			await app.start()
			await produce(input, ['a', 'huge', 'b', 'c'])
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(200)) {
				if ((await fetchCommitted(client, applicationId, input)).offset === 4n) break
			}
			expect((await fetchCommitted(client, applicationId, input)).offset).toBe(4n)
			expect((await readAllCommitted(client, out, 500)).map(m => m.value!.toString())).toEqual(['a', 'b', 'c'])
			// The handler saw the failed send of the input record at offset 1.
			expect(calls).toEqual([{ topic: out, offset: 1n, origin: 'send' }])
			expect(app.state()).toBe('RUNNING')
			expect(app.getError()).toBeNull()
		} finally {
			await app.close()
		}
	}, 60_000)

	it("exactly_once, 'continue': the rejected record stops the app; nothing of its transaction is committed", async () => {
		const input = uniqueTopicName('handlers-eos-in')
		const out = uniqueTopicName('handlers-eos-out')
		const applicationId = uniqueTopicName('handlers-eos')
		await createTopics(client, [{ name: input }, { name: out }])
		let calls = 0
		const app = copying(applicationId, input, out, {
			processingGuarantee: 'exactly_once',
			productionExceptionHandler: () => {
				calls += 1
				return 'continue'
			},
		})
		try {
			await app.start()
			await produce(input, ['a'])
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(200)) {
				if ((await readAllCommitted(client, out, 300)).length >= 1) break
			}
			await produce(input, ['huge', 'b'])
			await waitUntil('ERROR', () => app.state() === 'ERROR', 20_000)
			expect(app.getError()).toBeInstanceOf(FailedProcessingError)
			expect(calls).toBe(0)
			await sleep(1000)
			expect((await readAllCommitted(client, out, 500)).map(m => m.value!.toString())).toEqual(['a'])
			expect((await fetchCommitted(client, applicationId, input)).offset).toBe(1n)
		} finally {
			await app.close()
		}
	}, 60_000)
})
