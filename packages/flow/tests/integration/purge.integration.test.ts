/**
 * Internal topics against the broker (4.1.5, 4.8.6, BC20): records of a repartition topic below the app's committed
 * offset are deleted after commits (DeleteRecords), never one that is not processed yet; `repartitionPurgeIntervalMs:
 * 0` keeps them; a missing changelog with `changelog.autoCreate: false` fails the start (W17).
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { string, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '../../src/api/app.js'
import { SourceTopicNotFoundError } from '../../src/changelog/errors.js'
import { flow } from '../../src/app/flow.js'
import { internals } from '../../src/app/internal.js'
import { COUNT_CODEC } from '../../src/dsl/ops/aggregate.js'
import {
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	MultiTopicCollector,
	newClient,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const strings = { key: string(), value: string() }
const WORDS = ['apple', 'banana', 'cherry']

let client: KafkaClient
let producer: Producer

/** `input` grouped by value and counted into `out`, through a repartition topic. */
function counting(applicationId: string, input: string, out: string, config: Partial<FlowConfig>): FlowApp {
	const app = flow({ applicationId, client, consumer: { maxWaitMs: 100 }, ...config })
	app.stream(input, strings)
		.groupBy((_key, value) => value, strings)
		.count()
		.toStream()
		.to(out, { key: string(), value: COUNT_CODEC })
	return app
}

function repartitionTopic(app: FlowApp): string {
	return [...internals(app).build().internalTopics.values()].find(spec => spec.kind === 'repartition')!.name
}

async function logStart(topic: string): Promise<bigint> {
	return (await client.admin().fetchTopicOffsets(topic, [0], 'earliest')).get(0)!
}

/** Polls `cond` every 100 ms until it holds; throws after `timeoutMs`. */
async function until(label: string, cond: () => Promise<boolean>, timeoutMs = 30_000): Promise<void> {
	const deadline = Date.now() + timeoutMs
	while (!(await cond())) {
		if (Date.now() > deadline) throw new Error(`Timed out waiting for: ${label}`)
		await sleep(100)
	}
}

describe('internal topics', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('purge')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	describe.each(['at_least_once', 'exactly_once'] as const)('%s', processingGuarantee => {
		it('purges a repartition topic up to the committed offset; every record is counted once', async () => {
			const input = uniqueTopicName('purge-in')
			const out = uniqueTopicName('purge-out')
			const applicationId = uniqueTopicName('purge')
			await createTopics(client, [{ name: input }, { name: out }])
			const app = counting(applicationId, input, out, {
				processingGuarantee,
				commitIntervalMs: 200,
				repartitionPurgeIntervalMs: 500,
			})
			const repartition = repartitionTopic(app)
			const output = new MultiTopicCollector({
				client,
				groupId: uniqueTopicName('purge-reader'),
				topics: [{ topic: out, keyCodec: string(), valueCodec: COUNT_CODEC }],
			})
			await output.start()
			let sent = 0
			try {
				await app.start()
				// Records keep coming, so commits keep coming, until a purge moved the log start.
				await until('a purge', async () => {
					const word = WORDS[sent++ % WORDS.length]!
					await producer.send(input, { key: Buffer.from(word), value: Buffer.from(word) })
					return (await logStart(repartition)) > 0n
				})
				await output.waitForCount(out, () => true, sent, 15_000)
				expect(await logStart(repartition)).toBeLessThanOrEqual(
					(await fetchCommitted(client, applicationId, repartition)).offset
				)
			} finally {
				await app.close()
				await output.stop()
			}
			// Nothing deleted before it was counted: each word's counts are 1, 2, 3, ... once each.
			for (const word of WORDS) {
				const counts = output.getTopicMessages<string, number>(out).filter(m => m.key === word)
				const expected = Array.from({ length: counts.length }, (_, i) => i + 1)
				expect(counts.map(m => m.value)).toEqual(expected)
			}
			expect(output.getTopicMessages(out)).toHaveLength(sent)
		}, 90_000)
	})

	it('repartitionPurgeIntervalMs 0 keeps the records of repartition topics', async () => {
		const input = uniqueTopicName('purge-off-in')
		const out = uniqueTopicName('purge-off-out')
		const applicationId = uniqueTopicName('purge-off')
		await createTopics(client, [{ name: input }, { name: out }])
		const app = counting(applicationId, input, out, { commitIntervalMs: 200, repartitionPurgeIntervalMs: 0 })
		const repartition = repartitionTopic(app)
		try {
			await app.start()
			for (const word of WORDS) await producer.send(input, { key: Buffer.from(word), value: Buffer.from(word) })
			await until(
				'3 repartition records committed',
				async () => (await fetchCommitted(client, applicationId, repartition)).offset === 3n
			)
			await sleep(1500)
			expect(await logStart(repartition)).toBe(0n)
		} finally {
			await app.close()
		}
	}, 60_000)

	it('a missing changelog with changelog.autoCreate false fails the start with SourceTopicNotFoundError', async () => {
		const input = uniqueTopicName('autocreate-in')
		await createTopics(client, [{ name: input }])
		const app = flow({ applicationId: uniqueTopicName('autocreate'), client, changelog: { autoCreate: false } })
		app.stream(input, strings).groupByKey().count()
		await expect(app.start()).rejects.toThrow(SourceTopicNotFoundError)
		expect(app.state()).toBe('ERROR')
		await app.close()
	}, 30_000)
})
