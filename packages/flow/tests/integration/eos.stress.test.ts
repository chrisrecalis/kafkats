import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient, type Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import { MultiTopicCollector, createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

const PARTITION_COUNT = 4
const KEY_COUNT = 12
const RECORDS_PER_KEY = 50
const RECORD_COUNT = KEY_COUNT * RECORDS_PER_KEY
const FAILURE_COUNTS = new Set([5, 10, 15, 20, 25, 30, 35, 40, 45])
const PROGRESS_TIMEOUT_MS = 90_000

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

async function waitForOutcome(
	app: FlowApp,
	collector: MultiTopicCollector,
	outputTopic: string,
	timeoutMs: number
): Promise<'complete' | 'error'> {
	const deadline = Date.now() + timeoutMs
	while (Date.now() < deadline) {
		if (app.state() === 'ERROR') {
			return 'error'
		}
		if (collector.getTopicMessages<string, number>(outputTopic).length >= RECORD_COUNT) {
			return 'complete'
		}
		await new Promise(resolve => setTimeout(resolve, 20))
	}

	throw new Error(
		`Timed out waiting for EOS flow outcome; state=${app.state()}, ` +
			`committedOutput=${collector.getTopicMessages<string, number>(outputTopic).length}/${RECORD_COUNT}`
	)
}

describe('exactly-once stress', () => {
	let client: KafkaClient
	let producer: Producer

	beforeAll(async () => {
		client = new KafkaClient({
			clientId: `flow-eos-stress-${Date.now()}`,
			brokers: requireKafkaBrokers(),
		})
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
	})

	it(
		'does not lose or duplicate stateful results across repeated aborted transactions and restarts',
		{
			timeout: PROGRESS_TIMEOUT_MS * 2,
			repeats: Number.parseInt(process.env.EOS_STRESS_REPEATS ?? '0', 10),
		},
		async () => {
			const inputTopic = uniqueTopicName('flow-it-eos-stress-input')
			const outputTopic = uniqueTopicName('flow-it-eos-stress-output')
			const applicationId = uniqueTopicName('flow-it-eos-stress-app')
			const failedOnce = new Set<number>()

			await createTopics(client, [
				{ name: inputTopic, partitions: PARTITION_COUNT },
				{ name: outputTopic, partitions: PARTITION_COUNT },
			])

			const collector = new MultiTopicCollector({
				client,
				groupId: uniqueTopicName('flow-it-eos-stress-collector'),
				topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: numberCodec }],
				maxWaitMs: 50,
			})

			const buildApp = (): FlowApp => {
				const app = flow({
					applicationId,
					client,
					numStreamThreads: 1,
					processingGuarantee: 'exactly_once',
					commitIntervalMs: 50,
					consumer: {
						autoOffsetReset: 'earliest',
						maxWaitMs: 50,
					},
					producer: {
						lingerMs: 0,
						retries: 10,
						transactionTimeoutMs: 5_000,
					},
					changelog: {
						restoration: {
							idleTimeoutMs: 200,
							initialIdleTimeoutMs: 5_000,
							checkIntervalMs: 50,
							consumerMaxWaitMs: 50,
						},
					},
				})

				app.stream(inputTopic, { key: stringCodec, value: numberCodec })
					.groupByKey()
					.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
					.toStream()
					.through(outputTopic)
					.peek((key, count) => {
						if (key !== 'key-0' || count === null || !FAILURE_COUNTS.has(count)) {
							return
						}

						if (!failedOnce.has(count)) {
							failedOnce.add(count)
							throw new Error(`injected failure after transactional writes at ${key}:${count}`)
						}
					})

				return app
			}

			await collector.start()
			try {
				const input = Array.from({ length: RECORD_COUNT }, (_, index) => {
					const keyIndex = index % KEY_COUNT
					return {
						key: `key-${keyIndex}`,
						value: numberCodec.encode(index),
						partition: keyIndex % PARTITION_COUNT,
					}
				})
				await producer.send(inputTopic, input)

				for (let restart = 0; restart <= FAILURE_COUNTS.size; restart += 1) {
					const app = buildApp()
					try {
						await app.start()
						const outcome = await waitForOutcome(app, collector, outputTopic, PROGRESS_TIMEOUT_MS)

						if (restart < FAILURE_COUNTS.size) {
							expect(outcome).toBe('error')
						} else {
							expect(outcome).toBe('complete')
						}
					} finally {
						await app.close()
					}
				}

				expect(failedOnce).toEqual(FAILURE_COUNTS)

				const committed = collector.getTopicMessages<string, number>(outputTopic)
				expect(committed).toHaveLength(RECORD_COUNT)

				const barrierKeys = new Set(
					Array.from({ length: PARTITION_COUNT }, (_, partition) => `barrier-${partition}`)
				)
				const verificationApp = buildApp()
				try {
					await verificationApp.start()
					await producer.send(
						inputTopic,
						Array.from({ length: PARTITION_COUNT }, (_, partition) => ({
							key: `barrier-${partition}`,
							value: numberCodec.encode(partition),
							partition,
						}))
					)
					await collector.waitForCount<string, number>(
						outputTopic,
						message => message.key !== null && barrierKeys.has(message.key) && message.value === 1,
						PARTITION_COUNT,
						PROGRESS_TIMEOUT_MS
					)
					expect(verificationApp.state()).not.toBe('ERROR')

					const verified = collector.getTopicMessages<string, number>(outputTopic)
					expect(verified).toHaveLength(RECORD_COUNT + PARTITION_COUNT)
					for (let keyIndex = 0; keyIndex < KEY_COUNT; keyIndex += 1) {
						const counts = verified
							.filter(message => message.key === `key-${keyIndex}`)
							.map(message => message.value)
						expect(counts).toEqual(Array.from({ length: RECORDS_PER_KEY }, (_, index) => index + 1))
					}
					for (const barrierKey of barrierKeys) {
						expect(
							verified.filter(message => message.key === barrierKey).map(message => message.value)
						).toEqual([1])
					}
				} finally {
					await verificationApp.close()
				}
			} finally {
				await collector.stop()
			}
		}
	)
})
