/**
 * Broker restart / unresponsive broker chaos tests.
 *
 * These restart (`docker restart`) and pause (`docker pause`) a dedicated broker, so they only run when
 * pointed at one explicitly - never at the shared broker the rest of the suite uses:
 *
 *   KAFKA_CHAOS_BROKERS=127.0.0.1:29092 KAFKA_CHAOS_CONTAINER=kafka-chaos \
 *     pnpm -C packages/client exec vitest run --config vitest.integration.config.ts tests/integration/chaos
 *
 * Without both env vars the whole file is skipped.
 */
import { execSync } from 'node:child_process'
import { describe, expect, it } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import type { Logger } from '@/logger.js'

import { sleep, uniqueName } from '../helpers/testkit.js'

const CHAOS_BROKERS = process.env.KAFKA_CHAOS_BROKERS
const CHAOS_CONTAINER = process.env.KAFKA_CHAOS_CONTAINER
const enabled = Boolean(CHAOS_BROKERS && CHAOS_CONTAINER)
const brokers = (CHAOS_BROKERS ?? '').split(',').map(b => b.trim())

function docker(cmd: 'restart' | 'pause' | 'unpause'): void {
	execSync(`docker ${cmd} ${CHAOS_CONTAINER}`, { stdio: 'pipe' })
}

/** Logger that counts error-level lines (and discards everything else). */
function countingLogger(counter: { errors: number }): Logger {
	const logger: Logger = {
		error: () => {
			counter.errors++
		},
		warn: () => {},
		info: () => {},
		debug: () => {},
		child: () => logger,
	}
	return logger
}

function makeClient(id: string, counter?: { errors: number }): KafkaClient {
	return new KafkaClient({
		brokers,
		clientId: uniqueName(id),
		...(counter ? { logger: countingLogger(counter) } : { logLevel: 'silent' as const }),
	})
}

/** Resolve once a fresh client can talk to the broker again. */
async function waitForBroker(timeoutMs = 90_000): Promise<void> {
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const probe = makeClient('chaos-probe')
		try {
			await probe.connect()
			await probe.getMetadata()
			await probe.disconnect()
			return
		} catch (error) {
			await probe.disconnect().catch(() => {})
			if (Date.now() > deadline) throw error
			await sleep(250)
		}
	}
}

async function restartBroker(): Promise<void> {
	docker('restart')
	await waitForBroker()
}

async function waitUntil(cond: () => boolean, timeoutMs: number, what: string): Promise<void> {
	const deadline = Date.now() + timeoutMs
	while (!cond()) {
		if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`)
		await sleep(100)
	}
}

async function createTopic(client: KafkaClient, name: string, partitions: number): Promise<void> {
	await client.createTopics([{ name, numPartitions: partitions, replicationFactor: 1 }])
	const deadline = Date.now() + 20_000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata([name])
		const t = meta.topics.get(name)
		if (t && t.partitions.size === partitions && [...t.partitions.values()].every(p => p.leaderId >= 0)) return
		await sleep(100)
	}
	throw new Error(`topic ${name} not ready`)
}

/** Produce `values` with a short-lived, independent client (retrying while the broker is not ready). */
async function produceValues(topicName: string, values: string[]): Promise<void> {
	const client = makeClient('chaos-feeder')
	await client.connect()
	const producer = client.producer({ lingerMs: 0 })
	try {
		for (const value of values) {
			await producer.send(topicName, [{ key: value, value: Buffer.from(value) }])
		}
	} finally {
		await producer.disconnect()
		await client.disconnect()
	}
}

function range(prefix: string, from: number, to: number): string[] {
	return Array.from({ length: to - from }, (_, i) => `${prefix}${from + i}`)
}

// Both an 'error'-event count and a logged-error-line count must stay far below what a hot spin produces
// (tens of thousands) - throttled reporting emits roughly one per few seconds.
const MAX_ERROR_EVENTS = 40
const MAX_ERROR_LINES = 400

describe.skipIf(!enabled)('broker restart / unresponsive broker (chaos)', () => {
	for (const mode of ['group', 'manual'] as const) {
		it(`consumer-only client (${mode} assignment) resumes after the broker restarts`, async () => {
			const setup = makeClient('chaos-setup')
			await setup.connect()
			const topicName = uniqueName('chaos-consumer')
			await createTopic(setup, topicName, 2)

			const counter = { errors: 0 }
			const consumerClient = makeClient('chaos-consumer', counter)
			await consumerClient.connect()
			const consumer = consumerClient.consumer({
				groupId: uniqueName('chaos-group'),
				autoOffsetReset: 'earliest',
				maxWaitMs: 100,
				sessionTimeoutMs: 6000,
				heartbeatIntervalMs: 1000,
			})
			const errorEvents: Error[] = []
			consumer.on('error', e => errorEvents.push(e))

			const seen = new Set<string>()
			let runResult: 'running' | 'resolved' | { rejected: Error } = 'running'
			const run = consumer
				.runEach(
					topicName,
					async message => {
						seen.add(Buffer.from(message.value as Buffer).toString())
					},
					mode === 'manual'
						? {
								assignment: [
									{ topic: topicName, partition: 0 },
									{ topic: topicName, partition: 1 },
								],
							}
						: undefined
				)
				.then(
					() => {
						runResult = 'resolved'
					},
					(error: Error) => {
						runResult = { rejected: error }
					}
				)

			try {
				const before = range('before-', 0, 20)
				await produceValues(topicName, before)
				await waitUntil(() => before.every(v => seen.has(v)), 30_000, 'initial records')

				await restartBroker()
				const errorsAtBrokerBack = errorEvents.length

				const after = range('after-', 0, 40)
				await produceValues(topicName, after)
				await waitUntil(() => after.every(v => seen.has(v)), 30_000, 'records produced after the restart')

				expect(runResult).toBe('running')
				expect(errorEvents.length).toBeLessThan(MAX_ERROR_EVENTS)
				expect(counter.errors).toBeLessThan(MAX_ERROR_LINES)
				// Once recovered the consumer must be quiet again.
				const settled = errorEvents.length
				await sleep(3000)
				expect(errorEvents.length).toBe(settled)
				expect(errorsAtBrokerBack).toBeLessThanOrEqual(settled)
			} finally {
				consumer.stop()
				await run
				await consumerClient.disconnect()
				await setup.disconnect()
			}
		}, 180_000)
	}

	it('transactional consume-transform-produce pipeline stays exactly-once across a broker restart', async () => {
		const setup = makeClient('chaos-eos-setup')
		await setup.connect()
		const input = uniqueName('chaos-eos-in')
		const output = uniqueName('chaos-eos-out')
		await createTopic(setup, input, 2)
		await createTopic(setup, output, 2)

		const groupId = uniqueName('chaos-eos-group')
		const transactionalId = uniqueName('chaos-eos-tx')
		const inputValues = [...range('in-', 0, 30), ...range('in-', 30, 60)]
		const processed = new Set<string>()

		const pipelineClient = makeClient('chaos-eos-consumer')
		await pipelineClient.connect()
		const producerClient = makeClient('chaos-eos-producer')
		await producerClient.connect()

		let stopPipeline = false
		let iterations = 0
		const pipeline = (async () => {
			// A transaction in flight when the broker dies aborts (and may fail the run): the loop restarts the
			// consumer + producer with the same ids, like an operator/supervisor would.
			while (!stopPipeline) {
				iterations++
				const producer = producerClient.producer({
					transactionalId,
					retries: 10,
					retryBackoffMs: 200,
					maxRetryBackoffMs: 2000,
				})
				const consumer = pipelineClient.consumer({
					groupId,
					autoOffsetReset: 'earliest',
					isolationLevel: 'read_committed',
					maxWaitMs: 100,
					sessionTimeoutMs: 6000,
					heartbeatIntervalMs: 1000,
				})
				consumer.on('error', () => {})
				const stopper = setInterval(() => {
					if (stopPipeline) consumer.stop()
				}, 200)
				try {
					await consumer.runEach(
						input,
						async (message, context) => {
							const value = Buffer.from(message.value as Buffer).toString()
							await producer.transaction(async tx => {
								await tx.send(output, [{ key: value, value: Buffer.from(`out-${value}`) }])
								await tx.sendOffsets(context)
							})
							processed.add(value)
						},
						{ autoCommit: false, commitOffsets: false }
					)
				} catch {
					// aborted transaction / lost session: retry from the committed offsets
				} finally {
					clearInterval(stopper)
					consumer.stop()
					await producer.disconnect().catch(() => {})
				}
				if (!stopPipeline) await sleep(200)
			}
		})()

		try {
			await produceValues(input, inputValues.slice(0, 30))
			await waitUntil(() => processed.size >= 10, 60_000, 'initial pipeline progress')

			await restartBroker()
			await produceValues(input, inputValues.slice(30))
			await waitUntil(() => inputValues.every(v => processed.has(v)), 90_000, 'all input processed')
		} finally {
			stopPipeline = true
			await pipeline
			await pipelineClient.disconnect()
			await producerClient.disconnect()
		}
		expect(iterations).toBeGreaterThanOrEqual(1)

		// Read the output with a read_committed consumer: every input exactly once, no duplicates.
		const readerClient = makeClient('chaos-eos-reader')
		await readerClient.connect()
		const reader = readerClient.consumer({
			groupId: uniqueName('chaos-eos-reader'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
			maxWaitMs: 100,
		})
		const outputs: string[] = []
		const readRun = reader.runEach(output, async message => {
			outputs.push(Buffer.from(message.value as Buffer).toString())
		})
		try {
			await waitUntil(() => outputs.length >= inputValues.length, 30_000, 'output records')
			await sleep(1500) // any duplicate would show up by now
		} finally {
			reader.stop()
			await readRun
			await readerClient.disconnect()
			await setup.disconnect()
		}
		expect(outputs.sort()).toEqual(inputValues.map(v => `out-${v}`).sort())
	}, 300_000)

	it('consumer stop(), producer.disconnect() and client.disconnect() are bounded against an unresponsive broker', async () => {
		const setup = makeClient('chaos-pause-setup')
		await setup.connect()
		const topicName = uniqueName('chaos-pause')
		await createTopic(setup, topicName, 1)
		await setup.disconnect()

		const socketsBefore = process.getActiveResourcesInfo().filter(r => r === 'TCPSocketWrap').length

		const client = makeClient('chaos-pause')
		await client.connect()
		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({
			groupId: uniqueName('chaos-pause-group'),
			autoOffsetReset: 'earliest',
			maxWaitMs: 100,
		})
		consumer.on('error', () => {})
		const seen = new Set<string>()
		const run = consumer.runEach(
			topicName,
			async message => {
				seen.add(Buffer.from(message.value as Buffer).toString())
			},
			// Long interval: the consumed offsets are still pending when stop() runs, so stop() has a
			// final commit + LeaveGroup to do.
			{ autoCommitIntervalMs: 600_000 }
		)

		let paused = false
		try {
			const values = range('p-', 0, 10)
			await Promise.all(values.map(v => producer.send(topicName, [{ key: v, value: Buffer.from(v) }])))
			await waitUntil(() => values.every(v => seen.has(v)), 30_000, 'records')

			docker('pause')
			paused = true
			// Queue a send that can never be acknowledged so disconnect() has something to flush.
			const stuck = producer.send(topicName, [{ key: 'stuck', value: Buffer.from('stuck') }]).catch(() => {})

			const started = Date.now()
			consumer.stop()
			await run
			const stopMs = Date.now() - started
			await producer.disconnect()
			await stuck
			await client.disconnect()
			const totalMs = Date.now() - started

			expect(stopMs).toBeLessThan(10_000)
			expect(totalMs).toBeLessThan(15_000)
		} finally {
			if (paused) docker('unpause')
			await client.disconnect().catch(() => {})
		}

		await sleep(500)
		expect(process.getActiveResourcesInfo().filter(r => r === 'TCPSocketWrap').length).toBeLessThanOrEqual(
			socketsBefore
		)
		await waitForBroker()
	}, 120_000)
})
