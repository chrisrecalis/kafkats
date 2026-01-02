import { afterAll, beforeAll, describe, expect, expectTypeOf, it, vi } from 'vitest'

import { existsSync } from 'node:fs'
import { execSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers'

import { KafkaClient } from '@/client/index.js'
import { CoordinatorNotAvailableError, NotCoordinatorError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

import { sleep, uniqueName } from '../helpers/testkit.js'

const STARTER_SCRIPT = '/testcontainers_start.sh'
const WAIT_FOR_SCRIPT_MESSAGE = 'Waiting for script...'

function resolveDockerHostGateway(): string | null {
	try {
		const output = execSync('ip route', { encoding: 'utf-8' })
		const line = output
			.split('\n')
			.map(l => l.trim())
			.find(l => l.startsWith('default '))
		if (!line) return null

		const match = line.match(/\bvia\s+(\d+\.\d+\.\d+\.\d+)\b/)
		return match?.[1] ?? null
	} catch {
		return null
	}
}

async function waitForKafkaReady(broker: string): Promise<void> {
	const startedAt = Date.now()
	let lastError: unknown

	while (Date.now() - startedAt < 120_000) {
		const client = new KafkaClient({
			brokers: [broker],
			clientId: uniqueName('it-share-ready'),
			logLevel: 'silent',
		})

		try {
			await client.connect()
			await client.getMetadata()
			await client.disconnect()
			return
		} catch (error) {
			lastError = error
			await client.disconnect().catch(() => {})
			await sleep(500)
		}
	}

	throw lastError instanceof Error ? lastError : new Error(`Kafka did not become ready at ${broker}`)
}

async function warmUpShareGroups(broker: string): Promise<void> {
	const client = new KafkaClient({
		brokers: [broker],
		clientId: uniqueName('it-share-warmup'),
		logLevel: 'silent',
	})

	try {
		await client.connect()

		const topicName = uniqueName('it-share-warmup-topic')
		const groupId = uniqueName('it-share-warmup-group')

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		let coordinator: Awaited<ReturnType<typeof client.cluster.getCoordinator>> | null = null
		const coordinatorStartedAt = Date.now()

		while (Date.now() - coordinatorStartedAt < 60_000) {
			try {
				coordinator = await client.cluster.getCoordinator('GROUP', groupId)
				break
			} catch (error) {
				if (error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError) {
					await sleep(200)
					continue
				}
				throw error
			}
		}

		if (!coordinator) {
			throw new Error(`Timed out waiting for GROUP coordinator for ${groupId}`)
		}
		let memberId = randomBytes(16).toString('base64url')

		let memberEpoch = 0
		let assignment: Array<{ topicId: string; partitions: number[] }> = []
		let heartbeatCount = 0
		let lastResponse: {
			errorCode: ErrorCode
			errorMessage: string | null
			memberEpoch: number
			heartbeatIntervalMs: number
			assignment: Array<{ topicId: string; partitions: number[] }> | null
		} | null = null

		const startedAt = Date.now()
		while (Date.now() - startedAt < 60_000) {
			if (!coordinator) {
				try {
					coordinator = await client.cluster.getCoordinator('GROUP', groupId)
				} catch (error) {
					if (error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError) {
						await sleep(200)
						continue
					}
					throw error
				}
			}

			const resp = await coordinator.shareGroupHeartbeat({
				groupId,
				memberId,
				memberEpoch,
				rackId: null,
				subscribedTopicNames: [topicName],
			})
			heartbeatCount++
			lastResponse = {
				errorCode: resp.errorCode,
				errorMessage: resp.errorMessage,
				memberEpoch: resp.memberEpoch,
				heartbeatIntervalMs: resp.heartbeatIntervalMs,
				assignment: resp.assignment,
			}

			if (resp.errorCode !== ErrorCode.None) {
				if (resp.errorCode === ErrorCode.CoordinatorLoadInProgress) {
					await sleep(200)
					continue
				}
				if (
					resp.errorCode === ErrorCode.NotCoordinator ||
					resp.errorCode === ErrorCode.CoordinatorNotAvailable
				) {
					client.cluster.invalidateCoordinator('GROUP', groupId)
					coordinator = null
					continue
				}
				throw new Error(resp.errorMessage ?? `ShareGroupHeartbeat failed with ${resp.errorCode}`)
			}

			if (resp.memberId && resp.memberId !== memberId) {
				memberId = resp.memberId
			}
			memberEpoch = resp.memberEpoch
			assignment = resp.assignment ?? []

			if (assignment.length > 0) {
				break
			}

			await sleep(200)
		}

		if (assignment.length === 0) {
			throw new Error(
				`Timed out waiting for share assignment during warmup for ${groupId} (heartbeats=${heartbeatCount}, last=${JSON.stringify(
					lastResponse
				)})`
			)
		}

		// Trigger a non-blocking ShareFetch once to ensure the share path is initialized.
		const leader = await client.cluster.getLeaderForPartition(topicName, 0)
		await leader.shareFetch({
			groupId,
			memberId,
			shareSessionEpoch: 0,
			maxWaitMs: 0,
			minBytes: 0,
			maxBytes: 1024,
			maxRecords: 1,
			batchSize: 1,
			topics: [
				{
					topicId: assignment[0]!.topicId,
					partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }],
				},
			],
			forgottenTopicsData: [],
		})
	} finally {
		await client.disconnect().catch(() => {})
	}
}

describe('ShareConsumer (integration)', () => {
	const image = process.env.KAFKA_SHARE_TEST_IMAGE ?? 'apache/kafka:4.1.1'
	let container: StartedTestContainer | null = null
	let broker: string

	beforeAll(async () => {
		// Fix host detection when running inside Docker (e.g., devcontainers)
		if (!process.env.TESTCONTAINERS_HOST_OVERRIDE && existsSync('/.dockerenv')) {
			const gateway = resolveDockerHostGateway()
			if (gateway) {
				process.env.TESTCONTAINERS_HOST_OVERRIDE = gateway
			}
		}

		container = await new GenericContainer(image)
			.withExposedPorts(9092)
			.withEnvironment({
				KAFKA_NODE_ID: '1',
				KAFKA_PROCESS_ROLES: 'broker,controller',
				KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
					'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
				KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
				KAFKA_LISTENERS: 'PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092',
				KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
				KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
				CLUSTER_ID: 'q1Sh-9_ISia_zwGINzRvyQ',
				KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
				KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
				KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
				KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
				// Share Groups are feature-flagged.
				KAFKA_SHARE_VERSION: '1',
				KAFKA_CFG_SHARE_VERSION: '1',
				KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: '1',
				KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: '1',
				// Some images only pick up configs via KAFKA_CFG_* env vars.
				KAFKA_CFG_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: '1',
				KAFKA_CFG_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: '1',
				KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs',
				KAFKA_HEAP_OPTS: '-Xms256m -Xmx256m',
			})
			.withWaitStrategy(Wait.forLogMessage(WAIT_FOR_SCRIPT_MESSAGE))
			.withStartupTimeout(180_000)
			.withEntrypoint(['bash'])
			.withCommand([
				'-lc',
				`echo '${WAIT_FOR_SCRIPT_MESSAGE}'; while [ ! -f ${STARTER_SCRIPT} ]; do sleep 0.1; done; ${STARTER_SCRIPT}`,
			])
			.start()

		const host = container.getHost()
		const hostPort = container.getMappedPort(9092)

		const startScript = `#!/usr/bin/env bash
set -euo pipefail

export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:19092,PLAINTEXT_HOST://${host}:${hostPort}"

exec /etc/kafka/docker/run
`

		await container.copyContentToContainer([{ content: startScript, target: STARTER_SCRIPT, mode: 0o755 }])

		broker = `${host}:${hostPort}`
		await waitForKafkaReady(broker)

		// Share groups are feature-flagged; configuration alone isn't enough.
		await container.exec([
			'/opt/kafka/bin/kafka-features.sh',
			'--bootstrap-server',
			'localhost:19092',
			'upgrade',
			'--feature',
			'share.version=1',
		])

		// Feature upgrade can be asynchronous; warm up ShareGroupHeartbeat + ShareFetch once before tests.
		await warmUpShareGroups(broker)
	}, 180_000)

	afterAll(async () => {
		await container?.stop()
	})

	it('balances load between two consumers', async () => {
		const client = new KafkaClient({
			brokers: [broker],
			clientId: uniqueName('it-share-consumer'),
			logLevel: 'error',
		})

		let producer: ReturnType<KafkaClient['producer']> | null = null

		try {
			await client.connect()

			const topicName = uniqueName('it-share-topic')
			const groupId = uniqueName('it-share-group')
			const partitionCount = 4
			const messageCount = 40

			await client.createTopics([{ name: topicName, numPartitions: partitionCount, replicationFactor: 1 }])

			const shareConsumerA = client.shareConsumer({ groupId })
			const shareConsumerB = client.shareConsumer({ groupId })

			const assignmentA = new Set<number>()
			const assignmentB = new Set<number>()

			shareConsumerA.on('partitionsAssigned', partitions => {
				for (const p of partitions) {
					assignmentA.add(p.partition)
				}
			})
			shareConsumerB.on('partitionsAssigned', partitions => {
				for (const p of partitions) {
					assignmentB.add(p.partition)
				}
			})

			const received = new Set<string>()
			let aCount = 0
			let bCount = 0
			let runError: unknown | null = null

			const runA = shareConsumerA
				.runEach(topicName, async message => {
					expectTypeOf(message.value).toEqualTypeOf<Buffer>()
					expect(Buffer.isBuffer(message.value)).toBe(true)

					const value = message.value.toString('utf-8')

					if (received.has(value)) {
						throw new Error(`duplicate message: ${value}`)
					}

					received.add(value)
					aCount++
				})
				.catch(err => {
					runError ??= err
					shareConsumerA.stop()
					shareConsumerB.stop()
				})

			const runB = shareConsumerB
				.runEach(topicName, async message => {
					expectTypeOf(message.value).toEqualTypeOf<Buffer>()
					expect(Buffer.isBuffer(message.value)).toBe(true)

					const value = message.value.toString('utf-8')

					if (received.has(value)) {
						throw new Error(`duplicate message: ${value}`)
					}

					received.add(value)
					bCount++
				})
				.catch(err => {
					runError ??= err
					shareConsumerA.stop()
					shareConsumerB.stop()
				})

			try {
				await vi.waitFor(
					() => {
						if (runError) {
							throw runError
						}
						expect(assignmentA.size).toBeGreaterThan(0)
						expect(assignmentB.size).toBeGreaterThan(0)
						expect(new Set([...assignmentA, ...assignmentB]).size).toBe(partitionCount)
					},
					{ timeout: 60_000 }
				)

				producer = client.producer({ lingerMs: 0 })

				await producer.send(
					topicName,
					Array.from({ length: messageCount }, (_, i) => ({
						partition: i % partitionCount,
						value: Buffer.from(`v-${i}`, 'utf-8'),
					}))
				)
				await producer.flush()

				await vi.waitFor(
					() => {
						if (runError) {
							throw runError
						}
						expect(received.size).toBe(messageCount)
					},
					{ timeout: 60_000 }
				)
			} finally {
				shareConsumerA.stop()
				shareConsumerB.stop()
				await Promise.all([runA, runB])
			}

			if (runError) {
				throw runError
			}

			expect(received.size).toBe(messageCount)
			expect(aCount).toBeGreaterThan(0)
			expect(bCount).toBeGreaterThan(0)
			expect(aCount + bCount).toBe(messageCount)
		} finally {
			await producer?.disconnect().catch(() => {})
			await client.disconnect().catch(() => {})
		}
	}, 90_000)

	it('uses concurrency to process records concurrently', async () => {
		const client = new KafkaClient({
			brokers: [broker],
			clientId: uniqueName('it-share-partition-concurrency'),
			logLevel: 'error',
		})

		let producer: ReturnType<KafkaClient['producer']> | null = null

		try {
			await client.connect()

			const topicName = uniqueName('it-share-partition-concurrency')
			const groupId = uniqueName('it-share-group')

			await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

			const shareConsumer = client.shareConsumer({ groupId })

			const startedAt = new Map<number, number>()
			const finished = new Set<number>()
			const handlerSleepMs = 1200
			const assignment = new Set<number>()
			let runError: unknown | null = null

			shareConsumer.on('partitionsAssigned', partitions => {
				for (const p of partitions) {
					assignment.add(p.partition)
				}
			})

			const run = shareConsumer
				.runEach(
					topicName,
					async message => {
						expectTypeOf(message.value).toEqualTypeOf<Buffer>()
						expect(Buffer.isBuffer(message.value)).toBe(true)

						if (!startedAt.has(message.partition)) {
							startedAt.set(message.partition, Date.now())
						}

						await sleep(handlerSleepMs)
						await message.ack()
						finished.add(message.partition)
					},
					{ concurrency: 2 }
				)
				.catch(err => {
					runError ??= err
					shareConsumer.stop()
				})

			try {
				await vi.waitFor(
					() => {
						if (runError) {
							throw runError
						}
						expect(assignment.size).toBe(2)
					},
					{ timeout: 60_000 }
				)

				producer = client.producer({ lingerMs: 0 })
				await producer.send(topicName, [
					{ value: Buffer.from('p0', 'utf-8'), partition: 0 },
					{ value: Buffer.from('p1', 'utf-8'), partition: 1 },
				])
				await producer.flush()

				await vi.waitFor(
					() => {
						if (runError) {
							throw runError
						}
						expect(finished.size).toBe(2)
					},
					{ timeout: 60_000 }
				)
			} finally {
				shareConsumer.stop()
				await run
			}

			if (runError) {
				throw runError
			}

			expect(startedAt.has(0)).toBe(true)
			expect(startedAt.has(1)).toBe(true)

			const deltaMs = Math.abs(startedAt.get(0)! - startedAt.get(1)!)
			expect(deltaMs).toBeLessThan(1000)
		} finally {
			await producer?.disconnect().catch(() => {})
			await client.disconnect().catch(() => {})
		}
	}, 90_000)
})
