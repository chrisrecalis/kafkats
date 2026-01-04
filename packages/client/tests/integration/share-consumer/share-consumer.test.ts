import { describe, expect, it, beforeAll, afterAll, vi } from 'vitest'
import { once } from 'node:events'
import { randomUUID } from 'node:crypto'

import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers'

import { KafkaClient } from '@/client/index.js'
import { topic } from '@/topic.js'
import { string } from '@/codec.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const APACHE_KAFKA_IMAGE = 'apache/kafka:4.1.1'

type KafkaRuntime = {
	container: StartedTestContainer
	brokers: string
}

async function startApacheKafka41WithShareGroups(): Promise<KafkaRuntime> {
	const advertisedHost = process.env.TESTCONTAINERS_HOST_OVERRIDE ?? 'localhost'
	const hostPort = await getFreePort()

	const container = await new GenericContainer(APACHE_KAFKA_IMAGE)
		.withHostname('broker')
		.withExposedPorts({ container: hostPort, host: hostPort })
		.withEnvironment({
			KAFKA_NODE_ID: '1',
			KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
			KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT_HOST://${advertisedHost}:${hostPort},PLAINTEXT://broker:19092`,
			KAFKA_PROCESS_ROLES: 'broker,controller',
			KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093',
			KAFKA_LISTENERS: `CONTROLLER://:29093,PLAINTEXT_HOST://:${hostPort},PLAINTEXT://:19092`,
			KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
			KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
			CLUSTER_ID: '4L6g3nShT-eMCtK--X86sw',
			KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
			KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: '1',
			KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: '1',
			KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs',
		})
		.withWaitStrategy(Wait.forAll([Wait.forListeningPorts(), Wait.forLogMessage(/Kafka Server started/)]))
		.withStartupTimeout(120_000)
		.start()

	await enableKafkaFeature(container, 'share.version=1', 'localhost:19092')

	return { container, brokers: `${advertisedHost}:${hostPort}` }
}

async function enableKafkaFeature(
	container: StartedTestContainer,
	feature: string,
	bootstrapServer: string
): Promise<void> {
	const command = [
		'bash',
		'-lc',
		[
			// Prefer PATH, fall back to the standard install location.
			'FEATURES_BIN="$(command -v kafka-features.sh || true)"',
			'if [ -z "$FEATURES_BIN" ] && [ -x /opt/kafka/bin/kafka-features.sh ]; then FEATURES_BIN="/opt/kafka/bin/kafka-features.sh"; fi',
			'if [ -z "$FEATURES_BIN" ]; then echo "kafka-features.sh not found" >&2; exit 1; fi',
			`"$FEATURES_BIN" --bootstrap-server "${bootstrapServer}" upgrade --feature "${feature}"`,
		].join('\n'),
	]

	const result = await container.exec(command)
	if (result.exitCode !== 0) {
		throw new Error(`Failed to enable feature ${feature}: ${result.output}`)
	}
}

async function getFreePort(): Promise<number> {
	const { createServer } = await import('node:net')
	return await new Promise((resolve, reject) => {
		const server = createServer()
		server.unref()
		server.on('error', reject)
		server.listen(0, () => {
			const address = server.address()
			if (!address || typeof address === 'string') {
				server.close(() => reject(new Error('Failed to allocate a free port')))
				return
			}
			const { port } = address
			server.close(err => (err ? reject(err) : resolve(port)))
		})
	})
}

async function createTopicWithAdmin(client: KafkaClient, name: string): Promise<void> {
	const admin = client.admin()
	const results = await admin.createTopics([{ name, numPartitions: 1, replicationFactor: 1 }])
	const result = results[0]
	if (!result) throw new Error('Expected createTopics to return a result')
	if (result.errorCode !== ErrorCode.None) {
		throw new Error(`Failed to create topic ${name}: ${result.errorMessage ?? `errorCode=${result.errorCode}`}`)
	}
}

describe('ShareConsumer (integration) - Apache Kafka 4.1 share groups', () => {
	let runtime: KafkaRuntime

	beforeAll(async () => {
		runtime = await startApacheKafka41WithShareGroups()
	}, 180_000)

	afterAll(async () => {
		await runtime.container.stop()
	}, 60_000)

	it('consumes records produced after joining (latest offset)', async () => {
		const client = new KafkaClient({
			clientId: `it-share-${randomUUID()}`,
			brokers: [runtime.brokers],
			logLevel: 'error',
		})
		await client.connect()

		const topicName = `it-share-topic-${randomUUID()}`
		await createTopicWithAdmin(client, topicName)

		const testTopic = topic<string, string>(topicName, { key: string(), value: string() })

		const share = client.shareConsumer({ groupId: `it-share-group-${randomUUID()}` })
		const assigned = once(share, 'partitionsAssigned')

		let received: string | null = null
		const run = share.runEach(
			testTopic,
			async message => {
				received = message.value
				await message.ack()
				share.stop()
			},
			{ concurrency: 1 }
		)

		await assigned

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, { key: 'k', value: 'v' })
		await producer.flush()
		await producer.disconnect()

		await run
		expect(received).toBe('v')

		await client.disconnect()
	})

	it('delivers each record to only one member in the share group', async () => {
		const client = new KafkaClient({
			clientId: `it-share-${randomUUID()}`,
			brokers: [runtime.brokers],
			logLevel: 'error',
		})
		await client.connect()

		const topicName = `it-share-topic-${randomUUID()}`
		await createTopicWithAdmin(client, topicName)

		const testTopic = topic<string, string>(topicName, { value: string() })
		const groupId = `it-share-group-${randomUUID()}`

		const c1 = client.shareConsumer({ groupId })
		const c2 = client.shareConsumer({ groupId })

		const running = Promise.all([once(c1, 'running'), once(c2, 'running')])
		const assigned = Promise.race([once(c1, 'partitionsAssigned'), once(c2, 'partitionsAssigned')])

		const consumed1: string[] = []
		const consumed2: string[] = []

		const stopBothIfDone = () => {
			if (consumed1.length + consumed2.length >= 10) {
				c1.stop()
				c2.stop()
			}
		}

		const run1 = c1.runEach(
			testTopic,
			async message => {
				consumed1.push(message.value)
				await message.ack()
				stopBothIfDone()
			},
			{ concurrency: 1 }
		)

		const run2 = c2.runEach(
			testTopic,
			async message => {
				consumed2.push(message.value)
				await message.ack()
				stopBothIfDone()
			},
			{ concurrency: 1 }
		)

		await running
		await assigned

		const producer = client.producer({ lingerMs: 0 })
		for (let i = 0; i < 10; i++) {
			await producer.send(testTopic, { value: `msg-${i}` })
		}
		await producer.flush()
		await producer.disconnect()

		await vi.waitFor(
			() => {
				expect(consumed1.length + consumed2.length).toBeGreaterThanOrEqual(10)
			},
			{ timeout: 15_000, interval: 25 }
		)
		c1.stop()
		c2.stop()

		await Promise.all([run1, run2])

		const all = [...consumed1, ...consumed2]
		expect(all).toHaveLength(10)
		expect(new Set(all).size).toBe(10)

		await client.disconnect()
	})

	it('flushes pending acknowledgements when stopped', async () => {
		const client = new KafkaClient({
			clientId: `it-share-${randomUUID()}`,
			brokers: [runtime.brokers],
			logLevel: 'error',
		})
		await client.connect()

		const topicName = `it-share-topic-${randomUUID()}`
		await createTopicWithAdmin(client, topicName)

		const testTopic = topic<string, string>(topicName, { value: string() })

		const share = client.shareConsumer({ groupId: `it-share-group-${randomUUID()}` })
		const assigned = once(share, 'partitionsAssigned')

		const messageCount = 5
		let processed = 0
		let ackSettled = 0
		const ackErrors: unknown[] = []

		const run = share.runEach(
			testTopic,
			async message => {
				processed++

				const ack = message.ack()
				void ack
					.catch(err => {
						ackErrors.push(err)
					})
					.finally(() => {
						ackSettled++
					})

				if (processed === messageCount) {
					expect(ackSettled).toBeLessThan(processed)
					share.stop()
				}
			},
			{ concurrency: 1 }
		)

		await assigned

		const producer = client.producer({ lingerMs: 0 })
		for (let i = 0; i < messageCount; i++) {
			await producer.send(testTopic, { value: `msg-${i}` })
		}
		await producer.flush()
		await producer.disconnect()

		await run

		await vi.waitFor(
			() => {
				expect(ackSettled).toBe(messageCount)
				expect(ackErrors).toHaveLength(0)
			},
			{ timeout: 15_000, interval: 25 }
		)

		await client.disconnect()
	})
})
