import { Kafka as KafkaJs } from 'kafkajs'

export interface KafkaClusterConfig {
	brokerCount: number
}

export interface StartedKafkaCluster {
	brokers: string[]
	brokerIds: number[]
	stop: () => Promise<void>
}

function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

async function waitForClusterReady(brokers: string[], brokerCount: number): Promise<void> {
	const kafka = new KafkaJs({
		clientId: 'benchmark-cluster-check',
		brokers,
		logLevel: 0,
	})
	const admin = kafka.admin()
	const maxAttempts = 30
	const delayMs = 1000

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			await admin.connect()
			const description = await admin.describeCluster()
			if (description.brokers.length >= brokerCount) {
				await admin.disconnect()
				return
			}
			await admin.disconnect()
		} catch (error) {
			await admin.disconnect().catch(() => {})
			if (attempt === maxAttempts) {
				throw error
			}
		}
		await new Promise(resolve => setTimeout(resolve, delayMs))
	}

	throw new Error(`Kafka cluster did not reach ${brokerCount} brokers in time`)
}

async function waitForTopicLeaders(
	admin: ReturnType<KafkaJs['admin']>,
	topicName: string,
	partitionCount: number
): Promise<number[]> {
	const maxAttempts = 30
	const delayMs = 500
	let lastError: unknown

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			const metadata = await admin.fetchTopicMetadata({ topics: [topicName] })
			const topic = metadata.topics.find(t => t.name === topicName)
			const leaders = topic?.partitions.map(partition => partition.leader ?? -1) ?? []
			if (leaders.length === partitionCount && leaders.every(leader => leader >= 0)) {
				return leaders
			}
		} catch (error) {
			lastError = error
		}
		await new Promise(resolve => setTimeout(resolve, delayMs))
	}

	throw new Error(
		`Timed out waiting for leaders for topic ${topicName}${lastError instanceof Error ? `: ${lastError.message}` : ''}`
	)
}

export async function createTopicWithAssignments(
	cluster: StartedKafkaCluster,
	topicName: string,
	partitionCount: number
): Promise<void> {
	if (cluster.brokerIds.length < partitionCount) {
		throw new Error(`Cannot assign ${partitionCount} partitions across ${cluster.brokerIds.length} brokers`)
	}

	const kafka = new KafkaJs({
		clientId: 'benchmark-admin',
		brokers: cluster.brokers,
		logLevel: 0,
	})
	const admin = kafka.admin()
	await admin.connect()

	const replicaAssignment = Array.from({ length: partitionCount }, (_, index) => ({
		partition: index,
		replicas: [cluster.brokerIds[index % cluster.brokerIds.length]],
	}))

	const maxAttempts = 5
	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			await admin.createTopics({
				topics: [
					{
						topic: topicName,
						replicaAssignment,
					},
				],
				waitForLeaders: false,
			})
			break
		} catch (error) {
			const message = String(error)
			const retriable =
				message.includes('This server does not host this topic-partition') ||
				message.includes('UNKNOWN_TOPIC_OR_PARTITION') ||
				(message as { code?: number }).code === 3

			if (!retriable || attempt === maxAttempts) {
				await admin.disconnect()
				throw error
			}
			await sleep(500)
		}
	}

	const leaders = await waitForTopicLeaders(admin, topicName, partitionCount)
	const uniqueLeaders = new Set(leaders)

	if (uniqueLeaders.size < Math.min(partitionCount, cluster.brokerIds.length)) {
		await admin.disconnect()
		throw new Error(`Topic ${topicName} partitions are not spread across brokers: ${leaders.join(', ')}`)
	}

	await admin.disconnect()
}

async function getExistingClusterFromEnv(): Promise<StartedKafkaCluster | null> {
	const brokersEnv = process.env.KAFKA_BROKERS
	if (!brokersEnv) {
		return null
	}

	const expectedCount = Number(process.env.KAFKA_BROKER_COUNT ?? '3')
	const brokers = brokersEnv
		.split(',')
		.map(b => b.trim())
		.filter(Boolean)

	if (brokers.length === 0) {
		return null
	}

	const kafka = new KafkaJs({
		clientId: 'benchmark-admin',
		brokers,
		logLevel: 0,
	})
	const admin = kafka.admin()
	const maxAttempts = 30
	const delayMs = 1000
	let description: Awaited<ReturnType<typeof admin.describeCluster>> | null = null

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			await admin.connect()
			description = await admin.describeCluster()
			await admin.disconnect()
			if (description.brokers.length >= expectedCount) {
				break
			}
		} catch (error) {
			await admin.disconnect().catch(() => {})
			if (attempt === maxAttempts) {
				throw error
			}
		}
		await new Promise(resolve => setTimeout(resolve, delayMs))
	}

	if (!description) {
		throw new Error('Failed to describe existing Kafka cluster')
	}

	return {
		brokers,
		brokerIds: description.brokers.map(broker => broker.nodeId),
		stop: async () => {},
	}
}

/**
 * Uses an existing Kafka cluster defined by KAFKA_BROKERS.
 * Benchmarks assume a 3-broker cluster provided externally (e.g. docker compose).
 */
export async function startKafkaCluster(config: KafkaClusterConfig): Promise<StartedKafkaCluster> {
	const cluster = await getExistingClusterFromEnv()
	if (!cluster) {
		throw new Error(
			'KAFKA_BROKERS must be set when running benchmarks (use docker compose or another Kafka cluster)'
		)
	}

	await waitForClusterReady(cluster.brokers, config.brokerCount)
	console.log(`Using Kafka cluster: ${cluster.brokers.join(', ')}`)
	return cluster
}

export async function withKafkaCluster<T>(
	config: KafkaClusterConfig,
	fn: (cluster: StartedKafkaCluster) => Promise<T>
): Promise<T> {
	const cluster = await startKafkaCluster(config)
	try {
		return await fn(cluster)
	} finally {
		await cluster.stop()
	}
}
