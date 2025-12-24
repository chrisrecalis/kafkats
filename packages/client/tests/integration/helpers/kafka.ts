import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka'
import { Wait } from 'testcontainers'
import { afterAll } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import type { LogLevel } from '@/logger.js'

export type KafkaTestContext = {
	kafka: StartedKafkaContainer
	brokerAddress: string
	logLevel: LogLevel
	createClient: (clientId: string) => KafkaClient
}

let sharedPromise: Promise<KafkaTestContext> | null = null
let sharedClients: KafkaClient[] = []
let cleanupRegistered = false

const useProcessCleanup = process.env.VITEST_INTEGRATION === '1'

async function startKafka(): Promise<KafkaTestContext> {
	const logLevel = (process.env.KAFKA_TS_LOG_LEVEL as LogLevel) ?? 'silent'

	const kafka = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
		.withKraft()
		// Single-broker defaults in many images assume RF=3 for internal topics.
		// Transactions require __transaction_state to be creatable on a 1-node cluster.
		.withEnvironment({
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
		})
		.withWaitStrategy(Wait.forAll([Wait.forListeningPorts(), Wait.forLogMessage(/Kafka Server started/)]))
		.start()

	const brokerAddress = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`

	const createClient = (clientId: string) => {
		const client = new KafkaClient({
			brokers: [brokerAddress],
			clientId,
			logLevel,
		})
		sharedClients.push(client)
		return client
	}

	return { kafka, brokerAddress, logLevel, createClient }
}

async function cleanupShared(): Promise<void> {
	const ctx = await sharedPromise
	if (!ctx) {
		return
	}
	await Promise.allSettled(sharedClients.map(client => client.disconnect()))
	sharedClients = []
	await ctx.kafka.stop()
	sharedPromise = null
}

export async function withKafka<T>(fn: (ctx: KafkaTestContext) => Promise<T>): Promise<T> {
	if (!sharedPromise) {
		sharedPromise = startKafka()
	}
	if (!cleanupRegistered) {
		cleanupRegistered = true
		if (useProcessCleanup) {
			process.once('beforeExit', () => {
				void cleanupShared()
			})
		} else {
			afterAll(async () => {
				await cleanupShared()
			})
		}
	}

	const ctx = await sharedPromise
	return await fn(ctx)
}
