import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka'
import { Wait } from 'testcontainers'
import { afterAll } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import type { LogLevel } from '@/logger.js'
import type { SaslConfig } from '@/network/types.js'

export type SaslMechanism = 'PLAIN' | 'SCRAM-SHA-256' | 'SCRAM-SHA-512'

export type KafkaSaslTestContext = {
	kafka: StartedKafkaContainer
	brokerAddress: string
	logLevel: LogLevel
	saslConfig: SaslConfig
	createClient: (clientId: string) => KafkaClient
}

export interface WithKafkaSaslOptions {
	mechanism: SaslMechanism
	username?: string
	password?: string
}

type SharedSaslContext = {
	ctx: KafkaSaslTestContext
	clients: KafkaClient[]
}

const sharedSaslContexts = new Map<string, Promise<SharedSaslContext>>()
let saslCleanupRegistered = false
const useProcessCleanup = process.env.VITEST_INTEGRATION === '1'

/**
 * Test helper that creates a Kafka container with SASL authentication enabled
 */
export async function withKafkaSasl<T>(
	options: WithKafkaSaslOptions,
	fn: (ctx: KafkaSaslTestContext) => Promise<T>
): Promise<T> {
	const username = options.username ?? 'testuser'
	const password = options.password ?? 'testpass'
	const key = `${options.mechanism}:${username}:${password}`

	if (!sharedSaslContexts.has(key)) {
		sharedSaslContexts.set(key, startKafkaSasl(options, username, password))
	}

	if (!saslCleanupRegistered) {
		saslCleanupRegistered = true
		const cleanup = async () => {
			const entries = Array.from(sharedSaslContexts.values())
			await Promise.all(
				entries.map(async entryPromise => {
					const entry = await entryPromise
					await Promise.allSettled(entry.clients.map(client => client.disconnect()))
					await entry.ctx.kafka.stop()
				})
			)
			sharedSaslContexts.clear()
		}

		if (useProcessCleanup) {
			process.once('beforeExit', () => {
				void cleanup()
			})
		} else {
			afterAll(async () => {
				await cleanup()
			})
		}
	}

	const entry = await sharedSaslContexts.get(key)!
	return await fn(entry.ctx)
}

async function startKafkaSasl(
	options: WithKafkaSaslOptions,
	username: string,
	password: string
): Promise<SharedSaslContext> {
	const logLevel = (process.env.KAFKA_TS_LOG_LEVEL as LogLevel) ?? 'silent'
	const useKraft = options.mechanism === 'PLAIN'

	// Build environment configuration for the selected mechanism
	const envConfig = buildEnvConfig(options.mechanism, username, password)

	const kafkaBuilder = new KafkaContainer('confluentinc/cp-kafka:7.5.0')
	if (useKraft) {
		kafkaBuilder.withKraft()
	}
	if (options.mechanism === 'SCRAM-SHA-256' || options.mechanism === 'SCRAM-SHA-512') {
		const jaasConfig = 'KafkaServer { org.apache.kafka.common.security.scram.ScramLoginModule required; };'
		kafkaBuilder
			.withCopyContentToContainer([{ content: jaasConfig, target: '/etc/kafka/jaas.conf', mode: 0o644 }])
			.withEnvironment({
				KAFKA_OPTS: '-Djava.security.auth.login.config=/etc/kafka/jaas.conf',
			})
	}

	const waitStrategy =
		options.mechanism === 'PLAIN'
			? Wait.forAll([Wait.forListeningPorts(), Wait.forLogMessage(/Kafka Server started/)])
			: Wait.forListeningPorts()

	const startContainer = async (): Promise<StartedKafkaContainer> => {
		try {
			return await kafkaBuilder
				.withEnvironment({
					// Transaction state log configuration for single-broker setup
					KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
					KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
					// Inter-broker uses PLAINTEXT for simplicity
					KAFKA_INTER_BROKER_LISTENER_NAME: 'BROKER',
					...envConfig,
				})
				.withWaitStrategy(waitStrategy)
				.start()
		} catch (error) {
			const message = error instanceof Error ? error.message : ''
			if (message.includes('Failed to connect to Reaper')) {
				process.env.TESTCONTAINERS_RYUK_DISABLED = 'true'
				return await kafkaBuilder
					.withEnvironment({
						// Transaction state log configuration for single-broker setup
						KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
						KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
						// Inter-broker uses PLAINTEXT for simplicity
						KAFKA_INTER_BROKER_LISTENER_NAME: 'BROKER',
						...envConfig,
					})
					.withWaitStrategy(waitStrategy)
					.start()
			}
			throw error
		}
	}

	const kafka = await startContainer()

	if (options.mechanism === 'SCRAM-SHA-256' || options.mechanism === 'SCRAM-SHA-512') {
		await configureScramUser(kafka, options.mechanism, username, password, useKraft)
	}

	const brokerAddress = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`

	const saslConfig: SaslConfig = {
		mechanism: options.mechanism,
		username,
		password,
	}

	const clients: KafkaClient[] = []
	const createClient = (clientId: string) => {
		const client = new KafkaClient({
			brokers: [brokerAddress],
			clientId,
			logLevel,
			sasl: saslConfig,
		})
		clients.push(client)
		return client
	}

	const ctx: KafkaSaslTestContext = { kafka, brokerAddress, logLevel, saslConfig, createClient }

	return { ctx, clients }
}

/**
 * Build environment configuration for the broker based on SASL mechanism
 */
function buildEnvConfig(mechanism: SaslMechanism, username: string, password: string): Record<string, string> {
	switch (mechanism) {
		case 'PLAIN': {
			const jaasConfig = `org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_admin="admin-secret" user_${username}="${password}";`
			return {
				// Configure listeners: PLAINTEXT for external (with SASL), BROKER for inter-broker
				KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT',
				KAFKA_SASL_ENABLED_MECHANISMS: 'PLAIN',
				KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG: jaasConfig,
			}
		}

		case 'SCRAM-SHA-256':
		case 'SCRAM-SHA-512': {
			// SCRAM listener JAAS config is for broker authentication
			const scramJaasConfig = 'org.apache.kafka.common.security.scram.ScramLoginModule required;'
			const scramMechanism = mechanism === 'SCRAM-SHA-256' ? 'SCRAM-SHA-256' : 'SCRAM-SHA-512'

			return {
				KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT',
				KAFKA_SASL_ENABLED_MECHANISMS: scramMechanism,
				KAFKA_LISTENER_NAME_PLAINTEXT_SASL_JAAS_CONFIG: scramJaasConfig,
			}
		}

		default: {
			const exhaustive: never = mechanism
			throw new Error(`Unsupported SASL mechanism: ${exhaustive}`)
		}
	}
}

async function configureScramUser(
	kafka: StartedKafkaContainer,
	mechanism: Exclude<SaslMechanism, 'PLAIN'>,
	username: string,
	password: string,
	useKraft: boolean
): Promise<void> {
	const scramMechanism = mechanism === 'SCRAM-SHA-256' ? 'SCRAM-SHA-256' : 'SCRAM-SHA-512'
	const configValue = `${scramMechanism}=[password=${password}]`

	if (useKraft) {
		const command = [
			"cat <<'EOF' > /tmp/sasl.properties",
			'security.protocol=SASL_PLAINTEXT',
			'sasl.mechanism=PLAIN',
			'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
			'EOF',
			`kafka-configs --bootstrap-server localhost:9093 --command-config /tmp/sasl.properties --alter --add-config '${configValue}' --entity-type users --entity-name ${username}`,
		].join('\n')

		await kafka.exec(['bash', '-c', command])
	} else {
		await kafka.exec([
			'kafka-configs',
			'--zookeeper',
			'localhost:2181',
			'--alter',
			'--entity-type',
			'users',
			'--entity-name',
			username,
			'--add-config',
			configValue,
		])
	}
}
