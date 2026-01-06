import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka'
import { Wait } from 'testcontainers'
import { existsSync } from 'node:fs'
import { execSync } from 'node:child_process'

const DEFAULT_KAFKA_IMAGE = 'confluentinc/cp-kafka:7.5.0'
const KAFKA_PORT = 9093

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

async function startMainContainer(image: string): Promise<StartedKafkaContainer> {
	return new KafkaContainer(image)
		.withKraft()
		.withEnvironment({
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
			// Enable ACL authorization with StandardAuthorizer (KRaft mode)
			KAFKA_AUTHORIZER_CLASS_NAME: 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
			// Allow ANONYMOUS user (unauthenticated connections) to be a super user for testing
			KAFKA_SUPER_USERS: 'User:ANONYMOUS',
			// Allow operations when no ACL is found (less restrictive for testing)
			KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: 'true',
		})
		.withStartupTimeout(60_000)
		.start()
}

async function startSaslPlainContainer(image: string): Promise<StartedKafkaContainer> {
	const username = 'testuser'
	const password = 'testpass'
	const jaasConfig = `org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_admin="admin-secret" user_${username}="${password}";`

	return new KafkaContainer(image)
		.withKraft()
		.withEnvironment({
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
			KAFKA_INTER_BROKER_LISTENER_NAME: 'BROKER',
			KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT',
			KAFKA_SASL_ENABLED_MECHANISMS: 'PLAIN',
			KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG: jaasConfig,
		})
		.withWaitStrategy(Wait.forAll([Wait.forListeningPorts(), Wait.forLogMessage(/Kafka Server started/)]))
		.withStartupTimeout(60_000)
		.start()
}

async function startSaslScramContainer(
	image: string,
	mechanism: 'SCRAM-SHA-256' | 'SCRAM-SHA-512'
): Promise<StartedKafkaContainer> {
	const username = 'testuser'
	const password = 'testpass'
	const scramJaasConfig = 'org.apache.kafka.common.security.scram.ScramLoginModule required;'
	const jaasFileContent = 'KafkaServer { org.apache.kafka.common.security.scram.ScramLoginModule required; };'

	// SCRAM requires ZooKeeper mode (no withKraft)
	const container = await new KafkaContainer(image)
		.withCopyContentToContainer([{ content: jaasFileContent, target: '/etc/kafka/jaas.conf', mode: 0o644 }])
		.withEnvironment({
			KAFKA_OPTS: '-Djava.security.auth.login.config=/etc/kafka/jaas.conf',
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
			KAFKA_INTER_BROKER_LISTENER_NAME: 'BROKER',
			KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT',
			KAFKA_SASL_ENABLED_MECHANISMS: mechanism,
			KAFKA_LISTENER_NAME_PLAINTEXT_SASL_JAAS_CONFIG: scramJaasConfig,
		})
		.withWaitStrategy(Wait.forListeningPorts())
		.withStartupTimeout(60_000)
		.start()

	// Configure SCRAM user via ZooKeeper (SCRAM runs in ZooKeeper mode)
	const configValue = `${mechanism}=[password=${password}]`
	await container.exec([
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

	return container
}

export default async function globalSetup() {
	// Allow using externally-provided brokers
	if (process.env.KAFKA_BROKERS) {
		return
	}

	// Fix host detection when running inside Docker
	if (!process.env.TESTCONTAINERS_HOST_OVERRIDE && existsSync('/.dockerenv')) {
		const gateway = resolveDockerHostGateway()
		if (gateway) {
			process.env.TESTCONTAINERS_HOST_OVERRIDE = gateway
		}
	}

	const image = process.env.KAFKA_TEST_IMAGE ?? DEFAULT_KAFKA_IMAGE
	const containers: StartedKafkaContainer[] = []

	// Start all containers in parallel for maximum speed
	const [mainContainer, saslPlainContainer, saslScram256Container, saslScram512Container] = await Promise.all([
		startMainContainer(image),
		startSaslPlainContainer(image),
		startSaslScramContainer(image, 'SCRAM-SHA-256'),
		startSaslScramContainer(image, 'SCRAM-SHA-512'),
	])

	containers.push(mainContainer, saslPlainContainer, saslScram256Container, saslScram512Container)

	// Export broker addresses for all containers
	const getBrokerAddress = (c: StartedKafkaContainer) => `${c.getHost()}:${c.getMappedPort(KAFKA_PORT)}`

	process.env.KAFKA_BROKERS = getBrokerAddress(mainContainer)
	process.env.KAFKA_SASL_PLAIN_BROKERS = getBrokerAddress(saslPlainContainer)
	process.env.KAFKA_SASL_SCRAM_256_BROKERS = getBrokerAddress(saslScram256Container)
	process.env.KAFKA_SASL_SCRAM_512_BROKERS = getBrokerAddress(saslScram512Container)
	process.env.KAFKA_TS_LOG_LEVEL ??= 'error'

	return async () => {
		await Promise.all(containers.map(c => c.stop()))
	}
}
