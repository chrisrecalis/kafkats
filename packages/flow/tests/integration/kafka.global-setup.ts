import { KafkaContainer } from '@testcontainers/kafka'
import { existsSync } from 'node:fs'
import { execSync } from 'node:child_process'

const DEFAULT_KAFKA_IMAGE = 'confluentinc/cp-kafka:7.6.0'
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

export default async function globalSetup() {
	// Allow using an externally-provided Kafka (e.g. CI service container)
	if (process.env.KAFKA_BROKERS) {
		return
	}

	// When running tests inside a container, Testcontainers can mis-detect the host as "localhost",
	// causing Kafka to advertise unreachable broker addresses to the client. Override the host to the
	// container's default gateway so advertised.listeners are reachable.
	if (!process.env.TESTCONTAINERS_HOST_OVERRIDE && existsSync('/.dockerenv')) {
		const gateway = resolveDockerHostGateway()
		if (gateway) {
			process.env.TESTCONTAINERS_HOST_OVERRIDE = gateway
		}
	}

	const image = process.env.KAFKA_TEST_IMAGE ?? DEFAULT_KAFKA_IMAGE

	const container = await new KafkaContainer(image).withKraft().withStartupTimeout(60_000).start()

	process.env.KAFKA_BROKERS = `${container.getHost()}:${container.getMappedPort(KAFKA_PORT)}`
	process.env.KAFKA_TS_LOG_LEVEL ??= 'error'

	return async () => {
		await container.stop()
	}
}
