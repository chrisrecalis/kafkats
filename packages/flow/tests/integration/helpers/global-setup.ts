import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka'
import { Wait } from 'testcontainers'

let kafka: StartedKafkaContainer

export default async function globalSetup() {
	kafka = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
		.withKraft()
		.withEnvironment({
			KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
			KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
		})
		.withWaitStrategy(Wait.forAll([Wait.forListeningPorts(), Wait.forLogMessage(/Kafka Server started/)]))
		.start()

	const brokerAddress = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`
	process.env.KAFKA_BROKER_ADDRESS = brokerAddress

	return async () => {
		await kafka.stop()
	}
}
