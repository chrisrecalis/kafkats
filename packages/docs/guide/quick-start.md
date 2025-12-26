# Quick Start

This guide will help you send and receive your first messages with kafkats.

## Prerequisites

Make sure you have:

- [Installed @kafkats/client](/guide/installation)
- A running Kafka broker (e.g., via Docker)

Start Kafka locally with Docker:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  bitnami/kafka:latest
```

## Creating a Client

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})
```

## Producing Messages

```typescript
const producer = client.producer()

// Send a single message
await producer.send('my-topic', [{ value: 'Hello, Kafka!' }])

// Send multiple messages with keys
await producer.send('my-topic', [
	{ key: 'user-1', value: JSON.stringify({ action: 'login' }) },
	{ key: 'user-2', value: JSON.stringify({ action: 'signup' }) },
])

// Don't forget to close when done
await producer.disconnect()
```

## Consuming Messages

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	autoOffsetReset: 'earliest',
})

// Process messages one at a time
consumer.subscribe('my-topic')
await consumer.runEach(async (message, ctx) => {
	console.log({
		topic: ctx.topic,
		partition: ctx.partition,
		offset: ctx.offset,
		key: message.key?.toString(),
		value: message.value?.toString(),
	})
})
```

## Complete Example

Here's a complete example that produces and consumes messages:

```typescript
import { KafkaClient } from '@kafkats/client'

async function main() {
	const client = new KafkaClient({
		clientId: 'quickstart-app',
		brokers: ['localhost:9092'],
	})

	// Create producer and send messages
	const producer = client.producer()

	await producer.send('quickstart', [
		{ key: 'greeting', value: 'Hello from kafkats!' },
		{ key: 'farewell', value: 'Goodbye from kafkats!' },
	])

	console.log('Messages sent!')
	await producer.disconnect()

	// Create consumer and read messages
	const consumer = client.consumer({
		groupId: 'quickstart-group',
		autoOffsetReset: 'earliest',
	})

	console.log('Waiting for messages...')

	let seen = 0
	consumer.subscribe('quickstart')
	await consumer.runEach(async message => {
		console.log(`Received: ${message.key?.toString()} = ${message.value?.toString()}`)
		if (++seen >= 2) consumer.stop()
	})
}

main().catch(console.error)
```

## Next Steps

- Learn about [core concepts](/guide/concepts)
- Explore the [Producer API](/client/producer)
- Explore the [Consumer API](/client/consumer)
- Try [stream processing with Flow](/flow/)
