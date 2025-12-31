# @kafkats/client

A pure-protocol TypeScript Kafka client with producer, consumer, and admin APIs.

## Features

- **Pure TypeScript** — Direct protocol implementation, not a wrapper
- **Producer** — Batching, compression (gzip, snappy, lz4, zstd), custom partitioners
- **Consumer** — Consumer groups, partition assignment, offset management
- **Admin** — Topic management, consumer groups, cluster metadata
- **SASL Auth** — PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **Typed Codecs** — Built-in string, JSON, and buffer codecs

## Installation

```bash
npm install @kafkats/client
```

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({ brokers: ['localhost:9092'] })

// Producer
const producer = client.producer()
await producer.connect()
await producer.send('events', [{ key: 'user-1', value: 'hello' }])

// Consumer
const consumer = client.consumer({ groupId: 'my-group' })
await consumer.runEach('events', async (message, ctx) => {
	console.log(message.value?.toString())
})

// Admin
const admin = client.admin()
await admin.connect()
await admin.createTopics([{ name: 'events', partitions: 3 }])
```

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

## License

MIT
