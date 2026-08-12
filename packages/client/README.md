# @kafkats/client

A pure-protocol TypeScript Kafka client with producer, consumer, and admin APIs.

## Features

- **Pure TypeScript** — Direct protocol implementation, not a wrapper
- **Producer** — Batching, compression (gzip, snappy, lz4, zstd), custom partitioners
- **Consumer** — Consumer groups, partition assignment, offset management
- **ShareConsumer (experimental)** — Share Groups (KIP-932) with per-record acknowledgements
- **Admin** — Topic management, consumer groups, cluster metadata
- **SASL Auth** — PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
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

// SASL OAUTHBEARER (e.g. AWS MSK IAM)
const clientWithSasl = new KafkaClient({
	brokers: ['b-1.example:9098'],
	clientId: 'my-app',
	tls: { enabled: true },
	sasl: {
		mechanism: 'OAUTHBEARER',
		oauthBearerProvider: async ({ host, port }) => ({
			value: await getTokenForBroker(`${host}:${port}`),
		}),
	},
})

// Consumer
const consumer = client.consumer({ groupId: 'my-group' })
await consumer.runEach('events', async (message, ctx) => {
	console.log(message.value?.toString())
})

// ShareConsumer (experimental)
const shareConsumer = client.shareConsumer({ groupId: 'my-share-group' })
await shareConsumer.runEach('events', async message => {
	console.log(message.value?.toString())
	// If you don't call ack/release/reject, the message is implicitly ack'd (ACCEPT) on success.
})

// Admin
const admin = client.admin()
await admin.connect()
await admin.createTopics([{ name: 'events', partitions: 3 }])
```

## Compression

GZIP works out of the box. For Snappy, LZ4, or Zstd, install a supported library — it is detected
and registered automatically, no registration code needed:

```bash
npm install snappy   # or: lz4-napi, @mongodb-js/zstd
```

```typescript
const producer = client.producer({ compression: 'snappy' })
```

Consumers decompress automatically. See the [compression docs](https://chrisrecalis.github.io/kafkats/client/compression) for supported libraries and manual registration.

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

> **For AI agents / LLMs**: fetch <https://chrisrecalis.github.io/kafkats/llms.txt> for an index of all
> documentation pages, or <https://chrisrecalis.github.io/kafkats/llms-full.txt> for the complete
> documentation as one file. Every docs page is also available as markdown by replacing `.html` with `.md`.

## License

MIT
