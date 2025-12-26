# @kafkats/client

The core Kafka client package providing producer, consumer, and low-level protocol access.

## Features

- **Pure Protocol** - Direct Kafka wire protocol implementation, no native dependencies
- **Type-Safe** - Full TypeScript support with comprehensive types
- **High Performance** - Optimized batching, zero-copy operations
- **SASL Authentication** - PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **Transactions** - Full exactly-once semantics support
- **Compression** - gzip, snappy, lz4, zstd

## Installation

```bash
pnpm add @kafkats/client
```

## Quick Example

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

// Create a producer
const producer = client.producer({
	acks: 'all',
	compression: 'snappy',
})

await producer.send('events', [{ key: 'user-1', value: JSON.stringify({ action: 'click' }) }])

// Create a consumer
const consumer = client.consumer({ groupId: 'my-group' })

consumer.subscribe('events')
await consumer.runEach(async message => {
	console.log(message.value.toString())
})
```

## Architecture

```
@kafkats/client
├── KafkaClient      # Main entry point
├── Producer         # Message production with batching
├── Consumer         # Consumer groups and message handling
├── Cluster          # Broker discovery and metadata
└── Protocol         # Wire protocol encoding/decoding
```

## Next Steps

- [Getting Started](/client/getting-started) - Basic setup and usage
- [Producer API](/client/producer) - Sending messages
- [Consumer API](/client/consumer) - Receiving messages
- [Codecs](/client/codecs) - Type-safe serialization
- [Authentication](/client/authentication) - SASL configuration
