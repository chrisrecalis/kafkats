# Getting Started with @kafkats/client

## Creating a Client

The `KafkaClient` is the main entry point for all Kafka operations:

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-application',
	brokers: ['localhost:9092'],
})
```

### Client Options

| Option                      | Type         | Description                                         |
| --------------------------- | ------------ | --------------------------------------------------- |
| `clientId`                  | `string`     | Identifier for this client (appears in broker logs) |
| `brokers`                   | `string[]`   | Bootstrap broker addresses                          |
| `tls`                       | `TlsConfig`  | TLS configuration (omit for plaintext)              |
| `sasl`                      | `SaslConfig` | SASL authentication configuration                   |
| `connectionTimeoutMs`       | `number`     | Connection timeout in ms (default: 10000)           |
| `requestTimeoutMs`          | `number`     | Request timeout in ms (default: 30000)              |
| `metadataRefreshIntervalMs` | `number`     | Metadata refresh interval (default: 300000)         |
| `maxInFlightRequests`       | `number`     | Max in-flight requests per connection (default: 5)  |

### TLS Configuration

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true }, // Use system CA
})

// Or with custom certificates
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: {
		enabled: true,
		ca: fs.readFileSync('ca.pem'),
		cert: fs.readFileSync('client.pem'),
		key: fs.readFileSync('client-key.pem'),
	},
})
```

## Creating a Producer

```typescript
const producer = client.producer({
	acks: 'all', // Wait for all replicas
	compression: 'snappy', // Compress messages
	lingerMs: 5, // Batch for 5ms
})

// Send messages
await producer.send('my-topic', [{ value: 'Hello!' }])

// Close when done
await producer.disconnect()
```

See [Producer API](/client/producer) for full documentation.

## Creating a Consumer

```typescript
const consumer = client.consumer({
	groupId: 'my-consumer-group',
	autoOffsetReset: 'earliest',
})

// Process messages
await consumer.runEach('my-topic', async (message, ctx) => {
	console.log(`${ctx.topic}[${ctx.partition}] @ ${ctx.offset}: ${message.value}`)
})
```

See [Consumer API](/client/consumer) for full documentation.

## Creating a ShareConsumer (experimental)

Kafka Share Groups (KIP-932) provide queue-like consumption with per-record acknowledgements.
They require Kafka 4.1+ with Share Groups enabled (see [ShareConsumer API](/client/share-consumer)).

```typescript
const shareConsumer = client.shareConsumer({
	groupId: 'my-share-group',
})

await shareConsumer.runEach('my-topic', async message => {
	// For string topic subscriptions, key/value are raw Buffers (same as Consumer).
	await process(message.value.toString('utf-8'))

	// If you don't call ack/release/reject, the message is implicitly ack'd (ACCEPT) on success.
	// await message.release()
	// await message.reject()
})
```

See [ShareConsumer API](/client/share-consumer) for requirements and full documentation.

## Typed Topics

Define topics with type-safe codecs:

```typescript
import { topic, string, json } from '@kafkats/client'

interface UserEvent {
	userId: string
	action: 'login' | 'logout'
	timestamp: number
}

const userEvents = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})

// Producer - type-checked
await producer.send(userEvents, [
	{
		key: 'user-123',
		value: { userId: 'user-123', action: 'login', timestamp: Date.now() },
	},
])

// Consumer - type-inferred
await consumer.runEach(userEvents, async message => {
	// message.key is string, message.value is UserEvent
	console.log(`User ${message.value.userId} performed ${message.value.action}`)
})
```

## Error Handling

kafkats provides specific error types for different failure scenarios:

```typescript
import { KafkaError, ConnectionError, TimeoutError, isRetriable } from '@kafkats/client'

try {
	await producer.send('events', [{ value: 'data' }])
} catch (error) {
	if (error instanceof ConnectionError) {
		console.log('Connection failed:', error.message)
	} else if (error instanceof TimeoutError) {
		console.log('Request timed out')
	} else if (isRetriable(error)) {
		console.log('Retriable error, will retry automatically')
	}
}
```

## Graceful Shutdown

Always close clients when shutting down:

```typescript
process.on('SIGTERM', async () => {
	consumer.stop()
	await producer.disconnect()
	process.exit(0)
})
```
