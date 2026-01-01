# Simple Producer

A basic example of producing messages to Kafka.

## Code

```typescript
import { CompressionType, KafkaClient, compressionCodecs, createSnappyCodec, topic, string, json } from '@kafkats/client'
import snappy from 'snappy'

// Define a typed topic
interface UserEvent {
	userId: string
	action: 'login' | 'logout' | 'signup'
	timestamp: number
}

const userEvents = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})

async function main() {
	// Create client
	const client = new KafkaClient({
		clientId: 'simple-producer',
		brokers: ['localhost:9092'],
	})

	// Register non-gzip codecs once at startup
	compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))

	// Create producer
	const producer = client.producer({
		acks: 'all', // Wait for all replicas
		compression: 'snappy',
	})

	try {
		// Send some events
		const events: Array<{ key: string; value: UserEvent }> = [
			{
				key: 'user-1',
				value: { userId: 'user-1', action: 'login', timestamp: Date.now() },
			},
			{
				key: 'user-2',
				value: { userId: 'user-2', action: 'signup', timestamp: Date.now() },
			},
			{
				key: 'user-1',
				value: { userId: 'user-1', action: 'logout', timestamp: Date.now() + 1000 },
			},
		]

		// Send messages
		const results = await producer.send(userEvents, events)

		// Print results
		for (const result of results) {
			console.log(`Sent to ${result.topic}[${result.partition}] @ offset ${result.offset}`)
		}

		// Ensure all messages are sent
		await producer.flush()
		console.log('All messages sent!')
	} finally {
		// Close producer
		await producer.disconnect()
	}
}

main().catch(console.error)
```

## Output

```
Sent to user-events[0] @ offset 0
Sent to user-events[0] @ offset 1
Sent to user-events[0] @ offset 2
All messages sent!
```

## Key Points

1. **Typed topics** - Use `topic()` with codecs for type safety
2. **Acknowledgments** - `acks: 'all'` ensures durability
3. **Compression** - Reduces network bandwidth
4. **Flush** - Ensures all batched messages are sent
5. **Disconnect** - Always disconnect the producer when done

## Variations

### Fire and Forget

Don't wait for acknowledgments (fastest, least reliable):

```typescript
const producer = client.producer({
	acks: 'none',
})
```

### With Headers

Add metadata to messages:

```typescript
await producer.send(userEvents, [
	{
		key: 'user-1',
		value: { userId: 'user-1', action: 'login', timestamp: Date.now() },
		headers: {
			'trace-id': 'abc123',
			source: 'web-app',
		},
	},
])
```

### Multiple Topics

Send to different topics with separate calls:

```typescript
await producer.send('events', [{ key: 'k1', value: JSON.stringify({ type: 'event' }) }])
await producer.send('logs', [{ key: 'k2', value: JSON.stringify({ type: 'log' }) }])
```
