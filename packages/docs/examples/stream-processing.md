# Stream Processing

Basic stream transformations with @kafkats/flow.

## Code

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

// Input event
interface RawEvent {
	type: string
	userId: string
	data: Record<string, unknown>
	timestamp: number
}

// Output event
interface ProcessedEvent {
	type: string
	userId: string
	data: Record<string, unknown>
	processedAt: number
	source: string
}

// Define topics
const rawEvents = topic('raw-events', {
	key: string(),
	value: json<RawEvent>(),
})

const processedEvents = topic('processed-events', {
	key: string(),
	value: json<ProcessedEvent>(),
})

const errorEvents = topic('error-events', {
	key: string(),
	value: json<RawEvent>(),
})

async function main() {
	const app = flow({
		applicationId: 'event-processor',
		client: { clientId: 'event-processor', brokers: ['localhost:9092'] },
	})

	// Build processing topology
	const [valid, invalid] = app
		.stream(rawEvents)
		// Log incoming events
		.peek((key, event) => {
			console.log(`Received: ${event.type} from ${event.userId}`)
		})
		// Split valid and invalid events
		.branch(
			(_, event) => event.type !== 'heartbeat' && event.userId.length > 0,
			() => true
		)

	// Process valid events
	valid
		.mapValues(
			event =>
				({
					type: event.type,
					userId: event.userId,
					data: event.data,
					processedAt: Date.now(),
					source: 'event-processor',
				}) as ProcessedEvent
		)
		.peek((key, event) => {
			console.log(`Processed: ${event.type}`)
		})
		.to(processedEvents)

	// Route invalid events to error topic
	invalid
		.peek((key, event) => {
			console.log(`Invalid event: ${JSON.stringify(event)}`)
		})
		.to(errorEvents)

	// Handle shutdown
	process.on('SIGTERM', async () => {
		console.log('Shutting down...')
		await app.close()
	})

	// Start processing
	console.log('Starting stream processor...')
	await app.start()
}

main().catch(console.error)
```

## How It Works

1. **Read** from `raw-events` topic
2. **Peek** to log each event (side effect)
3. **Branch** into valid and invalid streams
4. **Transform** valid events with processing metadata
5. **Write** to respective output topics

## Topology Visualization

```
raw-events
    │
    ├──► peek (log)
    │
    ├──► branch
    │       │
    │       ├── valid ──► mapValues ──► peek ──► processed-events
    │       │
    │       └── invalid ──► peek ──► error-events
```

## Testing the Example

1. Start the processor:

```bash
npx tsx stream-processing.ts
```

2. In another terminal, produce some events:

```typescript
import { KafkaClient, topic, string, json } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'test-producer',
	brokers: ['localhost:9092'],
})

const producer = client.producer()

await producer.send('raw-events', [
	{
		key: 'user-1',
		value: JSON.stringify({
			type: 'click',
			userId: 'user-1',
			data: { page: '/home' },
			timestamp: Date.now(),
		}),
	},
	{
		key: 'heartbeat',
		value: JSON.stringify({
			type: 'heartbeat',
			userId: 'user-1',
			data: {},
			timestamp: Date.now(),
		}),
	},
])

await producer.disconnect()
```

3. Check the output topics:

```bash
# Processed events (valid)
kafka-console-consumer --topic processed-events --from-beginning

# Error events (invalid)
kafka-console-consumer --topic error-events --from-beginning
```
