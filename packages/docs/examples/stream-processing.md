# Stream Processing

Basic stream transformations with @kafkats/flow.

## Code

```typescript
import { flow, topic, string, json } from '@kafkats/flow'

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
const rawEvents = topic('raw-events', { key: string(), value: json<RawEvent>() })

async function main() {
	const app = flow({
		applicationId: 'event-processor',
		client: { clientId: 'event-processor', brokers: ['localhost:9092'] },
	})

	// Build processing topology
	const { 'events-valid': valid, 'events-invalid': invalid } = app
		.stream(rawEvents)
		// Log incoming events
		.peek((key, event) => {
			console.log(`Received: ${event?.type} from ${event?.userId}`)
		})
		// Split valid and invalid events: each event goes to the first branch that matches
		.split({ name: 'events-' })
		.branch((_, event) => event !== null && event.type !== 'heartbeat' && event.userId.length > 0, {
			name: 'valid',
		})
		.defaultBranch({ name: 'invalid' })

	// Process valid events
	valid!
		.mapValues((event): ProcessedEvent => ({
			type: event!.type,
			userId: event!.userId,
			data: event!.data,
			processedAt: Date.now(),
			source: 'event-processor',
		}))
		.peek((key, event) => {
			console.log(`Processed: ${event?.type}`)
		})
		.to('processed-events', { key: string(), value: json<ProcessedEvent>() })

	// Route invalid events to the error topic
	invalid!
		.peek((key, event) => {
			console.log(`Invalid event: ${JSON.stringify(event)}`)
		})
		.to('error-events')

	// Handle shutdown
	process.on('SIGTERM', () => {
		console.log('Shutting down...')
		void app.close()
	})

	// Start processing
	await app.start()
	console.log('Stream processor started')
}

main().catch(console.error)
```

## How It Works

1. **Read** from `raw-events` topic
2. **Peek** to log each event (side effect)
3. **Split** into valid and invalid streams (`split()` with a named branch and a default branch)
4. **Transform** valid events with processing metadata
5. **Write** to respective output topics

## Topology Visualization

```
raw-events
    │
    ├──► peek (log)
    │
    ├──► split
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
