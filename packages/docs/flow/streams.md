# KStream

A KStream represents an unbounded stream of key-value records. Each record is an independent event.

## Creating a Stream

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
})

// From a topic
const stream = app.stream(myTopic)

// With explicit types
const stream = app.stream<string, MyEvent>(myTopic)
```

## Transformation Operations

### map

Transform both key and value:

```typescript
stream.map((key, value) => ({
	key: value.userId,
	value: { ...value, processed: true },
}))
```

### mapValues

Transform only the value (preserves key):

```typescript
stream.mapValues(value => ({
	...value,
	timestamp: Date.now(),
}))
```

### selectKey

Change the key:

```typescript
stream.selectKey((key, value) => value.userId)
```

### filter

Keep only matching records:

```typescript
stream.filter((key, value) => value.amount > 100)
```

### filterNot

Remove matching records:

```typescript
stream.filterNot((key, value) => value.deleted)
```

### flatMap

Emit multiple records per input:

```typescript
stream.flatMap((key, value) => [
	{ key: `${key}-1`, value: value.part1 },
	{ key: `${key}-2`, value: value.part2 },
])
```

### flatMapValues

Emit multiple values per input:

```typescript
stream.flatMapValues(value => value.items)
```

## Side Effects

### peek

Perform side effects without modifying the stream:

```typescript
stream.peek((key, value) => {
	console.log(`Processing: ${key}`)
	metrics.increment('processed')
})
```

## Branching

### branch

Split stream into multiple branches:

```typescript
const [highValue, lowValue] = stream.branch(
	(key, value) => value.amount > 1000,
	(key, value) => value.amount <= 1000
)

highValue.to(highValueTopic)
lowValue.to(lowValueTopic)
```

## Merging

### merge

Combine multiple streams:

```typescript
const merged = stream1.merge(stream2)

// Or merge multiple
const merged = stream1.merge(stream2, stream3, stream4)
```

## Output

### to

Write to a topic (terminal operation):

```typescript
stream.to(outputTopic)

// With options
stream.to(outputTopic, {
	partitioner: (key, _value, partitionCount) => {
		if (key === null) return 0
		return String(key).length % partitionCount
	},
})
```

### through

Write to topic and continue processing:

```typescript
stream
	.through(intermediateTopic)
	.mapValues(value => transform(value))
	.to(finalTopic)
```

## Grouping

### groupByKey

Group by existing key:

```typescript
const grouped = stream.groupByKey()
// Returns KGroupedStream
```

### groupBy

Group by a new key:

```typescript
const grouped = stream.groupBy((key, value) => value.category)
```

## Conversion

### toTable

Convert stream to table:

```typescript
const table = stream.toTable()
```

## Example: Event Processing Pipeline

```typescript
interface RawEvent {
	type: string
	userId: string
	data: unknown
	timestamp: number
}

interface ProcessedEvent {
	type: string
	userId: string
	data: unknown
	processedAt: number
	source: string
}

app.stream<string, RawEvent>(rawEvents)
	// Filter valid events
	.filter((_, event) => event.type !== 'heartbeat')

	// Enrich
	.mapValues(
		event =>
			({
				...event,
				processedAt: Date.now(),
				source: 'stream-processor',
			}) as ProcessedEvent
	)

	// Log
	.peek((key, event) => {
		logger.debug(`Processing event ${event.type} for user ${event.userId}`)
	})

	// Route by type
	.branch(
		(_, e) => e.type === 'purchase',
		(_, e) => e.type === 'pageview',
		() => true // default
	)
	.forEach((branch, index) => {
		const topics = [purchasesTopic, pageviewsTopic, otherEventsTopic]
		branch.to(topics[index])
	})
```

## Type Parameters

KStream operations preserve and transform types:

```typescript
// KStream<string, UserEvent>
const stream = app.stream(userEvents)

// KStream<string, { count: number }> - value type changed
const mapped = stream.mapValues(e => ({ count: 1 }))

// KStream<string, { count: number }> - key type changed to userId
const rekeyed = mapped.selectKey((_, v) => v.userId)
```
