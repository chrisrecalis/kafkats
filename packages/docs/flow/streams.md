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
// Return a [key, value] tuple
stream.map((key, value) => [value.userId, { ...value, processed: true }] as const)
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
// The function receives (value, key)
stream.selectKey(value => value.userId)
```

`map()` and `selectKey()` can change which partition a key belongs to. If the re-keyed stream is followed by `groupByKey()`, a join or `toTable()`, Flow [repartitions](#repartitioning) it automatically.

### filter

Keep only matching records:

```typescript
stream.filter((key, value) => value.amount > 100)
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
const merged = stream1.merge(stream2).merge(stream3).merge(stream4)
```

If a merged stream feeds a stateful operation, all merged topics must have the same partition count, otherwise `start()` throws `CoPartitioningError`. See [Co-partitioning](/flow/state-stores#co-partitioning).

## Output

### to

Write to a topic (terminal operation):

```typescript
stream.to('output-topic')

// With options
stream.to('output-topic', {
	partitioner: (key, _value, partitionCount) => {
		if (key === null) return 0
		return String(key).length % partitionCount
	},
})
```

`to()` and `through()` take the topic name. Records are encoded with the `key` and `value` codecs from the options, or else with the codecs the stream carries. `mapValues()`, `flatMapValues()` and `map()` drop the value codec, and `map()` and `selectKey()` drop the key codec, so pass the codecs when writing such a stream; a record that is not a `Buffer` and has no codec fails to encode.

Records written with `to()` or `through()` keep the timestamp of the input record, so windowing and joins downstream see the original event time.

### through

Write to a topic and continue from what is read back from it:

```typescript
stream
	.through(intermediateTopic)
	.mapValues(value => transform(value))
	.to(finalTopic, { value: json() })
```

`through()` is a real repartition point. Records are written to the topic, and the rest of the topology processes them from the partition they were written to. Use it to repartition with your own partitioner or partition count:

```typescript
orders
	.through(ordersByUser, {
		key: string(),
		value: json<Order>(),
		partitioner: (key, _value, partitionCount) => String(key).length % partitionCount,
	})
	.join(users, joiner)
```

The topic must already exist, because Flow does not create it. When the group has no committed offset for it, it is read from the beginning, so records written before the first assignment are not skipped. If the stream was transformed with `map()` or `selectKey()`, pass the `key` and `value` codecs in the options.

## Grouping

### groupByKey

Group by existing key:

```typescript
const grouped = stream.groupByKey()
// Returns KGroupedStream
```

### groupBy

Group by a new key. The new key needs a codec, and the records are [repartitioned](#repartitioning) so that all records of a key are processed in one partition:

```typescript
const grouped = stream.groupBy((key, value) => value.category, {
	key: string(),
	value: json<Order>(),
	name: 'orders-by-category', // stable name for the repartition topic
})
```

## Conversion

### toTable

Convert stream to table:

```typescript
const table = stream.toTable()
```

A re-keyed stream is [repartitioned](#repartitioning) before it becomes a table.

## Repartitioning

State is kept per partition, so all records of a key must be processed in the same partition. After `selectKey()` or `map()` that is no longer guaranteed. Flow therefore writes the stream through an internal topic and reads it back when a re-keyed stream is followed by:

- `groupByKey()`
- a join (`join()`, `leftJoin()`, `outerJoin()`)
- `toTable()`

`groupBy(fn, options)` always repartitions, because it changes the key itself.

The results of a windowed aggregation are repartitioned in the same way. They are keyed by key and window, but stay in the partition of the key alone, which is not where anything else keyed by key and window would be. So `windowedBy(...).count().toStream()` followed by `groupByKey()`, a join or `toTable()` goes through an internal topic too. Writing the results to a topic with `to()` does not.

```typescript
app.stream(events)
	.selectKey(event => event.userId)
	.groupByKey({ key: string(), value: json<Event>(), name: 'events-by-user' })
	.count()
```

The internal topic is named `<applicationId>-<name>-repartition`. It is created at startup with the partition count of the upstream topic (the largest one, if several topics feed it). With `changelog.autoCreate: false` it must already exist, and an existing topic with a different partition count fails `start()` with `ChangelogPartitionMismatchError`. Newly assigned partitions of repartition topics are read from the beginning when the group has no committed offset.

Repartitioning needs both key and value codecs. If they cannot be inferred from the stream, you get an error that says so, and you pass them via the options (`groupBy(fn, { key, value })`, `groupByKey({ key, value })`, or `key`/`value` on a join's options).

::: tip Name your repartition topics
Pass `name` in the `Grouped` options for `groupBy()` and `groupByKey()`. Without it, topics are numbered (`repartition-0`, `repartition-1`, ...) in the order the topology is built, so adding or reordering an operation renames the topics. Joins and `toTable()` cannot be named, so they always use numbered topics.
:::

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
		branch.to(topics[index], { value: json<ProcessedEvent>() })
	})
```

## Type Parameters

KStream operations preserve and transform types:

```typescript
// KStream<string, UserEvent>
const stream = app.stream(userEvents)

// KStream<string, { count: number }> - value type changed
const mapped = stream.mapValues(e => ({ count: 1 }))

// KStream<string, UserEvent> - key changed to userId
const rekeyed = stream.selectKey(e => e.userId)
```
