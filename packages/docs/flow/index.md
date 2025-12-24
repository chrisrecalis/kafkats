# @kafkats/flow

Kafka Streams-like flow APIs for building stream processing applications in TypeScript.

## Features

- **Kafka Streams DSL** - Familiar APIs: KStream, KTable, windowing, joins
- **Exactly-Once Semantics** - Transactional processing with batch commits
- **Type-Safe** - Full TypeScript support with strong typing
- **Pluggable State** - In-memory and LMDB state stores
- **Windowing** - Time, session, and sliding windows
- **Joins** - Stream-stream and stream-table joins
- **Testing** - Built-in test utilities

## Installation

```bash
pnpm add @kafkats/flow
```

## Quick Example

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

interface ClickEvent {
	userId: string
	page: string
}

interface ClickCount {
	userId: string
	count: number
}

// Define topics
const clicks = topic('clicks', {
	key: string(),
	value: json<ClickEvent>(),
})

const counts = topic('click-counts', {
	key: string(),
	value: json<ClickCount>(),
})

// Create app
const app = flow({
	applicationId: 'click-counter',
	client: { clientId: 'click-counter', brokers: ['localhost:9092'] },
})

// Build topology
app.stream(clicks)
	.groupByKey()
	.count()
	.toStream()
	.mapValues((count, key) => ({ userId: key, count }))
	.to(counts)

// Start processing
await app.start()
```

## Core Concepts

### KStream

An unbounded stream of key-value records. Each record is an independent event.

```typescript
app.stream(inputTopic)
	.filter((key, value) => value.amount > 100)
	.mapValues(value => ({ ...value, processed: true }))
	.to(outputTopic)
```

### KTable

A changelog stream representing a table. Each key has a latest value.

```typescript
const usersTable = app.table(usersTopic)
usersTable.mapValues(user => user.email)
```

### Windowing

Group records by time for aggregations:

```typescript
import { TimeWindows } from '@kafkats/flow'

app.stream(clicks).groupByKey().windowedBy(TimeWindows.of('5m')).count()
```

## Architecture

```
@kafkats/flow
├── flow()           # Create streaming application
├── topic()          # Define typed topics
├── KStream          # Unbounded record stream
├── KTable           # Changelog table
├── KGroupedStream   # Grouped stream for aggregations
├── Windowing        # Time, session, sliding windows
└── State Stores     # In-memory and persistent

@kafkats/client (codecs)
├── string()         # UTF-8 string codec
├── json<T>()        # JSON codec with types
├── buffer()         # Raw buffer codec
└── codec()          # Custom codec factory
```

## Next Steps

- [Getting Started](/flow/getting-started) - Setup and basic usage
- [KStream](/flow/streams) - Stream operations
- [KTable](/flow/tables) - Table operations
- [Windowing](/flow/windowing) - Time-based processing
- [State Stores](/flow/state-stores) - Stateful processing
