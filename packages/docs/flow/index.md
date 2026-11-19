# @kafkats/flow

A stream processing library for TypeScript built on Kafka Streams' processing model: the same DSL (`KStream`, `KTable`, `GlobalKTable`, windowed aggregations, joins, `suppress()`), the Processor API, tasks with their own state and stream time, and Kafka Streams' commit protocol for at-least-once and exactly-once processing. It runs on [`@kafkats/client`](/client/), which speaks the Kafka protocol directly.

## Features

- **Kafka Streams DSL**: `KStream`, `KTable`, `GlobalKTable`, grouping, `count` / `reduce` / `aggregate`, `split()`, `repartition()`, named operators
- **Windowing**: tumbling, hopping, session and sliding windows with grace periods, retention and emit-on-window-close
- **Joins**: windowed stream-stream (inner, left, outer), stream-table, table-table, and stream-global table joins on a key selector
- **`suppress()`**: final results of windows, or rate-limited table updates
- **Processor API**: `process()`, `processValues()`, `KTable.transformValues()`, typed state stores and punctuators
- **Exactly-once**: offsets, outputs and changelog records of every task in one transaction
- **Fault tolerance**: changelog-backed state per task, restoration, in-place recovery of corrupted tasks, rejoin after fencing or lost partitions, a `close()` that returns within its timeout
- **Exception handlers**: deserialization, processing, production and uncaught exceptions, as in Kafka Streams
- **Pluggable state**: in-memory, [LMDB](/flow-state-lmdb/) or [RocksDB](/flow-state-rocksdb/)
- **Testing**: a deterministic `TestDriver` that runs the production task runtime without Kafka

## Installation

```bash
pnpm add @kafkats/flow
```

Node.js 22 or later.

## Quick Example

```typescript
import { flow, topic, string, json, TimeWindows } from '@kafkats/flow'

interface Click {
	userId: string
	page: string
}

const clicks = topic('clicks', { key: string(), value: json<Click>() })

const app = flow({
	applicationId: 'click-counter',
	client: { clientId: 'click-counter', brokers: ['localhost:9092'] },
})

app.stream(clicks)
	.groupByKey()
	.windowedBy(TimeWindows.ofSizeAndGrace('5m', '30s'))
	.count()
	.toStream()
	.map((windowed, count) => [windowed!.key, { userId: windowed!.key, start: windowed!.window.start, count }] as const)
	.to('clicks-per-5m', { key: string(), value: json() })

await app.start()
```

## How an Application Runs

```
 DSL calls ──► topology ──► subtopologies ──► tasks (subtopology × partition)
                                                │
               one consumer, one producer ──────┤  records are routed to the task of their partition
                                                │
                     each task: its record queues, processors, state stores, stream time, offsets
```

- `flow()` creates an app; the DSL calls describe a **topology**. `start()` builds it (later DSL calls throw) and cuts it into **subtopologies**: parts connected by processors and shared stores, separated by topics.
- Each subtopology runs as one **task** per input partition. A task owns the processor instances, the state stores and the stream time of its partition. Tasks are the unit of assignment, state, commits and recovery, as in Kafka Streams.
- An instance has one group consumer (group id = `applicationId`) and one producer. Its tasks process concurrently on Node's event loop; the records of one task are processed one at a time, in timestamp order across its input partitions. Scale out by running more instances: the group spreads the partitions, and with them the tasks, across them.
- State stores are backed by **changelog topics** (unless disabled per store), from which a task's state is restored wherever the task runs next.

See [Topology and Naming](/flow/topology) for subtopologies, tasks and `describe()`, and [Fault Tolerance](/flow/fault-tolerance) for commits, rebalances and failures.

## Core Concepts

### KStream

An unbounded stream of key-value records; every record is an independent event.

```typescript
app.stream(orders)
	.filter((key, order) => order!.amount > 100)
	.mapValues(order => ({ ...order!, large: true }))
	.to('large-orders', { key: string(), value: json() })
```

### KTable

A changelog read as a table: the latest value per key, where a null value deletes the key.

```typescript
const users = app.table(usersTopic)
users.mapValues(user => user.email)
```

### GlobalKTable

A table every instance reads in full, used to enrich streams without co-partitioning:

```typescript
const products = app.globalTable(productsTopic)
app.stream(orders).join(
	products,
	(orderId, order) => order.productId, // which product row to look up
	(order, product) => ({ ...order, product })
)
```

## Next Steps

- [Getting Started](/flow/getting-started): setup, a first topology, start and close
- [Configuration](/flow/configuration): every option, its default, and the options that were removed
- [KStream](/flow/streams), [KTable](/flow/tables) and [Operations](/flow/operations)
- [Windowing](/flow/windowing), [Aggregations](/flow/aggregations), [Suppression](/flow/suppress) and [Joins](/flow/joins)
- [State Stores](/flow/state-stores) and the [Processor API](/flow/processor-api)
- [Fault Tolerance](/flow/fault-tolerance) and [Error Handling](/flow/error-handling)
- [Testing](/flow/testing)
- [Differences from Kafka Streams](/flow/kafka-streams) and [Upgrading to 0.14](/flow/upgrading)
