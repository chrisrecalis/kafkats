# Core Concepts

Understanding these core concepts will help you use kafkats effectively.

## Topics, Partitions, and Ordering

A **topic** is a named log of records. Topics are divided into **partitions**, which are ordered, immutable sequences of records.

```typescript
// Produce records to a topic
await producer.send('orders', [{ key: 'order-123', value: orderJson }])
```

Key points:

- **Ordering is per partition** (not per topic).
- **Parallelism comes from partitions** (more partitions → more consumers can work in parallel).

| Concept   | What it means in practice                       |
| --------- | ----------------------------------------------- |
| Topic     | A named log of records (often one “event type”) |
| Partition | The unit of ordering and parallelism            |
| Record    | A key/value payload plus headers and timestamp  |

## Keys and Partitioning

Message **keys** determine which partition a record goes to. Records with the same key always go to the same partition, which preserves order for that key.

```typescript
await producer.send('events', [
	{ key: 'user-1', value: event1 },
	{ key: 'user-1', value: event2 }, // same partition as above
	{ key: 'user-2', value: event3 }, // potentially different partition
])
```

## Brokers, Leaders, and Replication

A Kafka **cluster** is made of **brokers**. Each partition has a **leader** broker and zero or more replica brokers. The leader handles reads/writes; replicas follow the leader.

This matters when producing:

| Producer `acks` | Meaning                              | Typical use                         |
| --------------- | ------------------------------------ | ----------------------------------- |
| `'none'`        | Don’t wait for broker acknowledgment | Fire-and-forget logs (risk of loss) |
| `'leader'`      | Wait for the leader to write         | Lower latency, less durable         |
| `'all'`         | Wait for all in-sync replicas        | Most durable (recommended default)  |

## Producer Batching and Queueing

In kafkats, `producer.send()` is **queue-based**: records are appended to an in-memory accumulator and flushed as partition batches.

| Setting         | What it controls                                                        |
| --------------- | ----------------------------------------------------------------------- |
| `lingerMs`      | Time-based batching: how long to wait before flushing a partition batch |
| `maxBatchBytes` | Size-based batching: flush when the batch reaches this size             |
| `compression`   | Compression applied to record batches                                   |

See [Producer API](/client/producer) for details.

## Consumer Groups and Rebalances

**Consumer groups** allow multiple consumers to share the work of processing a topic. Each partition is assigned to exactly one consumer in the group at a time.

When consumers join/leave, Kafka performs a **rebalance** to reassign partitions.

| Assignment strategy    | Description                                                        |
| ---------------------- | ------------------------------------------------------------------ |
| `'cooperative-sticky'` | Incremental rebalancing (Kafka 2.4+), minimizes movement (default) |
| `'sticky'`             | Eager rebalance with minimized movement                            |
| `'range'`              | Simple per-topic assignment                                        |

## Offsets and Offset Resets

An **offset** is a monotonically increasing position within a partition. Kafka stores committed offsets (per group) in `__consumer_offsets`.

When a group has no committed offset (new group, offsets expired), `autoOffsetReset` decides what to do:

| Value        | Behavior                                 |
| ------------ | ---------------------------------------- |
| `'earliest'` | Start from the earliest available offset |
| `'latest'`   | Start from the end (new records only)    |
| `'none'`     | Fail if no committed offset exists       |

## Delivery Semantics

Kafka’s durability is a property of the log, but what your application observes depends on how you produce, consume, and commit offsets.

| Semantics     | What you get                               | Typical approach                                              |
| ------------- | ------------------------------------------ | ------------------------------------------------------------- |
| At-most-once  | No duplicates, possible loss               | Commit before processing (rare)                               |
| At-least-once | No loss, possible duplicates               | Process → commit (common default)                             |
| Exactly-once  | No loss, no duplicates (within a topology) | Transactions + `read_committed` (use Flow for end-to-end EOS) |

## Codecs and Typed Topics

Use codecs to get type-safe key/value encode/decode on both producer and consumer.

```typescript
import { topic, string, json } from '@kafkats/client'

const userTopic = topic('users', {
	key: string(),
	value: json<{ id: string; name: string }>(),
})

await producer.send(userTopic, [{ key: 'user-1', value: { id: 'user-1', name: 'Alice' } }])
```

## Transactions and Isolation

Transactions let you write to multiple partitions/topics atomically.

```typescript
const producer = client.producer({
	transactionalId: 'my-transaction',
	acks: 'all',
})

await producer.transaction(async txn => {
	await txn.send('output', [{ value: 'processed' }])
})
```

Consumers can control whether they see uncommitted transactional data:

| `isolationLevel`     | What you see                                       |
| -------------------- | -------------------------------------------------- |
| `'read_committed'`   | Only committed transactional records (recommended) |
| `'read_uncommitted'` | All records, including uncommitted                 |

## Stream Processing Concepts

### KStream

A **KStream** represents an unbounded stream of records. Each record is an independent event.

```typescript
import { flow } from '@kafkats/flow'

const app = flow({ applicationId: 'my-app', ... })
app.stream('events')
  .filter((key, value) => value.type === 'click')
  .mapValues(value => ({ ...value, processed: true }))
  .to('processed-events')
```

### KTable

A **KTable** represents a changelog stream, where each key has a latest value. It's like a continuously-updated table.

```typescript
app.table('users') // Latest value for each user ID
	.mapValues(user => user.name)
	.to('user-names')
```

### Windowing

**Windowing** groups stream records by time for aggregations.

```typescript
import { TimeWindows } from '@kafkats/flow'

app.stream('clicks')
	.groupByKey()
	.windowedBy(TimeWindows.of('5m')) // 5-minute windows
	.count()
```

## Next Steps

- [Producer API](/client/producer) - Sending messages
- [Consumer API](/client/consumer) - Receiving messages
- [Flow Streams](/flow/streams) - Stream processing
