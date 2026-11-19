# Getting Started with @kafkats/flow

## Installation

```bash
pnpm add @kafkats/flow
```

`@kafkats/flow` depends on `@kafkats/client` and re-exports its codecs (`string()`, `json()`, `buffer()`, `codec`). Node.js 22 or later.

## Creating an Application

```typescript
import { flow } from '@kafkats/flow'

const app = flow({
	applicationId: 'my-stream-app',
	client: { clientId: 'my-stream-app', brokers: ['localhost:9092'] },
})
```

`applicationId` names everything the application owns: it is the consumer group id, the prefix of its internal topics (`<applicationId>-<name>-changelog`, `<applicationId>-<name>-repartition`) and of its state, and the base of its exactly-once transactional id. Instances of one application share it; two different applications must not.

`client` is a `KafkaClient` you created, or the config of one (`brokers`, `clientId`, `tls`, `sasl`, ...; see [client configuration](/client/configuration)). A client the app creates is connected by `start()` and disconnected by `close()`; a client you pass stays yours.

`flow()` validates the config at once and throws `FlowConfigError` for an invalid value or an option that was removed. Every option is listed in [Configuration](/flow/configuration).

## Defining Topics

`topic()` gives a topic name its codecs, so they do not have to be repeated:

```typescript
import { topic, string, json } from '@kafkats/flow'

interface PageView {
	userId: string
	page: string
}

const pageViews = topic('page-views', { key: string(), value: json<PageView>() })
```

A source without codecs reads raw `Buffer`s. Codecs are covered in [Codecs](/flow/codecs).

## Building a Topology

```typescript
// A stream: every record is an event
app.stream(pageViews)
	.filter((key, view) => view !== null && view.page !== '/health')
	.mapValues(view => ({ ...view!, seenAt: Date.now() }))
	.to('page-views-clean', { key: string(), value: json() })

// A table: the latest value per key
const users = app.table('users', { key: string(), value: json<{ name: string }>() })

// An aggregation: a table of counts per key, written to a topic as it changes
app.stream(pageViews).groupByKey().count().toStream().to('page-views-per-user')
```

Callbacks that receive the record, such as `filter()`'s predicate, take `(key, value)`. In a stream, keys and values may be `null` (a record without a key, a tombstone), so they see `K | null` and `V | null`. `mapValues()` and `flatMapValues()` take the value alone, as above (`mapValuesWithKey()` and `flatMapValuesWithKey()` take the key too), and joiners take the two values (see [Joins](/flow/joins)).

`to()` writes with the codecs of its options, else the ones the stream knows; `count()` results are int64 numbers, so `page-views-per-user` above gets string keys and 8-byte big-endian counts. See [Operations](/flow/operations#how-codecs-are-resolved).

## Starting and Stopping

```typescript
await app.start()
console.log(app.state()) // 'RUNNING'

process.on('SIGTERM', () => {
	void app.close() // commits what was processed, then releases everything
})
```

`start()` connects, creates the internal topics, loads the global tables, joins the group, and resolves once every task of the first assignment is restored and running. It rejects if anything fails before that, or if no assignment arrives within `startTimeoutMs` (default 120 s). A failed start releases what it started and can be retried.

`close({ timeoutMs })` (default 20 s) commits what was processed, closes the tasks and leaves the group. It returns within its timeout even when the cluster is unreachable; work that could not be committed is redone by the next owner of the partitions.

The topology is fixed by the first `start()` (or `describe()`): later DSL calls throw `TopologyError`.

### Application States

| State         | Meaning                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------ |
| `CREATED`     | Not started, or a start is in progress                                                           |
| `RUNNING`     | Every assigned task is restored and processing                                                   |
| `REBALANCING` | A rebalance, a restoration or a recovery after a failure is in progress                          |
| `ERROR`       | `start()` failed, or a fatal error stopped the app (see `app.getError()`); `close()` releases it |
| `STOPPED`     | Closed                                                                                           |

`state()` works as a readiness probe: it is `RUNNING` only while every assigned task is running. `getError()` returns the fatal error while the state is `ERROR`, otherwise the most recent recoverable error, which does not change the state. See [Fault Tolerance](/flow/fault-tolerance) and [Error Handling](/flow/error-handling).

## Processing Guarantees

```typescript
const app = flow({
	applicationId: 'payments',
	client: { clientId: 'payments', brokers: ['localhost:9092'] },
	processingGuarantee: 'exactly_once', // default 'at_least_once'
})
```

- **`at_least_once`** (default): nothing is lost; after a crash, the records since the last commit are processed again. Offsets are committed every `commitIntervalMs` (default 5000).
- **`exactly_once`**: the outputs, changelog records and input offsets of all tasks commit in one Kafka transaction every `commitIntervalMs` (default 100). Consumers of the output topics must read with `isolationLevel: 'read_committed'`.

See [Fault Tolerance](/flow/fault-tolerance#processing-guarantees) for how each one commits.

## Partitions and State

State is kept per task, that is per input partition. The topics feeding one stateful task (merged streams, both sides of a join, a stream and the table it looks up) must have the same partition count, otherwise `start()` throws `CoPartitioningError`. A stream whose keys changed (`selectKey()`, `map()`, `groupBy()`, ...) is repartitioned through an internal topic before a key-based operation. See [Repartitioning](/flow/streams#repartitioning) and [Topology and Naming](/flow/topology).

## Complete Example

```typescript
import { flow, topic, string, json, TimeWindows } from '@kafkats/flow'

interface PageView {
	userId: string
	page: string
}

const pageViews = topic('page-views', { key: string(), value: json<PageView>() })

const app = flow({
	applicationId: 'page-view-counter',
	client: { clientId: 'page-view-counter', brokers: ['localhost:9092'] },
})

app.stream(pageViews)
	// Re-keyed by page: the records go through the repartition topic page-view-counter-by-page-repartition
	.groupBy((_userId, view) => view!.page, { key: string(), name: 'by-page' })
	// Hourly windows that accept records up to 5 minutes late
	.windowedBy(TimeWindows.ofSizeAndGrace('1h', '5m'))
	// Only the final count of each window, once it closed
	.count({ storeName: 'views-per-page', emit: 'on-window-close' })
	.toStream()
	.map(
		(windowed, count) =>
			[
				windowed!.key,
				{ page: windowed!.key, windowStart: windowed!.window.start, windowEnd: windowed!.window.end, count },
			] as const
	)
	.to('page-view-counts', { key: string(), value: json() })

process.on('SIGTERM', () => void app.close())

await app.start()
```

## Next Steps

- [Configuration](/flow/configuration): every option and its default
- [KStream](/flow/streams) and [KTable](/flow/tables)
- [Windowing](/flow/windowing) and [Aggregations](/flow/aggregations)
- [Testing](/flow/testing): run a topology without Kafka
