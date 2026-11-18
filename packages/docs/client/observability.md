# Observability

`@kafkats/client` reports what it does without depending on a telemetry library. It publishes events on Node's built-in [`diagnostics_channel`](https://nodejs.org/api/diagnostics_channel.html), and `@kafkats/opentelemetry` turns them into OpenTelemetry traces and metrics. Nothing is patched at runtime, and a client nobody listens to does no extra work.

## What the Client Measures

A Kafka deployment is usually already measured from the broker side, by `kafka_exporter` and the brokers' JMX metrics. The client does not repeat that. It reports what only it can see.

| From a broker exporter                       | From the client                                               |
| -------------------------------------------- | ------------------------------------------------------------- |
| Topic offsets, partitions, leaders, replicas | How long `send()` takes for the application, retries included |
| Consumer group lag (from committed offsets)  | Records per request: how well batching works                  |
| Group membership and state                   | Records and bytes waiting inside the producer                 |
| Bytes and messages in and out per topic      | How long handlers take, and their failures                    |
| Broker-side request latency                  | How long commits take, and their failures                     |
| Quotas and throttling                        | How long a rebalance stopped this member                      |
|                                              | Records fetched but not handled yet; paused partitions        |
|                                              | Trace context from the sender to the handler                  |

Alert on consumer group lag from the broker exporter: it covers every member of the group. The client's own `position.lag` shows how far one consumer's fetching is behind between commits.

## OpenTelemetry

```bash
pnpm add @kafkats/opentelemetry @opentelemetry/api
```

Call `instrumentKafka()` once, after setting up the OpenTelemetry SDK and before creating producers and consumers:

```typescript
import { instrumentKafka } from '@kafkats/opentelemetry'

instrumentKafka()
```

### Traces

- **`send <topic>`**: one span per `send()` call, a child of whatever span is active when you call it.
- **`process <topic>`**: one span per handler call. Your handler runs with it active, so spans created inside are its children.

The two are connected through message headers. On send, the trace context is written into the headers of each message in the format of your configured propagator; by default that is the W3C `traceparent` header. On consume it is read back:

- A `runEach` handler's span continues the trace of the send that produced the message.
- A `runBatch` handler's span starts a new trace and **links** to the sends of its messages, because a batch has no single origin.

A send or a message inside a trace that was not sampled gets no span. The sampling decision of the sender is followed, as OpenTelemetry's default sampler does, and it is passed on to whatever the handler sends.

A [flow](/flow/) application processes records in its tasks, one at a time, after its consumer handed them over in batches. Each record gets its own `process <topic>` span, which continues the trace of the send that produced the record. The processors run with it active, so the records the topology sends while processing it (outputs, repartition and changelog records) stay in that trace. Flow publishes these on its own tracing channel, `kafkats:flow:process` (exported as `flowDiagnostics` from `@kafkats/flow`); `instrumentKafka()` subscribes to it as well.

### Metrics

| Metric                                  | Type      | What it tells you                                            |
| --------------------------------------- | --------- | ------------------------------------------------------------ |
| `messaging.client.sent.messages`        | counter   | Messages sent, by topic and outcome                          |
| `messaging.client.operation.duration`   | histogram | How long `send()` and offset commits take                    |
| `messaging.client.consumed.messages`    | counter   | Messages handed to handlers, by topic, partition and group   |
| `messaging.process.duration`            | histogram | How long handlers take                                       |
| `kafkats.producer.request.duration`     | histogram | Round trip of a produce request, per broker                  |
| `kafkats.producer.request.records`      | histogram | Records per produce request                                  |
| `kafkats.producer.queue.records`        | gauge     | Records waiting to be sent                                   |
| `kafkats.producer.queue.bytes`          | gauge     | Bytes waiting to be sent                                     |
| `kafkats.producer.requests.in_flight`   | gauge     | Produce requests not answered yet                            |
| `kafkats.producer.sends.unacknowledged` | gauge     | `send()` calls not acknowledged yet                          |
| `kafkats.consumer.rebalance.duration`   | histogram | How long a rebalance stopped this member                     |
| `kafkats.consumer.position.lag`         | gauge     | How far this consumer's fetching is behind the partition end |
| `kafkats.consumer.buffered.bytes`       | gauge     | Fetched but not yet handled                                  |
| `kafkats.consumer.partitions.assigned`  | gauge     | Partitions this member owns                                  |
| `kafkats.consumer.partitions.paused`    | gauge     | Partitions this member has paused                            |

Durations are in seconds. Failed operations carry an `error.type` attribute. The `messaging.*` names follow OpenTelemetry's messaging semantic conventions, which are still marked as in development and may be renamed.

Gauges cover producers and consumers created after `instrumentKafka()` was called. Instances that share their attributes, such as two producers of one client, are added up into one series.

`kafkats.consumer.position.lag` is measured against the offset the consumer can read up to: the high watermark, or the last stable offset under `read_committed`. It is not reported for a paused partition, which is not fetched, so its end is not known.

### What It Costs

Nothing until `instrumentKafka()` is called. After that, each send and each handler call costs a few microseconds. Next to a handler that does real I/O that is small (about 1.5% for metrics and about 6% with traces, for a handler waiting 1 ms); next to a handler that does almost nothing it dominates. The package's README has the measurements and a benchmark to run on your own machine.

## Without OpenTelemetry

### Snapshots

```typescript
producer.metrics()
// { queuedBatches, queuedRecords, queuedBytes, inFlightRequests, unacknowledgedSends }

consumer.metrics()
// { partitions: [{ topic, partition, position, endOffset, bufferedBytes, paused }] }
```

Read them on whatever schedule your monitoring uses, for example in a Prometheus collector.

### Channels

Every operation is a [tracing channel](https://nodejs.org/api/diagnostics_channel.html#class-tracingchannel) exported as `diagnostics`:

| Channel                      | Operation                                | Context                                              |
| ---------------------------- | ---------------------------------------- | ---------------------------------------------------- |
| `kafkats:producer:send`      | A `send()` call until it is acknowledged | `topic`, `messages`, `transactional`, `result`       |
| `kafkats:producer:request`   | One produce request                      | `brokerId`, `records`, `bytes`, `rejected`           |
| `kafkats:consumer:process`   | One handler call                         | `groupId`, `topic`, `partition`, `messages`, `batch` |
| `kafkats:consumer:commit`    | One offset commit                        | `groupId`, `offsets`                                 |
| `kafkats:consumer:rebalance` | One rebalance of this member             | `groupId`, `assignment`                              |

```typescript
import { diagnostics } from '@kafkats/client'

diagnostics.process.subscribe({
	start() {},
	end() {},
	asyncStart() {},
	asyncEnd(context) {
		console.log(context.topic, context.messages.length, context.error ?? 'ok')
	},
	error() {},
})
```

`asyncEnd` is always the last event of an operation; a failed one has `context.error` set. A produce request the broker answered is not a failed operation even when it rejected partitions: those are listed in `context.rejected`.

Two things a subscriber can do besides observing, both in a `start` handler:

- **Add headers to messages being sent.** `context.addHeaders(headers)` on a send adds them to every message of the call; `context.addHeaders(headers, index)` to one. The objects the caller passed are not modified.
- **Run a handler in its own asynchronous context.** `context.runHandlerIn(scope)` on a `process` event makes the consumer call the handler through `scope`, for example `handler => storage.run(value, handler)` with an `AsyncLocalStorage`. Several subscribers can each ask; their scopes nest.

These two are what trace propagation needs, and they are how `@kafkats/opentelemetry` is built.
