# @kafkats/opentelemetry

OpenTelemetry traces and metrics for `@kafkats/client` and `@kafkats/flow`.

## Installation

```bash
npm install @kafkats/client @kafkats/opentelemetry @opentelemetry/api
```

## Usage

Call `instrumentKafka()` once, after setting up the OpenTelemetry SDK and before creating producers and consumers:

```typescript
import { instrumentKafka } from '@kafkats/opentelemetry'

instrumentKafka()
```

Nothing in the client is patched: the client publishes what it does on `node:diagnostics_channel`, and this package listens.

## What You Get

**Traces.** A `send <topic>` span for every `send()` call and a `process <topic>` span for every handler call. The trace context travels in the message headers (W3C `traceparent` unless you configured another propagator), so the handler's span continues the trace of the request that sent the message. A batch handler's span links to the sends of its messages. The handler runs with its span active, so spans it creates are children.

**Flow applications.** A `@kafkats/flow` application (0.14 or later) gets a `process <topic>` span for every record one of its tasks processes, continuing the trace of the send that produced the record. The processors run with it active, so the records the topology sends (outputs, repartition and changelog records) stay in that trace, and so do spans your processors create. The span carries the task (`kafkats.flow.task.id`) and the application id as the consumer group. The application's consumer still reports a `process` span per fetched batch: that is the handler that hands the records to the tasks.

**Metrics.** Only what a broker cannot see. Consumer group lag, topic offsets and broker request rates come from a broker exporter such as `kafka_exporter`.

| Metric                                  | What it tells you                                             |
| --------------------------------------- | ------------------------------------------------------------- |
| `messaging.client.sent.messages`        | Messages sent, by topic and outcome                           |
| `messaging.client.operation.duration`   | How long `send()` and offset commits take for the application |
| `messaging.client.consumed.messages`    | Messages handed to handlers, by topic, partition and group    |
| `messaging.process.duration`            | How long handlers take                                        |
| `kafkats.producer.request.duration`     | Round trip of a produce request, per broker                   |
| `kafkats.producer.request.records`      | Records per produce request: how well batching works          |
| `kafkats.producer.queue.records/bytes`  | Waiting to be sent: backpressure                              |
| `kafkats.producer.requests.in_flight`   | Requests not answered yet                                     |
| `kafkats.producer.sends.unacknowledged` | `send()` calls not acknowledged yet                           |
| `kafkats.consumer.rebalance.duration`   | How long a rebalance stopped this member                      |
| `kafkats.consumer.position.lag`         | How far this consumer's fetching is behind what it can read   |
| `kafkats.consumer.buffered.bytes`       | Fetched but not yet handled                                   |
| `kafkats.consumer.partitions.assigned`  | Partitions this member owns                                   |
| `kafkats.consumer.partitions.paused`    | Partitions this member has paused                             |

The `messaging.*` names follow OpenTelemetry's messaging semantic conventions, which are still in development and may be renamed.

## What It Costs

Nothing while `instrumentKafka()` has not been called: the client then publishes nothing.

Once it is on, every send and every handler call costs a few microseconds, measured on Node.js 22 with spans and metrics exported to nothing:

| The handler                       | Metrics only | Traces and metrics | Same, 1% of traces sampled |
| --------------------------------- | ------------ | ------------------ | -------------------------- |
| does nothing (the worst case)     | about −70%   | about −88%         | about −83%                 |
| parses 1 KB of JSON               | about −20%   | about −48%         | about −38%                 |
| waits 1 ms (a fast database call) | about −1.5%  | about −6%          | about −5%                  |

Throughput of `runEach`, relative to no instrumentation. What you pay depends on what your handler does: next to real I/O it is small, next to an empty handler it dominates. `pnpm bench:client` in this package measures it for your machine.

A flow application pays per record, for the record's `process` span and for the send of each record it produces. A topology that does almost nothing (the `stateless` scenario of `pnpm bench:flow`, at-least-once) runs about 70% slower with every record traced, of which about 20 points are the `process` spans of the records, and about 55% slower with 1% of traces sampled and metrics on. A flow application nothing listens to pays nothing.

A message that belongs to a trace that was not sampled gets no span: the decision of the sender is followed, as OpenTelemetry's default sampler does, and it is passed on to whatever the handler sends.

## Options

```typescript
const stop = instrumentKafka({
	tracerProvider, // default: the global tracer provider
	meterProvider, // default: the global meter provider
	traces: true, // false: no spans, no trace headers
	metrics: true, // false: no metrics
})

stop() // stop listening
```

## License

MIT
