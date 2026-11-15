# @kafkats/opentelemetry

OpenTelemetry traces and metrics for `@kafkats/client`.

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
