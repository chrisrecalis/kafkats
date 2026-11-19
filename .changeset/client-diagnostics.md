---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

Observability for `@kafkats/client`, without a telemetry dependency in the client.

- The client publishes what it does on `node:diagnostics_channel` (exported as `diagnostics`): tracing channels for `send()`, produce requests, handler calls, offset commits and rebalances. A subscriber can add headers to messages being sent and run a handler in its own asynchronous context. Nothing is built or published without subscribers.
- `producer.metrics()` and `consumer.metrics()` return what is waiting inside: queued records and bytes, requests in flight, and per partition the fetch position, the offset it can read up to, buffered bytes and pause state.
- New package `@kafkats/opentelemetry`: `instrumentKafka()` turns those diagnostics into `send` and `process` spans connected through W3C trace context in message headers, and into metrics following OpenTelemetry's messaging conventions. It also subscribes to `@kafkats/flow`'s `kafkats:flow:process` channel: a `process <topic>` span per record a flow task processes, continuing the trace of the send that produced it, with the records the topology sends meanwhile as its children.
- Only what a broker cannot see is reported. Consumer group lag, topic offsets and broker request rates remain the job of broker exporters.
