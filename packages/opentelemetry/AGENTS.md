# @kafkats/opentelemetry

Turns the diagnostics `@kafkats/client` publishes into OpenTelemetry spans and metrics.

## Commands

```bash
pnpm build      # Build the package
pnpm test       # Run tests (the end-to-end test needs KAFKA_BROKERS, otherwise it is skipped)
pnpm typecheck  # Type checking
```

## Design

- **The client is not patched and has no telemetry dependency.** It publishes on `node:diagnostics_channel` (`packages/client/src/diagnostics.ts`); this package subscribes. `@opentelemetry/api` is a peer dependency of this package only.
- **Only what a broker cannot see.** Group lag, topic offsets and broker request rates belong to broker exporters (`kafka_exporter`, JMX). A metric goes in here if it describes the inside of the client or the application's handlers.
- `src/operations.ts`: follows one operation of a tracing channel from start to finish. Both tracing and metrics are built on it.
- `src/tracing.ts`: `send` and `process` spans; writes and reads trace context in message headers through the configured propagator; runs the handler with its span active (`ProcessDiagnostics.runHandlerIn`).
- `src/metrics.ts`: counters and histograms from operations; gauges read from `metrics()` of every producer and consumer created after `instrumentKafka()` (held weakly; instances with the same attributes are summed into one series).
- `src/attributes.ts`: names from OpenTelemetry's messaging semantic conventions. They are "in development" upstream and have been renamed before, so they are kept in one place and out of the client.

## Tests

`tests/tracing.test.ts` and `tests/metrics.test.ts` drive the client's channels directly, with in-memory OpenTelemetry exporters. `tests/end-to-end.test.ts` uses a real producer and consumer; `pnpm test:integration` runs it with a broker from testcontainers, which is what CI does.

## What Instrumentation Costs

`bench/` measures throughput with OpenTelemetry set up by `OTEL_MODE` (`off`, `metrics`, `traces`, `both`, `sampled` for 1% of traces), exporting to nothing:

```bash
OTEL_MODE=both KAFKA_BROKERS=localhost:9092 pnpm bench:client   # send() per message, then runEach
OTEL_MODE=both KAFKA_BROKERS=localhost:9092 pnpm bench:flow -- --scenario count
```

Counters are summed per series in `src/metrics.ts` and handed to the SDK when it collects, because the SDK hashes the attributes of every measurement. Histograms cannot be handed over that way, so each operation still costs one SDK histogram update.
