# @kafkats/client

Pure-protocol TypeScript Kafka client - not a wrapper around existing libraries.

## Commands

```bash
pnpm build              # Build the package
pnpm test:unit          # Run unit tests only
pnpm test:integration   # Run integration tests (requires Docker)
pnpm typecheck          # Type checking
```

## Architecture

```
src/
├── network/      # TCP/TLS connections, pooling, request queuing
├── protocol/     # Binary encoding/decoding, Kafka wire protocol
├── auth/         # SASL mechanisms (PLAIN, SCRAM-SHA-256/512)
├── client/       # Broker connections, cluster topology, metadata
├── producer/     # Batching, partitioners, RecordAccumulator
├── consumer/     # Consumer groups, FetchManager, partition assignors
├── diagnostics.ts # What the client publishes about its work on node:diagnostics_channel
└── index.ts      # Public API exports
```

### Public API

`package.json` has only the `"."` export, so a type, function or class is public only once `client/index.ts` (re-exported by `index.ts`) exports it. Add a new public name there and to its module's `index.ts` (`consumer/`, `producer/`, `admin/`); `tests/unit/exports.test.ts` checks the root types of the consumer, producer and admin APIs.

### Diagnostics

The client depends on no telemetry library. `src/diagnostics.ts` defines tracing channels (send, produce request, handler call, offset commit, rebalance) and `Producer.metrics()` / `Consumer.metrics()` return what is waiting inside. Rules for adding to it:

- Publish only what a broker cannot see (application-side latency, handlers, backlog inside the client). Group lag, topic offsets and broker rates belong to broker exporters.
- Go through `trace()`: it builds no context and publishes nothing when a channel has no subscribers.
- Names of telemetry standards (OpenTelemetry attributes and metrics) do not belong here; they live in `@kafkats/opentelemetry`.

### Key Classes

- **KafkaClient** (`client/kafka-client.ts`): Entry point, creates producers/consumers
- **Cluster** (`client/cluster.ts`): Broker connections and metadata discovery
- **Broker** (`client/broker.ts`): Typed protocol operations
- **Connection** (`network/connection.ts`): TCP connection with request correlation
- **Encoder/Decoder** (`protocol/primitives/`): Binary serialization

## Testing

Integration tests use a global Kafka setup via testcontainers (Docker required). A single Kafka container is started before all tests and shared across test files.

```typescript
import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

const client = createClient('test-id')
await client.connect()

const topic = uniqueName('my-topic')
// ... test logic ...

await client.disconnect()
```

For SASL tests, use `withKafkaSasl` from `helpers/kafka-sasl.js` (starts separate containers per auth mechanism).

## Performance Considerations

- RecordBatch encoding is synchronous and optimized for throughput
- Use `Encoder.sizeOf*` methods to pre-calculate buffer sizes
- Decoder uses zero-copy views where possible
