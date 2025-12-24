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
└── index.ts      # Public API exports
```

### Key Classes

- **KafkaClient** (`client/kafka-client.ts`): Entry point, creates producers/consumers
- **Cluster** (`client/cluster.ts`): Broker connections and metadata discovery
- **Broker** (`client/broker.ts`): Typed protocol operations
- **Connection** (`network/connection.ts`): TCP connection with request correlation
- **Encoder/Decoder** (`protocol/primitives/`): Binary serialization

## Testing

Integration tests use testcontainers - each test gets its own Kafka container.

```typescript
import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

await withKafka(async ({ createClient }) => {
	const topic = uniqueName('my-topic')
	const client = createClient('test-id')
	// ...
})
```

For SASL tests, use `withKafkaSasl` from `helpers/kafka-sasl.js`.

## Performance Considerations

- RecordBatch encoding is synchronous and optimized for throughput
- Use `Encoder.sizeOf*` methods to pre-calculate buffer sizes
- Decoder uses zero-copy views where possible
