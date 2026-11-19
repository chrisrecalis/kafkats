# @kafkats/client

Pure-protocol TypeScript Kafka client - not a wrapper around existing libraries.

## Commands

```bash
pnpm build              # Build the package (@kafkats/flow and the other packages read the client from dist)
pnpm test:unit          # Run unit tests only
pnpm test:integration   # Run integration tests (requires Docker)
pnpm typecheck          # Type checking

# Integration tests against a running broker instead of testcontainers (SASL tests still need their own containers):
KAFKA_BROKERS=127.0.0.1:19092 VITEST_MAX_WORKERS=2 pnpm exec vitest run -c vitest.integration.config.ts [files]
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

### Contracts `@kafkats/flow` Relies On

Flow's task runtime is built on these consumer and producer behaviours; a change to one of them is a change to flow:

- `consumer.commit(offsets, { signal })` commits with metadata outside handlers (also with `commitOffsets: false`),
  pinned to the generation it started in: a commit still retried after a rejoin, or after a partition of it was
  revoked or lost, rejects with `CommitFailedError`. `consumer.committed()` reads offsets back with their metadata.
  Their `timeoutMs` bounds the whole call (requests in flight and coordinator lookups included), as does a signal;
  a rebalance fetches its committed offsets again after `defaultApiTimeoutMs`, like while the cluster is unreachable.
- `onPartitionsRevoked`, `onPartitionsAssigned` and `onPartitionsLost` are awaited inside the rebalance; assigned
  partitions are not fetched before `onPartitionsAssigned` returned; the lost hook runs synchronously with the loss.
  `rebalancePending` and its events tell flow to skip timer commits.
- `pause()` and `seek()` withhold records already fetched; `nextOffset()` and `currentLag()` are exact per partition
  (KIP-695 idling and KIP-1094 committed offsets depend on them).
- `autoOffsetReset` as a function chooses the reset policy per partition; under `'none'` a position out of range pauses
  the partition and emits `offsetOutOfRange` instead of ending the run; `'latest'` is the last stable offset under
  `read_committed`.
- `groupMetadata()` and `producer.beginTransaction()` (a `TransactionHandle` with `sendOffsets`, `commit`, `abort`)
  give fenced exactly-once commits; an unconfirmed `EndTxn` rejects with `TransactionAbortedError` and
  `outcome: 'unknown'`. `producer.disconnect({ timeoutMs })` and `consumer.stop()` are bounded. What a transaction
  registers (partitions, offsets) is its own: `commit()` waits for `sendOffsets()` calls still in progress (a failed
  one aborts it); once it aborts, registrations and sends not taken for sending yet reject with
  `TransactionAbortedError` (their records are dropped), a produce request already sent and its retries settle with
  the broker's answer, EndTxn waits for every produce request of the transaction (and is sent when a registration's
  answer was lost), and nothing of them reaches the next transaction. `abort()` on a handle that ended resolves only
  once it aborted (`InvalidTxnStateError` after a commit, also one of unknown outcome, or a failed abort). An
  initialization given up (`maxBlockMs`, `disconnect()`, the signal of the begin waiting for it) sends no
  InitProducerId after a coordinator lookup that answers late.
- A fenced transactional producer (PRODUCER_FENCED on a produce, PRODUCER_FENCED or INVALID_PRODUCER_EPOCH from a
  coordinator) is fatal and never sends InitProducerId again: the handle's calls, `abort()` included (also once the
  handle ended, and when the fence arrives during the abort), every later begin and a `transaction()` run on a lane
  of the pool (which begins nothing more once a lane is in the 'error' state) reject with `ProducerFencedError`. Any
  other failure of the epoch (out-of-order sequence, an outcome the retries left unknown, INVALID_PRODUCER_EPOCH on a
  produce, INVALID_PRODUCER_ID_MAPPING from the transaction coordinator) rejects the transaction's sends and leaves it
  abortable; `abort()` sends EndTxn (not for INVALID_PRODUCER_ID_MAPPING), then InitProducerId bumps the epoch, within
  `maxBlockMs` (not during `disconnect()`, also one called during the bump). Flow migrates on all of these and
  replaces the producer.
- `admin.deleteRecords()` purges repartition topics; `murmur2` / `toPositive` and `producer.partitionCount()` give a
  key's partition like the Java client.

Tests: `tests/unit/consumer/{consumer-commit,rebalance-hooks,rebalance-offset-fetch,group-metadata,session-lost}`,
`tests/unit/consumer/fetch-pause-undelivered`, `tests/unit/producer/producer-txn-handle`,
`tests/integration/consumer/{commit-api,rebalance-hooks,offset-reset}`, `tests/integration/eos/transaction-handle`.
User documentation: `packages/docs/client/*.md`.

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
