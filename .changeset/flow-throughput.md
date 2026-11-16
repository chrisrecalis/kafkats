---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

Raise `@kafkats/flow` throughput by roughly an order of magnitude without changing its guarantees.

**Flow**

- Output and changelog records are no longer acknowledged one by one. A worker sends them as it processes and waits for all acknowledgements before it commits: at least every `commitIntervalMs` and at the end of each consumed batch under at-least-once, per transaction under exactly-once. A failed send fails the worker without committing, as before.
- Under at-least-once an output record can now be written before the changelog record of the same input is acknowledged (and is written again if that changelog write fails). Offsets are still only committed after both are acknowledged.
- A partition read from `'latest'` without a committed offset now has its start position committed, so a restart before the first processed batch no longer skips records.
- Restoration applies the changelog batch by batch, and an exactly-once commit applies its buffered store writes together. With LMDB both were one disk transaction per record.
- In-memory window and session stores index entries by key; `fetch()` and `findSessions()` return windows in time order, like the LMDB stores.
- `MockConsumer` in `@kafkats/flow/testing` implements `runBatch`.

**Client**

- Producer: batches of one partition that queue up behind an in-flight request are merged (in order, up to `maxBatchBytes`).
- Producer: records are appended in the order `send()` was called, also for sends that are not awaited one by one.
- Consumer: every consumer fetches on its own connections. Consumers sharing a `KafkaClient` no longer wait for each other's long polls.
- Consumer: `runBatch` handlers get `ctx.resolveOffset(offset)` to make progress within a batch committable before the handler returns.
- Consumer: new `commitStartPosition(partitions)` commits the position partitions start reading from, before any of their records has been fetched, and `position(topic, partition)` returns the current position.
