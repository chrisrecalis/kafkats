---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

Contract changes before the next release.

**Breaking**

- All packages require Node.js 22 or later. Node.js 18 and 20 are past their end of life.
- `FlowConfig.runEach` is now `consumerRun` (flow consumes in batches, and the option takes `RunBatchOptions`).

**Changes to APIs that have not been released yet**

- `consumer.commitPosition()` is now `commitStartPosition()`, and throws once records of the partition have been fetched: committing a position past records no handler has seen would skip them.
- The building blocks for state store providers (`OrderedByteStore`, `OrderedKeyValueStore`, `OrderedWindowStore`, `OrderedSessionStore`, `OrderedChangelogCheckpointStore`) are exported from `@kafkats/flow/state` instead of `@kafkats/flow`. `writeSignedTime` and `readSignedTime` are no longer exported.
- `rocksdb()` takes `writeBufferSize`, `compression` and `maxOpenFiles` directly instead of passing the binding's own options through.
