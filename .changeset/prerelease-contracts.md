---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

Breaking contract changes.

- All packages require Node.js 22 or later. Node.js 18 and 20 are past their end of life.
- `FlowConfig.runEach` is now `consumerRun` (flow consumes in batches, and the option takes `RunBatchOptions`).
