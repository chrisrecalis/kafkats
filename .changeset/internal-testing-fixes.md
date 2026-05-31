---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Various fixes from internal testing:

- Surface internal connection and coordinator errors without crashing the process.
- Time-bound coordinator retries for transactional producer init and consumer offset fetch/commit. Configurable via the new producer `maxBlockMs` and consumer `defaultApiTimeoutMs` options (both default 60000).
- Re-discover the group coordinator on `NotCoordinator` during offset commit.
- Add human-readable messages for `ProducerFenced` and `InvalidProducerEpoch`.
