---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Remove the consumer `maxRecords` option. `maxBytesPerPartition` now also bounds each delivered partition batch: prefetched Fetch responses buffered while a handler is busy are coalesced only up to that many (estimated) bytes, and the rest stays buffered for the next batch. A record larger than the limit is still delivered on its own. This also fixes out-of-order delivery within a partition: when `maxRecords` ended a batch inside a buffered response and another response for the same partition was buffered behind it, the remainder was moved behind that response and delivered after newer records. Default batches are no longer capped at 500 records, so lower `maxBytesPerPartition` if per-record handling is slow: a rebalance waits for in-flight batches. Priority strategies schedule in bytes: `SchedulerState.recordBudget` is now `byteBudget`, drain entries use `maxBytes` instead of `maxRecords`, and `SchedulerPartition.bufferedRecords` is removed.
