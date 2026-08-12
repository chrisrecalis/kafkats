---
'@kafkats/client': patch
---

Fast polling for CONCURRENT_TRANSACTIONS. The transaction coordinator returns this error while the previous transaction's markers are still being written — an expected state that normally clears within milliseconds. It was previously treated as a normal retriable failure, so every back-to-back transaction paid the full `retryBackoffMs` (default 100ms) and consumed retry attempts. Transactional coordinator requests (AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn) now poll it at a 20ms interval (the Java client's ADD_PARTITIONS_RETRY_BACKOFF_MS) without consuming the retry budget, bounded by `maxBlockMs`. In the EOS consume-transform-produce loop this roughly cuts per-transaction latency from ~115ms to ~30ms.
