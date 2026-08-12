---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Concurrent `producer.transaction()` calls now queue instead of throwing `InvalidTxnStateError`. A Kafka producer can only have one open transaction at a time (protocol constraint); previously, sharing a transactional producer across consumer handlers running with `partitionConcurrency > 1` threw the moment two batches finished together. Transactions now wait for capacity in FIFO order, making the consume-transform-produce pattern safe with a single producer.

- Nested `transaction()` calls from inside the same producer's transaction callback throw immediately instead of deadlocking.
- New `transaction:queued` producer event reports queue depth for observing commit-bound throughput.
- `disconnect()` now also refuses to disconnect while transactions are queued (previously only while one was active).
- The transaction timeout starts when the transaction begins, not while it waits in the queue.
