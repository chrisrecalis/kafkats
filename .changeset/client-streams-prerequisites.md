---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

`@kafkats/client` gains the consumer, producer and admin APIs a Kafka Streams-style runtime builds on: explicit offset commits, awaited rebalance hooks, transaction handles and record deletion. Existing options keep their defaults.

**Consumer**

- `consumer.commit(offsets, { timeoutMs, signal })` commits offsets of assigned partitions with optional metadata, from anywhere (also with `commitOffsets: false` and in `onPartitionsRevoked`). Coordinator moves and connection failures are retried within `timeoutMs`; it rejects with `RebalanceInProgressError`, `CommitFailedError` (the member's generation is gone, and the consumer rejoins) or `TimeoutError`. A commit still being retried when the consumer rejoined, or when a partition of it was revoked or lost, rejects with `CommitFailedError` instead of committing offsets meant for the old assignment.
- `consumer.committed(partitions)` reads committed offsets back with their metadata and leader epoch.
- `consumer.groupMetadata()` returns the group metadata of the last completed join, for fenced transactional offset commits; `undefined` in manual mode, before the first join and after a lost session until the next join.
- Rebalance hooks `onPartitionsRevoked`, `onPartitionsAssigned` and `onPartitionsLost`, awaited inside the rebalance. Assigned partitions are not fetched before `onPartitionsAssigned` returned, so `pause()` and `seek()` there apply before the first record; the lost hook is invoked synchronously with the loss and awaited before the rejoin; partitions lost while a hook runs are reported lost only, not revoked or assigned afterwards. A hook that throws fails the run.
- `consumer.rebalancePending` and the `rebalancePending` / `rebalanceComplete` events. `partitionsAssigned` and `onPartitionsAssigned` receive `AssignedPartition`s, with the start offset and the committed offset of each partition.
- `consumer.nextOffset(topic, partition)` (the first offset of the earliest batch not delivered yet; delivery counts whole batches) and `consumer.currentLag(topic, partition)`; `metrics()` adds `nextOffset` and `bufferedRecords` per partition.
- `autoOffsetReset` accepts a function choosing the policy per partition. Resets emit `offsetReset`. Under `'none'`, a partition without a committed offset fails the run with `NoOffsetForPartitionError`, and a position out of range pauses the partition and emits `offsetOutOfRange` instead of ending the run.
- `pause()` and `seek()` withhold records already fetched: once they return, no record of the partition from before is delivered (except to a handler of it that is already running).
- `'latest'` resolves to the last stable offset under `read_committed`.
- Partitions lost with the session are no longer reported as revoked as well. `runBatch` no longer marks the whole batch of a handler that returns after the run was stopped. A record consumed while an automatic commit was in flight is committed by the next one instead of being dropped from the pending offsets.

**Producer**

- `producer.beginTransaction({ signal })` returns a `TransactionHandle` (`send`, `sendOffsets`, `commit()`, `abort()`, both with `{ timeoutMs, signal }`), taking turns first in first out with `transaction()` calls, which now run on top of it unchanged. Its signal aborts the open transaction until `commit()` or `abort()` is called, which are bounded by their own options.
- A commit whose EndTxn was sent but never confirmed rejects with `TransactionAbortedError` with `outcome: 'unknown'` (`'aborted'` otherwise) and puts the producer in the `'error'` state; `producer.transactionState` exposes the state. Transactional coordinator requests retry connection failures.
- `sendOffsets()` with the group metadata of a member that has not joined (negative generation, empty member id) rejects with `CommitFailedError` before sending. Of equal offsets sent for a partition, the last one's metadata is committed.
- Definitive produce rejections reject with `KafkaProtocolError` carrying the broker's error code (e.g. `MessageTooLarge`); exhausted retries with a retriable `KafkaError` whose `cause` is the last failure.
- `producer.disconnect({ timeoutMs })` is bounded (default 5 s), aborts a transaction a handle left open and is allowed in the `'error'` state.
- `producer.partitionCount(topic, { timeoutMs, signal })`, and `murmur2` / `toPositive` exported for computing a key's partition like the Java client.
- Partition counts include partitions that have no leader at the time, so keyed records are no longer sent to the wrong partition during a leader election. A partition without a leader no longer holds back the batches of the other partitions: only its own batch is retried.

**Admin**

- `admin.deleteRecords(records, { timeoutMs, signal })` moves partitions' log start offsets forward (DeleteRecords API), retrying moved leaders and returning failures per partition.

**Exports**

- The package root now exports `CommitFailedError`, `OffsetOutOfRangeError`, `NoOffsetForPartitionError`, `ProducerFencedError`, `InvalidTxnStateError`, `TransactionAbortedError`, and the types of the APIs above (`CommitOffset`, `CommittedOffset`, `AssignedPartition`, `OffsetResetPolicy`, `OffsetResetEvent`, `ConsumerEvents`, `TransactionHandle`, `ProducerTransaction`, `TransactionState`, `TransactionOffset` (the offsets `sendOffsets()` takes, with metadata), `DeleteRecordsResult`, ...).
