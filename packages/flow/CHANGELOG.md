# @kafkats/flow

## 0.8.0

### Minor Changes

- fe140c0: ShareConsumer: Kafka 4.2 share groups GA support (ShareFetch/ShareAcknowledge v2)
    - KIP-1206: new `acquireMode` config (`'batch_optimized'` default | `'record_limit'`) — `record_limit` strictly caps each fetch at `maxRecords`.
    - KIP-1222: new `message.renew()` to extend the acquisition lock without finalizing delivery; safe to call multiple times per message.
    - `ShareAcknowledgeResponse.acquisitionLockTimeoutMs` is now decoded on v2 responses.
    - AckManager dedupes same-offset RENEW + finalizing-ack pairs and collapses duplicate same-offset RENEWs so the wire never carries overlapping batches.
    - `@experimental` markers removed; share groups are GA in Kafka 4.2.
    - Backward compatible: against Kafka 4.1 brokers the client negotiates v1 and produces byte-identical wire output to before this change. `record_limit` and `renew()` throw with a clear "requires v2 (Kafka 4.2+)" error against older brokers.

### Patch Changes

- ca7d22f: Correctness and reliability fixes across the producer, consumer, client protocol, and flow state stores.

    ### Producer
    - `flush()` now waits for deferred drains so it no longer resolves while writes are still buffered.
    - `transactionalSend` freezes its partition set up front to avoid double-calling the partitioner.
    - Retry `TxnOffsetCommit` on retriable and rebalance-in-progress errors instead of failing the transaction.
    - Fence the producer on `OUT_OF_ORDER_SEQUENCE_NUMBER` to surface the idempotence violation rather than silently corrupting the stream.
    - Reject orphaned in-flight send promises during disconnect so callers are not left hanging.

    ### Consumer
    - Always reschedule the heartbeat after non-fatal errors, preventing the consumer from dropping out of the group.
    - Surface auto-commit errors to the consumer instead of swallowing them.
    - Clear only the successfully committed partitions on a partial commit failure so the rest are retried.
    - Fence stale fetch responses across partition replacement to avoid delivering records from a revoked assignment.

    ### Client / protocol
    - Guard against varint shift overflow in the decoder.
    - Close a `ConnectionPool.acquire` race that could hand out a connection mid-teardown.
    - Clamp the SASL reauthentication delay to the `setTimeout` maximum.
    - Don't await a response on `Produce` with `acks=0` (the broker sends none).
    - Always use the v0 response header for `ApiVersions`, matching broker behavior across versions.

    ### Share consumer
    - Release (rather than acknowledge) the last yielded message on shutdown so unprocessed records are redelivered.

    ### Flow
    - Write to the local store before the changelog (local-first ordering) to shrink the inconsistency window on crash.
    - Serialize the EOS commit triggered by a rebalance against the next message, and close the EOS rebalance offset-commit gap via an awaitable rebalance hook (shared with the consumer fix above).
    - Use stream time (not wall-clock time) for window expiry.
    - Implement hopping windows (`TimeWindows.advanceBy`).
    - Throw on `SlidingWindows` instead of silently misbehaving (not yet implemented).
    - Correct `TableGroupByNode` mapping ordering.
    - Apply retention to `reduce` and session aggregators, emitting changelog tombstones for expired state.
    - Surface checkpoint persistence errors instead of swallowing them.

    ### flow-state-lmdb
    - Make `range()` upper bound inclusive, matching the in-memory store.
    - Await `fsync` on checkpoint writes so a checkpoint is durable before it is reported complete.
    - Correct `WindowStore` time-bound math.
    - Use order-preserving signed-i64 encoding for time keys so range scans iterate in correct chronological order.

- Updated dependencies [ca7d22f]
- Updated dependencies [fe140c0]
    - @kafkats/client@0.8.0

## 0.7.0

### Minor Changes

- ec411b1: Fix flow integrity for KGroupedTable aggregations

    **Breaking:** `KGroupedTable.reduce()` now requires `(adder, subtractor)` and `aggregate()` requires `(initializer, adder, subtractor)` matching Kafka Streams semantics. This ensures correct retraction behavior when source table rows are updated or deleted.
    - Add delta retraction protocol for table groupBy: emits SUB/ADD headers so downstream aggregation nodes can properly subtract old values and add new values
    - Store both grouped key and source value in the mapping store (`GroupedTableMapping`) so retractions carry the correct old value
    - Add `TableDeltaCountNode`, `TableDeltaReduceNode`, `TableDeltaAggregateNode` for delta-aware aggregations
    - Reorder changelog write-before-local-store in all store wrappers to reduce inconsistency window on crash
    - Add `restrictRestorationToSourcePartitions` flag to prevent incorrect partition filtering during changelog restoration for re-keyed stores
    - Fix checkpoint offset in exactly_once mode to prevent skipping records on restoration

### Patch Changes

- Updated dependencies [ec411b1]
    - @kafkats/client@0.7.0

## 0.6.0

### Minor Changes

- 9c3180b: ### Consumer Refactor

    Simplified the consumer fetch architecture with a buffered poll model:
    - Removed callback-based fetch approach (~600 lines removed)
    - FetchManager now uses a background fetch loop with internal buffering
    - Added `StreamOptions` with manual partition assignment support for `consumer.stream()`

    ### Rebalance Improvements

    Fixed race conditions during cooperative rebalance:
    - Rebalance is now synchronous with the poll loop via `checkAndHandleRebalance()`
    - Added `PartitionTracker` to coordinate partition ownership and in-flight handler state
    - Partition assignment protection in `OffsetManager` prevents committing offsets for revoked partitions
    - Fixed cooperative rebalance losing tracked partitions when only newly added partitions are reported

    ### Bug Fixes
    - Fixed metadata refresh deduplication incorrectly caching topic-specific requests, which caused consumers sharing a `Cluster` instance to receive wrong metadata and get 0 partitions assigned

### Patch Changes

- Updated dependencies [9c3180b]
    - @kafkats/client@0.6.0

## 0.5.0

### Minor Changes

- 31136b9: ### ACL Support

    Added Access Control List (ACL) management to the admin client:
    - `admin.describeAcls()` - Query ACLs by filter criteria
    - `admin.createAcls()` - Create ACL bindings for principals
    - `admin.deleteAcls()` - Delete ACLs matching filters

    Includes full protocol support for DescribeAcls, CreateAcls, and DeleteAcls APIs (v2-v3).

    ### Consumer Seek

    Added `consumer.seek(topic, partition, offset)` method to reposition the fetch offset for a partition. Enables replaying messages or skipping ahead to a specific offset. Works with the pause/seek/resume pattern for controlled repositioning.

    ### Documentation

    Added comprehensive KafkaJS migration guide covering client configuration, producer, consumer, transactions, and admin API differences.

### Patch Changes

- Updated dependencies [31136b9]
    - @kafkats/client@0.5.0

## 0.4.0

### Minor Changes

- 34b2c63: ### Share Groups Support

    Added KIP-932 Share Groups implementation with `ShareConsumer` class for scalable, lock-free message consumption without partition assignment.

    ### Consumer Refactoring
    - Refactored consumer to use modular batch-based processing architecture
    - Simplified stream mode with poll-based API, removing separate stream-mode module
    - Fixed `sessionLost` flag not being cleared after successful rejoin

### Patch Changes

- Updated dependencies [34b2c63]
    - @kafkats/client@0.4.0

## 0.3.0

### Minor Changes

- d7a02fe: - fix(client): fix compression
    - docs: update compression documentation

### Patch Changes

- Updated dependencies [d7a02fe]
    - @kafkats/client@0.3.0

## 0.2.0

### Minor Changes

- 58c770f: - perf(client): optional native CRC32C via @node-rs/crc32 (now an optional peer dependency) with JS fallback; faster Record/RecordBatch encoding (more single-pass, fewer intermediate allocations); encoder uses Buffer.allocUnsafe; producer avoids per-message remapping. Also adds benchmark heap snapshot runners and updates docs.
    - chore(packages): add per-package README.md plus npm metadata (keywords, homepage, repository, bugs) for @kafkats/client, @kafkats/flow, @kafkats/codec-zod,
      @kafkats/flow-state-lmdb to improve npm package visibility.

### Patch Changes

- Updated dependencies [58c770f]
    - @kafkats/client@0.2.0
