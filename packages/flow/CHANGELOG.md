# @kafkats/flow

## 0.14.0

### Minor Changes

- 7ae72e9: Observability for `@kafkats/client`, without a telemetry dependency in the client.

  - The client publishes what it does on `node:diagnostics_channel` (exported as `diagnostics`): tracing channels for `send()`, produce requests, handler calls, offset commits and rebalances. A subscriber can add headers to messages being sent and run a handler in its own asynchronous context. Nothing is built or published without subscribers.
  - `producer.metrics()` and `consumer.metrics()` return what is waiting inside: queued records and bytes, requests in flight, and per partition the fetch position, the offset it can read up to, buffered bytes and pause state.
  - New package `@kafkats/opentelemetry`: `instrumentKafka()` turns those diagnostics into `send` and `process` spans connected through W3C trace context in message headers, and into metrics following OpenTelemetry's messaging conventions.
  - Only what a broker cannot see is reported. Consumer group lag, topic offsets and broker request rates remain the job of broker exporters.

- 754dfb6: Raise `@kafkats/flow` throughput by roughly an order of magnitude without changing its guarantees.

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

- ff86159: Redesign the `@kafkats/flow` runtime for correctness: partitioned state, repartitioning, a real `globalTable()`, transactional state under exactly-once, and workers that never continue past a failure.

  **Breaking and behaviour changes**

  - State is partitioned: each source partition has its own slice of every state store, logged to the changelog partition with the same index and restored from it. Provider-level store names are now `<applicationId>.<storeName>.p<partition>`, and the default LMDB `maxDbs` is 4096.
  - Inputs to a stateful operation must be co-partitioned. All topics feeding one stateful operation (merged streams, both sides of a join, the stream and table of a join) need the same partition count, otherwise `start()` throws the new `CoPartitioningError`. Records with the same key must be in the same partition index.
  - `groupBy(fn, { key, value, name? })`, and `selectKey()`/`map()` followed by `groupByKey()`, a join or `toTable()`, now write through an internal topic `<applicationId>-<name | repartition-N>-repartition` and read it back. The topic is created at startup (it must exist with `changelog.autoCreate: false`), and key and value codecs are required. Pass `name` for a stable topic name.
  - `through(topic)` now continues from what is read back from the topic instead of passing records straight on. The topic must exist. Internal and `through()` topics are read from the beginning when the group has no committed offset.
  - `globalTable()` is read in full by every instance outside the consumer group and loaded before processing starts. It writes no changelog and can only be the lookup side of a stream join.
  - Consumers always use the `range` partition assignor. Stop all instances before upgrading from an earlier version (no rolling upgrade). Windowed and session state and join buffers written by earlier versions were not partition-aligned and restart from empty; key-value state is rebuilt from the changelog.
  - Reusing a store name for a different kind of store (key-value, window, session) now throws when the topology is built instead of silently returning the wrong store.
  - A restore that makes no progress for `changelog.restoration.idleTimeoutMs` (now 30000 by default) fails instead of completing with partial state.

  **Fixes**

  - Exactly-once: a failed transaction commit no longer loses records. The worker stops, aborts and resumes from the last committed offsets.
  - Exactly-once: uncommitted state updates no longer survive an abort or crash, including in persistent LMDB stores. Store writes are buffered and applied only after the commit.
  - Restoration no longer completes early with partial state, and an unusable checkpoint wipes and rebuilds the store. It reads to the end of the changelog and waits out a crashed instance's open transaction instead of stopping at an older position (which left exactly-once state behind the committed offsets).
  - State is no longer lost or mixed across instances for windowed stores, joins and non-default partitioning.
  - `globalTable()` is now really global.
  - `KTable.groupBy()` aggregations are repartitioned by the grouped key and are now correct with several instances or threads; an emptied group is removed in both processing modes.
  - Stream time is tracked per partition, stream-stream joins keep records with the same key and timestamp, `KTable.filter()` emits a tombstone when a row stops matching, `filter()`/`mapValues()` on tables never call your function for tombstones, session `reduce()` merges sessions in time order with `(aggregate, value)` argument order, `aggregate()` on session windows uses real sessions (and requires a `merger` option) instead of silently running as fixed windows, `to()`/`through()` keep the input record's timestamp, and tables from `count()`/`reduce()`/`aggregate()` can be joined.
  - Exactly-once: a transaction aborted by the broker after `transactionTimeoutMs` (stalled broker, frozen process) no longer stops the worker for good. It restarts with a fresh producer and reprocesses from the committed offsets.
  - A worker that restarts or gets a partition back before its first commit no longer re-resolves `'latest'` and skips the records it had read; the start position is kept until the first commit.
  - Stream time survives restarts and rebalances: it is rebuilt from the restored window or session store, so a late record that was dropped as past retention is not accepted after a restart.
  - Applications sharing one state store provider (for example one LMDB `stateDir`) no longer overwrite each other's state; provider-level store names include the application id.
  - Exactly-once: the commit before a rebalance now also covers a record whose processing had started but had not opened its transaction yet, and the cleanup after a revoke no longer drops store writes buffered by a transaction that is still open. Together these let a transaction commit in Kafka without its update reaching the local store, so a count was emitted twice under rebalance churn.
  - Exactly-once: a partition that is revoked and assigned again while an earlier restore is still running stays paused until its own restore has finished. Resuming it early let a committed update be dropped from the local store.
  - `@kafkats/client`: consumers and producers recover after a broker restart or a dropped connection. Brokers re-dial dropped connections with backoff, the fetch loop backs off instead of spinning and reports failures through the `error` event, and group rebalance steps retry while the cluster is unreachable.
  - `@kafkats/client`: `consumer.stop()`, `producer.disconnect()` and `client.disconnect()` are bounded (about 5 seconds each) when a broker does not respond. Sends still in flight at that point are rejected.
  - `@kafkats/client`: multiple consumers on one `KafkaClient` no longer deadlock. Each consumer group member now uses a dedicated coordinator connection.
  - `TestDriver` feeds records written to a topic the topology also reads back into the topology, and `MockProducer` records timestamps and headers.

  **Additions**

  - `restartOnError` (default `true`): workers restart after recoverable failures with exponential backoff. Errors thrown by user code and fatal Kafka errors set `state()` to `'ERROR'`.
  - `app.getError()` on `FlowApp`.
  - `startTimeoutMs` (default 120000) for how long `start()` waits for workers to join the group. Exactly-once producers now default to a 10 second `transactionTimeoutMs`.
  - `@kafkats/flow-state-lmdb`: the environment is reopened on demand, so an app can be started again after `close()`.
  - `CoPartitioningError`, exported from `@kafkats/flow`.
  - `@kafkats/client`: `consumer.position(topic, partition)` returns the offset the next fetch of an assigned partition starts at.
  - `Grouped.name` for stable repartition topic names.

  **Known limitations**: stream-stream left and outer joins emit the unmatched result eagerly; derived tables (`filter()`/`mapValues()` results) cannot be the lookup side of a join. See the new fault tolerance guide.

- 7ae72e9: Breaking contract changes.

  - All packages require Node.js 22 or later. Node.js 18 and 20 are past their end of life.
  - `FlowConfig.runEach` is now `consumerRun` (flow consumes in batches, and the option takes `RunBatchOptions`).

- 70d1684: Add `@kafkats/flow-state-rocksdb`, a persistent RocksDB state store provider for `@kafkats/flow`.

  - `rocksdb({ stateDir })` is used like the LMDB provider. Writes do not wait for the disk, so stateful topologies under at-least-once are not limited by one disk commit per record. Requires Node.js 22.18 or later.
  - New entry point `@kafkats/flow/state` with `OrderedKeyValueStore`, `OrderedWindowStore`, `OrderedSessionStore` and `OrderedChangelogCheckpointStore`: flow's stores on top of any engine that maps byte keys to byte values in byte order (`OrderedByteStore`). The LMDB provider now uses them; its on-disk format and API are unchanged.
  - `@kafkats/flow-state-lmdb` now requires `@kafkats/flow` 0.14.0 or later.

### Patch Changes

- Updated dependencies [7ae72e9]
- Updated dependencies [754dfb6]
- Updated dependencies [ff86159]
- Updated dependencies [7ae72e9]
- Updated dependencies [70d1684]
  - @kafkats/client@0.14.0

## 0.13.0

### Minor Changes

- 905af84: Remove the consumer `maxRecords` option. `maxBytesPerPartition` now also bounds each delivered partition batch: prefetched Fetch responses buffered while a handler is busy are coalesced only up to that many (estimated) bytes, and the rest stays buffered for the next batch. A record larger than the limit is still delivered on its own. This also fixes out-of-order delivery within a partition: when `maxRecords` ended a batch inside a buffered response and another response for the same partition was buffered behind it, the remainder was moved behind that response and delivered after newer records. Default batches are no longer capped at 500 records, so lower `maxBytesPerPartition` if per-record handling is slow: a rebalance waits for in-flight batches. Priority strategies schedule in bytes: `SchedulerState.recordBudget` is now `byteBudget`, drain entries use `maxBytes` instead of `maxRecords`, and `SchedulerPartition.bufferedRecords` is removed.

### Patch Changes

- Updated dependencies [905af84]
  - @kafkats/client@0.13.0

## 0.12.0

### Minor Changes

- 09d499e: Add pluggable strict and weighted topic-priority scheduling for consumers.

### Patch Changes

- 53e18d2: Keep each consumer partition moving independently across buffered polls so a slow partition handler no longer blocks work already available for other partitions.
- Updated dependencies [53e18d2]
- Updated dependencies [09d499e]
  - @kafkats/client@0.12.0

## 0.11.3

### Patch Changes

- 20f47fe: Graduate `ShareConsumer` from experimental status now that Share Groups are production-ready in Kafka 4.2.
  Require the production-ready ShareFetch and ShareAcknowledge v2 APIs, recommend Kafka 4.2.1 or newer, and cover
  the acquire-mode and lock-renewal paths against Kafka 4.2.
- 4af22ac: Require stable OffsetFetch v7 reads when initializing assigned consumer partitions so pending transactional offset
  commits cannot cause cross-owner duplicate processing during rebalances.
- Updated dependencies [20f47fe]
- Updated dependencies [4af22ac]
  - @kafkats/client@0.11.3

## 0.11.2

### Patch Changes

- a554163: Allow JoinGroup, SyncGroup, Fetch, ShareFetch, and acknowledged Produce requests to wait for their request-declared protocol wait in addition to the client connection timeout. Rejoin promptly when a heartbeat reports a rebalance instead of admitting every partition batch already returned by the current poll, and fairly bound each consumer poll to a configurable `maxRecords` (default 500), retaining excess prefetched records for the next poll.
- Updated dependencies [a554163]
  - @kafkats/client@0.11.2

## 0.11.1

### Patch Changes

- 3ed4da2: Bounded consumer prefetching and fetch-response coalescing. Before issuing a Fetch, the background loop now reserves part of the shared `maxBufferedBytes` budget — concurrent broker fetches split the remaining capacity between them, and the loop parks (instead of polling on a timer) until `poll()` drains records or partitions are removed, preventing unbounded buffer growth. On drain, responses for the same topic-partition and assignment epoch are coalesced so Kafka fetch-response boundaries no longer surface as separate `runBatch` invocations (and separate EOS transactions); responses from different rebalance epochs stay separate to preserve fencing.
- Updated dependencies [3ed4da2]
  - @kafkats/client@0.11.1

## 0.11.0

### Minor Changes

- a101e31: Parallel producer transactions via `transactionConcurrency`. The one-open-transaction limit is per transactional ID, so setting `transactionConcurrency: N` (default 1) makes the producer manage a pool of N internal transactional producers ("lanes") behind the unchanged `transaction()` API: lane 0 keeps the configured `transactionalId`, lanes 1..N-1 append `-1`..`-{N-1}`. Calls are admitted first-in first-out — a call takes a free lane or waits for the next one released — and `transaction:queued` now fires only when all lanes are busy. Transactions may commit out of call order (the same guarantee as independent transactions today); for consume-transform-produce, pass `consumerGroupMetadata` to `sendOffsets()` so zombie fencing works per consumer-group generation regardless of lane. `flush()` and `disconnect()` cover all lanes.

### Patch Changes

- d9d0556: fix(client): stop logging per-transaction events at info level

  An exactly-once loop commits a transaction per batch, so `transaction committed` was
  emitting at batch rate — several lines per second per producer at the default `info`
  level, drowning out everything else in the log. `transaction committed`, `transaction
aborted`, and `sequence wrap - splitting batch` are now `debug`. Pass
  `logLevel: 'debug'` in the client config to get them back.

  Two fencing events moved the other way, from `info` to `warn`: `producer fenced` and
  `share group member fenced; abandoning assignment and rejoining`. Both are abnormal
  conditions that force a re-init or reassignment, and both were previously invisible to
  anyone filtering at `warn` to escape the noise above.

  One-shot lifecycle logging (connect/disconnect, group join/leave, producer init) is
  unchanged and still `info`.

- Updated dependencies [a101e31]
- Updated dependencies [aa6304f]
- Updated dependencies [a101e31]
- Updated dependencies [d9d0556]
  - @kafkats/client@0.11.0

## 0.10.0

### Minor Changes

- 80b3c15: Automatic compression codec registration. When a Snappy, LZ4, or Zstd codec is needed and none is registered, the client now looks for a supported compression library (`snappy`, `snappyjs`, `lz4-napi`, `lz4`, `lz4js`, `@mongodb-js/zstd`, `zstd-napi`) and registers the first one it finds — installing the library is all that's required. The libraries are declared as optional peer dependencies so strict resolvers (Yarn PnP) allow the lookup. Manual registration via `compressionCodecs.register()` still works and takes precedence, and auto-detection can be disabled with `compressionCodecs.autoRegister = false`. Missing-codec errors now include an install hint.

  Snappy decompression now transparently handles Xerial stream framing, so batches produced by Java Kafka clients decode correctly.

- e81d3a5: Ship dual ESM + CommonJS builds. All packages now publish both `dist/index.js` (ESM) and `dist/index.cjs` (CJS) with matching `.d.ts`/`.d.cts` declarations and conditional `exports`, so `require('@kafkats/...')` works in CommonJS projects. The build pipeline moved from `tsc` + `tsc-alias` to `tsdown`.
- 089221b: Concurrent `producer.transaction()` calls now queue instead of throwing `InvalidTxnStateError`. A Kafka producer can only have one open transaction at a time (protocol constraint); previously, sharing a transactional producer across consumer handlers running with `partitionConcurrency > 1` threw the moment two batches finished together. Transactions now wait for capacity in FIFO order, making the consume-transform-produce pattern safe with a single producer.

  - Nested `transaction()` calls from inside the same producer's transaction callback throw immediately instead of deadlocking.
  - New `transaction:queued` producer event reports queue depth for observing commit-bound throughput.
  - `disconnect()` now also refuses to disconnect while transactions are queued (previously only while one was active).
  - The transaction timeout starts when the transaction begins, not while it waits in the queue.

### Patch Changes

- 8f13d86: Delivery-time consumer group metadata is now threaded through `ConsumeContext`, so `tx.sendOffsets(ctx)` can fence offset commits without exposing consumer internals. `tx.sendOffsets(ctx, offsets)` covers batched Flow transactions; the old params shape still works.
- 72ccff2: Flow-managed transactional IDs now include an internal per-process UUID alongside `applicationId` and the worker index, so replicas of the same deployment no longer fence each other's transactional producers. Explicitly configured transactional IDs are preserved unchanged.
- Updated dependencies [80b3c15]
- Updated dependencies [8f13d86]
- Updated dependencies [e81d3a5]
- Updated dependencies [089221b]
- Updated dependencies [72ccff2]
  - @kafkats/client@0.10.0

## 0.9.1

### Patch Changes

- f278cd7: fix(producer): prevent idempotent producer false-ack under leader failover

  Under `acks=all` with an idempotent producer, a batch that was sent and committed by
  the broker but whose response was lost (e.g. the partition leader is killed mid-flight)
  could exhaust its retries and then have its sequence rolled back. A subsequent batch
  reused the same base sequence, the broker answered `DUPLICATE_SEQUENCE_NUMBER` pointing
  at the prior batch's offsets, and the client resolved the _new_ batch's records with
  those offsets — acknowledging records that were never written to the log.

  On retry-exhaustion of an idempotent batch that was already put on the wire, the producer
  now reinitializes the producer id/epoch (rather than reusing the sequence) and rejects the
  affected sends, matching the Java client's handling of append-ambiguous outcomes. A
  stale-epoch guard additionally prevents a response from a previous producer epoch from
  mutating the new epoch's sequence state.

- a7f3952: fix(compression): emit LZ4 frame format for lz4-napi (Kafka rejected raw-block LZ4)

  `createLz4Codec` used `lz4-napi`'s `compress`/`uncompress`, which produce the raw LZ4
  _block_ format. Kafka's RecordBatch v2 requires the LZ4 _frame_ format (magic bytes
  `0x184D2204`), so every `lz4-napi`-compressed produce was rejected by the broker with
  `UnknownServerError` ("invalid magic bytes").

  When the `lz4-napi` instance exposes the framed API (`compressFrame`/`decompressFrame`,
  available in lz4-napi >= 2.x), the codec now uses it. Older `lz4-napi` builds that only
  expose the raw-block API now throw a clear error at codec-creation time instead of
  silently producing data the broker rejects. The `lz4` (node-lz4) and `lz4js` paths
  already use framed APIs and are unaffected.

- 9125b65: fix(consumer): static members must not send LeaveGroup on shutdown (KIP-345)

  A consumer configured with `groupInstanceId` (static membership) was sending a
  `LeaveGroup` request on graceful shutdown. Per KIP-345 a static member must not do
  this — sending `LeaveGroup` triggers an immediate group rebalance, defeating the
  purpose of static membership (cheap rolling restarts, where the member is expected
  to rejoin within `sessionTimeoutMs` with its existing partition assignment).

  `ConsumerGroup.leave()` now skips the `LeaveGroup` request when `groupInstanceId` is
  set and simply stops heartbeating, letting the broker hold the member's partitions
  until it rejoins or the session times out. Dynamic members (no `groupInstanceId`)
  continue to send `LeaveGroup` for an immediate rebalance.

- Updated dependencies [f278cd7]
- Updated dependencies [a7f3952]
- Updated dependencies [9125b65]
  - @kafkats/client@0.9.1

## 0.9.0

### Minor Changes

- 81d00ea: Various fixes from internal testing:

  - Surface internal connection and coordinator errors without crashing the process.
  - Time-bound coordinator retries for transactional producer init and consumer offset fetch/commit. Configurable via the new producer `maxBlockMs` and consumer `defaultApiTimeoutMs` options (both default 60000).
  - Re-discover the group coordinator on `NotCoordinator` during offset commit.
  - Add human-readable messages for `ProducerFenced` and `InvalidProducerEpoch`.

  Share consumer (KIP-932) reliability and correctness fixes:

  - Ride through broker loss / coordinator failover instead of dying on a single broker's transport error.
  - Gap-acknowledge control-batch and compacted offsets so the share-partition start offset advances (previously it could stall and redeliver forever).
  - Rejoin on member fencing (`FencedMemberEpoch` / `UnknownMemberId`) instead of failing fatally.
  - Deliver only records the broker actually acquired (no reprocessing of already-acked records under churn).
  - Treat recoverable per-partition fetch errors as skip-and-continue rather than fatal.
  - Fail (not retry) an acknowledgement on a leader change, matching the broker's record ownership.
  - A failed auto-acknowledgement no longer terminates the consumer; the record redelivers.
  - `stream()` surfaces fatal loop errors and applies back-pressure instead of buffering unboundedly.
  - Validate consumer config and expose `acquisitionLockTimeoutMs`.

### Patch Changes

- Updated dependencies [81d00ea]
  - @kafkats/client@0.9.0

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
