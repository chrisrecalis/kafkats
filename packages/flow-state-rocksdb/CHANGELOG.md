# @kafkats/flow-state-rocksdb

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
  - @kafkats/flow@0.14.0
