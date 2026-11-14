---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Redesign the `@kafkats/flow` runtime for correctness: partitioned state, repartitioning, a real `globalTable()`, transactional state under exactly-once, and workers that never continue past a failure.

**Breaking and behaviour changes**

- State is partitioned: each source partition has its own slice of every state store, logged to the changelog partition with the same index and restored from it. Provider-level store names are now `<storeName>.p<partition>`, and the default LMDB `maxDbs` is 4096.
- Inputs to a stateful operation must be co-partitioned. All topics feeding one stateful operation (merged streams, both sides of a join, the stream and table of a join) need the same partition count, otherwise `start()` throws the new `CoPartitioningError`. Records with the same key must be in the same partition index.
- `groupBy(fn, { key, value, name? })`, and `selectKey()`/`map()` followed by `groupByKey()`, a join or `toTable()`, now write through an internal topic `<applicationId>-<name | repartition-N>-repartition` and read it back. The topic is created at startup (it must exist with `changelog.autoCreate: false`), and key and value codecs are required. Pass `name` for a stable topic name.
- `through(topic)` now continues from what is read back from the topic instead of passing records straight on. The topic must exist. Internal and `through()` topics are read from the beginning when the group has no committed offset.
- `globalTable()` is read in full by every instance outside the consumer group and loaded before processing starts. It writes no changelog and can only be the lookup side of a stream join.
- Consumers always use the `range` partition assignor. Stop all instances before upgrading from an earlier version (no rolling upgrade). Windowed and session state and join buffers written by earlier versions were not partition-aligned and restart from empty; key-value state is rebuilt from the changelog.
- A restore that makes no progress for `changelog.restoration.idleTimeoutMs` (now 30000 by default) fails instead of completing with partial state.

**Fixes**

- Exactly-once: a failed transaction commit no longer loses records. The worker stops, aborts and resumes from the last committed offsets.
- Exactly-once: uncommitted state updates no longer survive an abort or crash, including in persistent LMDB stores. Store writes are buffered and applied only after the commit.
- Restoration no longer completes early with partial state, and an unusable checkpoint wipes and rebuilds the store. It reads to the end of the changelog and waits out a crashed instance's open transaction instead of stopping at an older position (which left exactly-once state behind the committed offsets).
- State is no longer lost or mixed across instances for windowed stores, joins and non-default partitioning.
- `globalTable()` is now really global.
- `KTable.groupBy()` aggregations are repartitioned by the grouped key and are now correct with several instances or threads; an emptied group is removed in both processing modes.
- Stream time is tracked per partition, stream-stream joins keep records with the same key and timestamp, `KTable.filter()` emits a tombstone when a row stops matching, `filter()`/`mapValues()` on tables never call your function for tombstones, session `reduce()` merges sessions in time order with `(aggregate, value)` argument order, `aggregate()` on session windows uses real sessions (and requires a `merger` option) instead of silently running as fixed windows, `to()`/`through()` keep the input record's timestamp, and tables from `count()`/`reduce()`/`aggregate()` can be joined.
- `@kafkats/client`: multiple consumers on one `KafkaClient` no longer deadlock. Each consumer group member now uses a dedicated coordinator connection.
- `TestDriver` feeds records written to a topic the topology also reads back into the topology, and `MockProducer` records timestamps and headers.

**Additions**

- `restartOnError` (default `true`): workers restart after recoverable failures with exponential backoff. Errors thrown by user code and fatal Kafka errors set `state()` to `'ERROR'`.
- `app.getError()` on `FlowApp`.
- `startTimeoutMs` (default 120000) for how long `start()` waits for workers to join the group. Exactly-once producers now default to a 10 second `transactionTimeoutMs`.
- `@kafkats/flow-state-lmdb`: the environment is reopened on demand, so an app can be started again after `close()`.
- `CoPartitioningError`, exported from `@kafkats/flow`.
- `Grouped.name` for stable repartition topic names.

**Known limitations**: stream-stream left and outer joins emit the unmatched result eagerly; derived tables (`filter()`/`mapValues()` results) cannot be the lookup side of a join. See the new fault tolerance guide.
