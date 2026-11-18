# Upgrading to 0.14

0.14 rebuilds `@kafkats/flow` on Kafka Streams' task model: tasks per subtopology and partition with their own state, stream time and offsets, Kafka Streams' commit protocol, grace periods and retention, and the Processor API. Most topologies keep compiling, but several defaults, formats and semantics changed. This page lists every breaking change and what to do about it.

## Before You Deploy

**Stop every instance and reset the application's state.** State stores, changelogs and windowed keys use new formats (timestamped stores, Kafka Streams' windowed key formats, partitioned state), and the consumer group now uses the `range` assignor; old and new instances cannot run side by side, and old state cannot be read. Either:

- delete the application's internal topics (`<applicationId>-*-changelog`, `<applicationId>-*-repartition`) and its local state directory, then start the new version; or
- start the new version with a new `applicationId` (it reads its inputs from the start, see [Offset reset](/flow/fault-tolerance#offset-reset)).

## Configuration

Removed options throw `FlowConfigError` from `flow()`, with their replacement in the message:

| Removed                                                                        | Replacement                                                                                           |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `numStreamThreads`                                                             | Run more instances. The tasks of one instance already process concurrently.                           |
| `runEach` (and `consumerRun` of pre-release builds)                            | `commitIntervalMs`; there is no partition concurrency setting.                                        |
| `stateDir`                                                                     | Give the directory to the provider: `lmdb({ stateDir })`, `rocksdb({ stateDir })`.                    |
| `restartOnError` (pre-release builds)                                          | `uncaughtExceptionHandler: 'shutdown'` (default) or `'restart'`. Recoverable failures always recover. |
| `consumer.groupId`, `consumer.onBeforeRebalance`, the consumer rebalance hooks | None: the group id is `applicationId`, and flow handles every rebalance.                              |
| `consumer.partitionAssignmentStrategy` other than `'range'`                    | None: `range` is required.                                                                            |

Changed defaults:

- **Commits under at-least-once**: flow commits offsets itself every `commitIntervalMs`, default **5000** (with Kafka Streams' commit metadata); the consumer's automatic commits are off. Set `commitIntervalMs` for another interval. Exactly-once keeps 100.
- **Offset reset**: partitions without a committed offset start from **`'earliest'`** (Kafka Streams' default; it was the client's `'latest'`). Set `consumer.autoOffsetReset: 'latest'` to keep the old behaviour, or `Consumed.offsetReset` per source. A source with `'none'` and no committed offset stops the app with `NoOffsetResetError`.
- **Exactly-once transactional id**: `<applicationId>-<processId>` (a random process id per `start()`). A `producer.transactionalId` you set is used as it is, so it must be unique among running instances.
- **Exactly-once transaction timeout**: the producer's `transactionTimeoutMs` defaults to **10000** (Kafka Streams' default; it was the client's 60000), and `flow()` throws `FlowConfigError` when it is below `commitIntervalMs`, since a transaction stays open for a commit interval. If you raised `commitIntervalMs` above 10 s, raise `producer.transactionTimeoutMs` with it.
- **`close({ timeoutMs })`** returns within `timeoutMs` (default 20000), also when the cluster is unreachable.
- **`state()`** is `'RUNNING'` only when every assigned task is restored and running; it is `'REBALANCING'` during a rebalance, a restoration or a recovery. Use it as a readiness probe.

See [Configuration](/flow/configuration) for every option.

## Partitioning and Topics

- **State is per task.** Each input partition has its own instance of every store, logged to the same partition of the store's changelog. Provider store names are `<applicationId>.<storeName>.p<partition>` (the LMDB provider's default `maxDbs` is 4096).
- **Co-partitioning is checked.** Topics feeding one stateful subtopology (both sides of a join, a stream and the table it looks up, merged streams before an aggregation) must have the same partition count, otherwise `start()` throws `CoPartitioningError`.
- **Repartitioning.** A stream whose key may have changed (`map()`, `flatMap()`, `selectKey()`, `process()`, `KTable.toStream(mapper)`, `groupBy()`) is written to an internal topic `<applicationId>-<name>-repartition` and read back before a key-based operation. It needs key and value codecs (a `TopologyError` names the operator that lacks one); name the operation (`Grouped.name`, `Joined.name`, ...) for a stable topic name. Repartition topics are created with `cleanup.policy=delete` and `retention.ms=-1` and purged up to the committed offsets every `repartitionPurgeIntervalMs` (30000).
- **`through(topic)`** (deprecated) continues with what it reads back from the topic, which must exist. Prefer `repartition()`.
- **Names.** A `Grouped.name` is shared by every aggregation of its grouping (one repartition topic). Illegal names, two stores of one name and a repartition name claimed by two different operations throw `TopologyError` from the DSL call that makes the mistake.

## Windows

- **Grace periods decide lateness**, by stream time per operator, as in Kafka Streams. Use the new factories: `TimeWindows.ofSizeWithNoGrace` / `ofSizeAndGrace`, `SessionWindows.ofInactivityGapWithNoGrace` / `ofInactivityGapAndGrace`, `SlidingWindows.ofTimeDifferenceWithNoGrace` / `ofTimeDifferenceAndGrace`, `JoinWindows.ofTimeDifferenceWithNoGrace` / `ofTimeDifferenceAndGrace`.
- **Deprecated factories get Kafka Streams' legacy grace**: `TimeWindows.of(size)` accepts records `max(24h - size, 0)` late, `SessionWindows.withInactivityGap(gap)` `max(24h - gap, 0)`, `JoinWindows.of(d)` `max(24h - 2d, 0)`. Their stores keep about a day of windows; a warning names the replacement once per process. Switch to the new factories to bound memory and get final results sooner.
- **Retention** defaults to what a record can still update (size + grace, gap + grace, 2 × time difference + grace); `Materialized.retention` keeps windows longer.
- **Windowed keys use Kafka Streams' formats** on topics: time and sliding windows `[key][start int64 BE]`, sessions `[key][end int64 BE][start int64 BE]` (`WindowedCodecs`). Windowed results are partitioned by their base key, at `to()` and at repartition topics, and are no longer repartitioned before key-based operations. To join them with a topic partitioned by the whole windowed key, `repartition()` with a plain key codec.
- **Null values** reach the aggregators and reducers of time and session window aggregations, and `count()` counts them (Kafka Streams); their callbacks are typed `V | null`. Handle them, or filter tombstones before `groupByKey()`. Non-windowed and sliding window aggregations still drop them.
- **Session aggregations** retract every session a record merges (a record with a null value for each), also one with the merged session's bounds, before forwarding the merged session; `aggregate()` takes the merger as its third argument (the `merger` option is deprecated). Session results carry the session end as their timestamp.
- `windowedBy()` returns `TimeWindowedKStream`, `SessionWindowedKStream` or `SlidingWindowedKStream` (was `WindowedKGroupedStream`), whose options take `retention` and `emit`. Sliding windows are implemented.

## Joins

- **Stream-stream joins take `JoinWindows` as their third argument** and `StreamJoined` options. Every match carries the timestamp `max(left, right)` (it carried the timestamp of the record that produced it), and records of one key and timestamp on a side all join (a later one replaced an earlier one). The `Joined.within` form is deprecated and maps to `JoinWindows.of(size)`: its unmatched records are still joined with `null` at once, and each side keeps the records of the last `max(24h, 2 × size)` of stream time.
- **Left and outer joins built with the new factories emit unmatched records once their window closed** (when stream time passed `ts + after + grace`), not eagerly. Expect the `(value, null)` results later; in tests, send a later record to move stream time.
- **`globalTable()` returns a `GlobalKTable`**, only usable as the lookup side of `KStream.join` / `leftJoin` with a key selector: `join(globalTable, (key, value) => lookupKey, joiner)`. The two-argument form is deprecated and looks up the record key.
- Records with a null key are joined with null by the outer sides of left and outer joins (KIP-962) instead of being dropped.

## Tables and Aggregations

- **Every callback takes `(key, value)`.** `selectKey((key, value) => ...)` was `(value, key)`: swap the arguments.
- **Table callbacks never see deletes.** `KTable.filter()`, `filterNot()`, `mapValues()`, `groupBy()` and the table joins pass deletes on without calling your function, so their parameters are typed without `null`. `transformValues()` and `toStream(mapper)` do see them.
- **Grouped tables keep their aggregate when a group empties** (a count goes to 0) and send no tombstone, as Kafka Streams does. Filter zeros downstream if needed. `KTable.groupBy()`'s mapper may change the value type (give `Grouped.value`).
- **`count()` results are int64 big-endian** (Kafka Streams' `Serdes.Long`; they were 8-byte little-endian doubles). Pass `value` in the options of `count()` or of `to()` to keep another encoding.
- **Result timestamps** follow Kafka Streams: aggregations `max(record, previous result)`, time and sliding windows the largest record timestamp of the window, sessions their end, table-table joins the larger timestamp of both sides.
- **Stores of tables and aggregations are timestamped**: values are stored as `[int64 BE timestamp][value]`, and reading them through `getStateStore()` gives `{ value, timestamp }`.

## Failures

- Errors of your code and fatal Kafka errors go through Kafka Streams-style handlers: `deserializationExceptionHandler`, `processingExceptionHandler`, `productionExceptionHandler` (`'fail'` by default) and `uncaughtExceptionHandler` (`'shutdown'` by default, state `ERROR`). See [Error Handling](/flow/error-handling).
- Infrastructure failures always recover: a send that failed after the producer's retries (at-least-once) revives the affected tasks in place, commit timeouts are retried at the next interval, and fencing, lost partitions and failed commits make the instance rejoin the group after a backoff.

## Testing

`@kafkats/flow/testing` runs the production task runtime, deterministically:

- `send()` resolves once the record and everything it led to were processed; offsets are committed after every record by default (`commitEvery`).
- `driver.consumers` and `driver.producers` have exactly one element: drop assertions on consumers per thread.
- New: `advanceWallClockTime(ms)`, `getStateStore(name, partition?)`, `driver.globalTable()`, `initialWallClockTimeMs`, `partitions`, `emitIntervalMs`.
- `vi` is no longer re-exported: import it from `vitest`.

## Removed Exports

- `WindowedKGroupedStream`: use `TimeWindowedKStream`, `SessionWindowedKStream` or `SlidingWindowedKStream`.
- `ChangelogTopicSpec`.
- `ChangelogWriter`, `ChangelogBackedKeyValueStore`, `ChangelogBackedWindowStore` and `ChangelogBackedSessionStore`: the runtime adds changelog logging to every store whose definition enables it (`logging` of `Stores` definitions, `changelog` of `Materialized` and the DSL options), and sends its records with the task's output and commits.
- `TestMessage` and `vi` from `@kafkats/flow/testing`.

## New

The [Processor API](/flow/processor-api), [`suppress()`](/flow/suppress), `emit: 'on-window-close'`, `split()`, `repartition()`, `Stores` and `addStateStore()`, `{ name }` on every operator, [`describe()`](/flow/topology#describe), the exception handlers, timestamp extractors, `getError()`, `taskTimeoutMs`, `maxTaskIdleMs`, `bufferedRecordsPerPartition`, `repartitionPurgeIntervalMs`, and the `kafkats:flow:process` tracing channel. See [Differences from Kafka Streams](/flow/kafka-streams) for where flow deliberately differs.
