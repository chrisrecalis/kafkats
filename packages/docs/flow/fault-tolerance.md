# Fault Tolerance

This page covers what Flow guarantees when things fail: processing guarantees, how failures are handled, how state is restored, and the known limitations.

## Processing Guarantees

Set `processingGuarantee` in the app config:

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	processingGuarantee: 'exactly_once', // or 'at_least_once' (default)
	commitIntervalMs: 100, // how often work is committed (see below)
})
```

### At-Least-Once (default)

No record is lost. After a crash or a redelivery, records since the last committed offset are processed again, so their **outputs and state updates may be applied again**. A `count()` can over-count, and a record can be written to the output topic twice.

Records are processed in the batches the consumer delivers. Output and changelog records are sent without waiting for each acknowledgement; before offsets are committed, every one of them must have been acknowledged. This happens at the end of a batch and, within a long batch, every `commitIntervalMs`. If a send fails, the worker fails without committing and the batch is processed again. An output record can therefore be written before the changelog record of the same input is acknowledged, and is written again after such a failure. This is the same contract as Kafka Streams: flush everything, then commit.

The offsets themselves are committed by the consumer: every `consumerRun.autoCommitIntervalMs` (default `5000`), when partitions are revoked and when the app closes. A process that is killed therefore repeats everything since its last offset commit, up to that interval of work. Lower `autoCommitIntervalMs` to repeat less after a crash, at the cost of more commit requests.

### Exactly-Once

Input offsets, output records and changelog records are committed atomically in a single Kafka transaction. Transactions are committed every `commitIntervalMs` (default `100`), when the app closes, and before a rebalance.

State store writes are **buffered while the transaction is open** and applied to the local store only after the commit succeeds. An aborted transaction, or a crash at any point, never leaves uncommitted updates in a store. This includes persistent LMDB and RocksDB stores.

Read the output topics with `isolationLevel: 'read_committed'`, otherwise consumers see records from transactions that are later aborted:

```typescript
const consumer = client.consumer({
	groupId: 'downstream',
	isolationLevel: 'read_committed',
})
```

Flow's own consumers use `read_committed` automatically when `processingGuarantee` is `exactly_once`.

## Failure Handling

A worker (one per `numStreamThreads`) that hits a failure **never continues past it**. It stops consuming, so nothing after the last committed offset is acknowledged. What happens next depends on the kind of failure.

| Failure                                                                                        | What happens                                                                                                           |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Recoverable**: broker or network errors, failed commits, lost sessions, restore failures     | The worker aborts its transaction, drops buffered state, rejoins the group and resumes from the last committed offsets |
| **Not retried**: errors thrown by your code (mappers, aggregators, codecs), fatal Kafka errors | `state()` becomes `'ERROR'` and every worker stops                                                                     |

Fatal Kafka errors are: fenced static group member, authorization failures, unsupported version, invalid session timeout or config, inconsistent group protocol, and message too large.

Recoverable failures are retried with exponential backoff, starting at 0.5 seconds and capped at 30 seconds. The backoff resets once the worker has processed records again. Set `restartOnError: false` to disable restarts, so every failure puts the app in `'ERROR'`:

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	restartOnError: false,
})
```

A transaction that stays open longer than `producer.transactionTimeoutMs` (a stalled broker, a frozen process) is aborted by the broker. The worker treats the resulting fenced-producer error as recoverable: it starts again with a fresh producer and reprocesses from the committed offsets.

If the brokers are unreachable, workers keep retrying until they are back. `close()` stays bounded in that case, but it can take up to the request timeout plus a few seconds, because the final commit is attempted first.

An error that is not retried stops the whole app, not only the worker that hit it: with several stream threads, another worker would be assigned the failed worker's partitions and fail on the same record. This is what Kafka Streams does by default.

When the app is in `'ERROR'`, `app.getError()` returns the error. The records that were not committed are processed again after you restart the app:

```typescript
await app.start()

setInterval(() => {
	if (app.state() === 'ERROR') {
		console.error('Flow failed', app.getError())
		process.exit(1) // let your supervisor restart the process
	}
}, 1000)
```

If a failure happens during `start()`, `start()` rejects, the state is `'ERROR'`, and `start()` can be called again.

::: tip
`getError()` returns the most recent error the app observed. It can also be set by transient errors the app recovered from, so check `state()` to find out whether the app has failed.
:::

### Recovering From a Crash Under Exactly-Once

A crashed instance leaves its last transaction open until the broker aborts it after the producer's `transactionTimeoutMs`. Until then, `read_committed` consumers of its output topics and the state restoration of its successor wait. Flow sets `transactionTimeoutMs` to 10 seconds for exactly-once producers (override it via `producer.transactionTimeoutMs`); a transaction that stays open longer than that, for example because a single record takes that long to process, is aborted and retried.

`start()` waits up to `startTimeoutMs` (default `120000`) for each worker to join the consumer group. After a crash the group may wait for the dead member's session timeout before it rebalances.

### Application States

| State         | Meaning                                                                                                                                      |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `CREATED`     | Created, `start()` not called yet                                                                                                            |
| `RUNNING`     | Started and processing                                                                                                                       |
| `REBALANCING` | A rebalance is in progress, or a worker is waiting to restart after a recoverable failure. Returns to `RUNNING` once partitions are assigned |
| `ERROR`       | Processing stopped on an error that is not retried (or `restartOnError` is `false`), or `start()` failed. Check `getError()`                 |
| `STOPPED`     | `close()` completed                                                                                                                          |

## State Restoration

On every assignment, the newly assigned partitions are **paused**, their state is restored from the changelog, and then they resume. No record is processed against state that has not been restored.

- State partition `N` is restored from changelog partition `N` (see [Partitioned State](/flow/state-stores#partitioned-state)).
- A restore that makes no progress for `changelog.restoration.idleTimeoutMs` (default `30000`) **fails**. It never completes with partial state. During `start()` the failure rejects `start()`; afterwards it is a recoverable failure and the worker restarts.
- Restoration reads to the end of the changelog. If an instance crashed with a transaction open, the restore waits until the broker aborts that transaction (after its transaction timeout) instead of stopping at an older position; this wait is not treated as a stall.
- `start()` resolves once the partitions assigned at startup are restored.

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	changelog: {
		restoration: {
			idleTimeoutMs: 60_000, // give slow brokers more time
		},
	},
})
```

### Checkpoints

Providers with durable state (LMDB) keep a checkpoint per changelog partition, so a restart restores only what is missing instead of replaying the whole changelog. A checkpoint is ignored, and the store is wiped and rebuilt from the start of the changelog, when:

- it is outside the changelog's offset range (for example the topic was recreated or truncated by retention), or
- the local store is empty although the checkpoint claims progress (for example the state directory was replaced).

In-memory state is caught up from in-process checkpoints after a rebalance, so a partition that comes straight back is not rebuilt from scratch. A partition that moved to another instance is dropped about 30 seconds later, because the other instance now advances it.

### Global Tables

A [global table](/flow/tables#global-tables) is fully loaded before any record is processed, and is rebuilt from its topic on every start (it has no changelog). If it makes no progress for `idleTimeoutMs` while loading, `start()` fails.

After startup the table follows its topic. If the connection to Kafka fails, its consumer restarts from the last row it applied. A row that cannot be applied (its codec throws, or the store rejects the write) would fail the same way again. During the initial load such a row fails `start()`. Afterwards the app goes to `state()` `'ERROR'` and its stream processing stops, so that nothing more is joined against a table that no longer advances.

## Upgrading From Earlier Versions

- **Stop all instances before upgrading.** Consumers now always use the `range` partition assignor (required for [co-partitioning](/flow/state-stores#co-partitioning)), which changes the group protocol, so old and new instances cannot be in the same group at the same time. A rolling upgrade does not work.
- **Windowed stores, session stores and join buffers restart from empty.** Earlier versions did not write them partition-aligned, so their state cannot be reused. In-flight windows and joins start again after the upgrade.
- **Key-value state is restored normally.** State from inputs that used the default partitioner is rebuilt from the changelog into the new per-partition stores.
- **LMDB:** stores are now named `<applicationId>.<storeName>.p<partition>`. The old unpartitioned databases stay in the file until you delete the state directory, and count towards `maxDbs`. Deleting it is safe, as state is rebuilt from the changelog.
- Topologies that re-key a stream (`selectKey()`, `map()`, `groupBy()`) before a stateful operation now write through [internal repartition topics](/flow/streams#repartitioning), and inputs to a stateful operation must be co-partitioned. See [Co-partitioning](/flow/state-stores#co-partitioning).
- `through()` now reads the topic back. The topic must exist.
- **At-least-once commits per batch, not per record.** Output and changelog records are acknowledged together before offsets are committed (see [At-Least-Once](#at-least-once-default)). After a failed send, up to one `commitIntervalMs` of records is processed again, and state updates of those records are applied again. After a process crash it is everything since the consumer's last offset commit (`consumerRun.autoCommitIntervalMs`, default `5000`).
- **Every consumer opens its own fetch connection to each broker.** Applications with several stream threads or a global table open more connections than before; check `max.connections.per.ip` on brokers with tight limits.
- **In-memory window and session stores return a key's windows in time order** from `fetch()` and `findSessions()`, like the persistent stores. They returned them in insertion order before.

## Known Limitations

- **Name your state stores and repartition topics before changing a topology.** Unnamed stores get names from the order in which operators are defined (`count-store-0`, `repartition-1`, ...). Adding or removing a stateful operator earlier in the topology shifts those numbers, and an operator then picks up another operator's changelog. Set `storeName` (and `name` for `groupBy()`) on every stateful operator of an application you plan to evolve.
- **`offsetReset: 'latest'` and failures before the first commit.** A partition without a committed offset is pinned to the position it was first read from, and that position is committed right away, so a restart or crash before the first processed batch resumes from it instead of from the then-current end of the topic. If that initial commit fails (a warning is logged), a process that crashes before committing anything starts from the end of the topic again. Use `'earliest'` when no record may be skipped.
- **Stream time after a restart is rebuilt from the restored store.** For windowed aggregations it resumes from the newest window start, which can be up to one window size behind the stream time before the restart. A late record inside that margin can be accepted where an uninterrupted run would have dropped it.

- **Stream-stream `leftJoin()` and `outerJoin()` emit the unmatched result eagerly.** When a later record matches, a second record is emitted for the same input.
- **Derived tables are not materialized.** The result of `filter()` or `mapValues()` on a table, a table-table join, or a windowed aggregation has no state store of its own and cannot be the lookup side of a join. Join the table it was derived from instead, or write it to a topic and read it back with `app.table()`. Tables from `count()`, `reduce()` and `aggregate()` can be joined.

## Next Steps

- [State Stores](/flow/state-stores) - Partitioned state, changelogs and LMDB
- [Joins](/flow/joins) - Co-partitioning requirements
- [Testing](/flow/testing) - Test topologies without a broker
