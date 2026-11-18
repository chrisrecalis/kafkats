# Fault Tolerance

Flow follows Kafka Streams' runtime model: work is divided into tasks, each task commits its progress with its state, and failures are handled per kind: in place where only some tasks are affected, by rejoining the group where the instance's membership or producer is affected, and through the uncaught exception handler where nothing automatic can help.

## The Runtime

An instance runs one **group membership** at a time: a group consumer (group id = `applicationId`), a producer, and the tasks of the partitions it is assigned.

- The consumer's handlers only append the fetched records to the queues of their tasks. A partition whose queue holds `bufferedRecordsPerPartition` records (default 1000) is paused until its task catches up.
- A scheduler runs the tasks that have records to process: different tasks concurrently (a task waiting on slow I/O does not hold up the others), the records of one task one at a time, in timestamp order across its partitions. It also runs punctuators and commits.
- Commits are barriers: a commit starts once every task finished the record it was processing, and no record is processed while it runs. The outputs of a record and its offset therefore always end up in the same commit.

There are no threads: an instance uses one Node.js event loop. Scale out by running more instances of the application; the group divides the partitions, and with them the tasks, among them.

## Processing Guarantees

### At-Least-Once (default)

Every `commitIntervalMs` (default **5000**), and when partitions are revoked, when the app closes, or when a processor calls `context.commit()`:

1. Wait until every record sent so far (outputs, repartition and changelog records) is acknowledged.
2. Commit the input offsets of every task, with the task's commit metadata (its partition times and processor clocks), through the group consumer.
3. Write the changelog checkpoints of the stores.

After a crash, the records processed since the last commit are processed again: their outputs are written again and their state updates applied again. Nothing is lost.

An interval commit that times out or meets a rebalance is retried at the next interval; only after commits failed for `taskTimeoutMs` (default 300000) does the instance leave and rejoin the group.

### Exactly-Once

```typescript
flow({ applicationId: 'payments', client, processingGuarantee: 'exactly_once' })
```

All tasks of an instance write into one Kafka transaction, committed every `commitIntervalMs` (default **100**), at revocation and at close:

1. Wait until every record sent in the transaction is acknowledged.
2. Add the input offsets of every task, with their commit metadata, to the transaction, fenced by the group generation of the current assignment (a zombie instance cannot commit offsets of partitions it lost).
3. Commit the transaction.
4. Apply the store writes buffered during the transaction to the local stores, then write checkpoints.

Outputs, changelog records and offsets become visible together, or not at all. Store writes stay in a per-store buffer until the transaction commits, so an aborted transaction or a crash never leaves uncommitted updates in a store, also with persistent providers. Consumers of the output topics must read with `isolationLevel: 'read_committed'`; flow's own consumers do.

The transactional id is `<applicationId>-<processId>`, where the process id is a random UUID of each `start()`, so instances can share every setting. A transaction left open by a crashed instance is aborted by the broker after `producer.transactionTimeoutMs` (default 10000); until then, `read_committed` readers of its partitions (restores included) wait for it.

### Stream Time Across Restarts

The commit metadata of each input partition carries its partition time and the clocks of the task's windowed operators, in Kafka Streams' format. A task restarted elsewhere seeds its clocks from it, so a record is late or on time no matter where and when it is processed. See [Stream time](/flow/windowing#stream-time).

## Rebalances

Flow uses the eager `range` assignor, which gives a member partition N of every topic, so a task's input partitions stay together:

- **Revocation**: inside the consumer's revocation hook (which the rebalance waits for), flow commits the tasks, then suspends them. Their stores stay open.
- **Assignment**: tasks assigned again resume and catch up from their checkpoints; new tasks are created and restored from their changelogs (see [Restoration](/flow/state-stores#restoration)). Tasks not assigned again are closed. A member assigned only some of the input partitions of a stateful task does not process it: it leaves and rejoins.
- **Lost partitions** (the member's session expired): the tasks close without committing, as their partitions may already belong to another member.

Interval commits are skipped while a rebalance is pending. The app is `REBALANCING` from the start of a rebalance until every assigned task is restored and running again.

## Failure Handling

| Failure                                                                                         | Handling                                                                                                        |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| A record that cannot be decoded, processed, encoded or sent                                     | The [exception handlers](/flow/error-handling) decide: drop it (`'continue'`) or fail (`'fail'`, fatal)          |
| At-least-once: a send that failed with a retriable error after the producer's retries           | The affected tasks are revived in place: they close dirty, reopen and reprocess from their last commit            |
| At-least-once: interval commits timing out                                                      | Retried at the next interval; after `taskTimeoutMs`, as a migration                                             |
| A changelog that cannot be restored from its position (records deleted)                         | The task's stores are wiped and rebuilt in place                                                                |
| Fenced producer, aborted transaction, failed commit, lost partitions, partial assignment, a lost connection, any other Kafka error | **Migration**: every task closes dirty, the consumer leaves the group, and a new membership joins after a backoff and resumes from the committed offsets |
| Fatal errors: a handler answered `'fail'`, a processor's `init()`/`close()` threw, authorization, invalid configuration, mismatched partition counts, an invalid timestamp | The [uncaught exception handler](/flow/error-handling#uncaught-exceptions): `'shutdown'` (state `ERROR`) or `'restart'` |

The backoff before rejoining starts at 0.5 s and doubles up to 30 s, and starts over once a membership committed. Migrations are retried without limit: a broker outage only delays processing. A failed membership processes and commits nothing more; under exactly-once its open transaction is aborted and its buffered store writes are dropped, so the stores keep their committed state.

`app.getError()` returns the most recent recoverable failure (it does not change the state), or the fatal error while the app is `ERROR`.

## Closing

```typescript
await app.close({ timeoutMs: 30_000 }) // default 20000
```

`close()` commits what was processed, closes the tasks, leaves the group, stops the global tables, closes the state stores, and disconnects the client if the app created it. It returns within `timeoutMs` even when the cluster is unreachable: network steps are cut short before the deadline and the tasks then close without committing; what was not committed is processed again by the next owner of the partitions. A `close()` during `start()` makes the start fail.

## Offset Reset

Partitions without a committed offset start where their source's reset policy says: `Consumed.offsetReset`, else `consumer.autoOffsetReset`, else `'earliest'` (Kafka Streams' default). Repartition and `through()` topics always start from `'earliest'`.

- `'latest'`: the position is pinned when the process first starts the partition, and kept until an offset is committed, so a rebalance or recovery before the first commit does not skip what arrived meanwhile. The pinned position is also committed (best effort), so a restart of the process resumes from it too.
- `'none'`: a partition without a committed offset, or whose position is out of range, is fatal (`NoOffsetResetError`).

## Repartition Topics

Repartition topics are created with `cleanup.policy=delete` and `retention.ms=-1`, so records are never lost to retention while an application is down. After commits, at most every `repartitionPurgeIntervalMs` (default 30000), flow deletes their records below the committed offsets (`DeleteRecords`). Purging is best effort: a failure is logged and retried with the next purge. With `repartitionPurgeIntervalMs: 0`, or without permission to delete records, set a retention through `changelog.topicConfigs` to bound them.

## Global Tables

Each global table is read by a consumer of its own, outside the group, with `read_committed`. `start()` loads every global table to the end its topic had when the load began, before any task processes a record; afterwards the table follows its topic. A failed global consumer is replaced after a backoff. A position out of range reloads the table in place while stream processing pauses (after a commit). A row that cannot be applied fails `start()`, and stops the app afterwards.

## Application States

| State         | Meaning                                                                                       |
| ------------- | --------------------------------------------------------------------------------------------- |
| `CREATED`     | Not started, or `start()` in progress                                                         |
| `RUNNING`     | Every assigned task is restored and processing                                                |
| `REBALANCING` | A rebalance, a restoration, or a recovery (backoff and rejoin) is in progress                |
| `ERROR`       | A fatal error stopped the app, or `start()` failed; see `getError()`. `close()` releases it   |
| `STOPPED`     | Closed                                                                                        |

## Monitoring

- `app.state()` as a readiness probe, `app.getError()` for the cause of a recovery or a stop.
- Logs go through the client's logger (`logger` or `logLevel` of the client config), with `component: 'flow'`. Dropped records (late records, null keys, records a handler dropped) are logged as warnings, at most once per second per reason and task, with a count.
- Each record a task processes is published on the `kafkats:flow:process` tracing channel (`flowDiagnostics`); `@kafkats/opentelemetry` turns it into spans. See [Observability](/client/observability).

## Known Limitations

Not available yet: standby replicas and a task-aware sticky assignor (state moves with every rebalance and is restored from changelogs), interactive queries, the record cache, cogroup, foreign-key table joins, versioned stores and stream-table join grace, topology optimizations (merging repartition topics, reusing source topics as changelogs), multiple threads per instance, `SHUTDOWN_APPLICATION`, dead-letter queues, metrics beyond logs and the tracing channel, `UnlimitedWindows`. See [Differences from Kafka Streams](/flow/kafka-streams).
