# @kafkats/flow

Kafka Streams' processing model on top of `@kafkats/client`: the DSL (`KStream`, `KTable`, `GlobalKTable`, windows,
joins, `suppress()`), the Processor API, and a task runtime with Kafka Streams' commit protocol for at-least-once and
exactly-once. User documentation: `packages/docs/flow/*.md` (where flow deliberately differs from Kafka Streams:
`kafka-streams.md`; breaking changes: `upgrading.md`).

## Commands

```bash
pnpm build             # tsdown: dist/index, dist/testing (@kafkats/flow/testing), dist/state (@kafkats/flow/state)
pnpm typecheck         # tsc over src, tests and bench
pnpm test:unit         # unit tests (TestDriver, per-component suites); no broker needed
pnpm bench:task-core   # micro-benchmark of the task core against bench/baseline.json (--record rewrites it)

# Integration tests read the client from its dist: build it first.
pnpm -C packages/client build
# Against the shared broker (otherwise the global setup starts a testcontainers broker):
KAFKA_BROKERS=127.0.0.1:19092 VITEST_MAX_WORKERS=2 pnpm -C packages/flow exec vitest run -c vitest.integration.config.ts [files]

# Chaos suite (27 scenarios; single broker kafka-chaos at 127.0.0.1:29092, three-broker cluster kafka-mb1..3 at
# 127.0.0.1:39091-39093, both brought up by tests/chaos/brokers.sh). Patterns are globs on scenario names.
packages/flow/tests/chaos/run-all.sh ['1b-*' | '3b-*' | <name>...]
```

- `VITEST_MAX_WORKERS=2` on shared machines: integration and chaos runs are memory-heavy. The three-broker scenarios
  wait for `CHAOS_MIN_FREE_MB` (default 5000) of available memory and stop the cluster containers they started.
- `run-all.sh` holds a lock (`CHAOS_LOCK`, default `/tmp/kafkats-brokers.lock`) so chaos, integration and perf runs do
  not overlap; one log per scenario goes to `CHAOS_LOG_DIR`. A scenario passes when its harness prints `RESULT ...: PASS`.
- The state store providers run the shared suites of `@kafkats/flow-state-testkit` (`pnpm -C packages/flow-state-lmdb
test`, `pnpm -C packages/flow-state-rocksdb test`).
- `KAFKA_TS_LOG_LEVEL=debug` for verbose client and flow logs.

## Runtime Model

- **Topology**: the DSL registers nodes, stores and internal topics with the `InternalTopologyBuilder`; `build()`
  (at `start()` or `describe()`, after which DSL calls throw `TopologyError`) cuts it into subtopologies: nodes
  connected by parent edges, processors sharing a store and sources of one topic are united; repartition and through
  topics separate them. Names come from `{ name }` options or counters in DSL-call order (Kafka Streams' rules).
- **Tasks**: one `StreamTask` per subtopology and input partition (`TaskId` `<sub>_<partition>`, interned). A task
  owns its record queues (`PartitionGroup`: timestamp-ordered selection, KIP-695 idling with `maxTaskIdleMs`), its
  processor graph, its store instances (`TaskStateManager`), its clocks, offsets and commit metadata, and goes through
  Kafka Streams' states CREATED, RESTORING, RUNNING, SUSPENDED, CLOSED.
- **One runtime per start**: `FlowRuntime` connects, prepares the topics (partition counts, co-partitioning, creation
  of changelog and repartition topics), loads the global tables, then runs one `Run` per group membership: one group
  consumer (eager `range` assignor), one producer (transactional under exactly-once: one transaction for all tasks,
  transactional id `<applicationId>-<processId>`), a `TaskManager`, a `Scheduler`, a `RecordCollector` and a
  `CommitCoordinator`. A failed run is never revived: recovery builds a new one.
- **Callbacks, no poll loop**: consumer handlers only append records to task queues (pausing a partition at
  `bufferedRecordsPerPartition`). The scheduler runs task slices concurrently (records of one task strictly one at a
  time), punctuates, and commits at barriers: `Scheduler.exclusive()` starts once every slice settled and no slice
  starts while it runs. Never await `consumer.stop()` (or anything awaiting the client's run loop) inside `exclusive`.
- **Commits** (Kafka Streams' protocol): at-least-once flushes every send, commits offsets with the commit metadata
  (`consumer.commit()`), then writes checkpoints; exactly-once flushes, `sendOffsets` with the group metadata, commits
  the transaction, applies the buffered store writes (`state/transactional.ts`), then writes checkpoints. Triggers:
  `commitIntervalMs` (5000 at-least-once, 100 exactly-once), revocation (inside the awaited revocation hook), close,
  `context.commit()`. Timer commits are skipped while a rebalance is pending.
- **Time**: partition time per input partition, task stream time, and a clock per windowed operator, join and
  suppression (`ObservedStreamTime`, keyed by store name). Partition times and these clocks are persisted in the
  offset commit metadata (Kafka Streams' `TopicPartitionMetadata` format) and seed the task when it starts again.
  Grace periods decide lateness; stores are clipped by their store clock and retention (`state/time-clipping.ts`).
- **State**: every task has its own instance of each store of its subtopology (provider name
  `<applicationId>.<storeName>.p<partition>`), stacked provider store -> exactly-once write buffer -> changelog
  logging -> clock/clipping; DSL stores are timestamped (`[int64BE timestamp][value]`). `StateDirectory` keeps
  instances open across task lifetimes (a re-assigned task catches up from its checkpoint); unowned instances are
  pruned while the run is RUNNING (`PruneTimer`): in-memory ones after 30 s, durable ones once unowned for
  `stateCleanupDelayMs` (KS `state.cleanup.delay.ms`; partitions an earlier start left on disk count from the start).
- **Restoration**: a restore job per task that is not RUNNING (at most `changelog.restoration.maxConcurrentTasks`),
  its partitions paused meanwhile: replay to the end offsets captured at the start, checkpoint trust rules (within the
  log and the store not empty, else wipe and rebuild), progress checkpoints, stall detection, open transactions
  waited out. `start()` resolves once every task of the first assignment is RUNNING.
- **Rebalances** (C4 hooks, all awaited): revocation commits and suspends; assignment resumes re-assigned tasks
  through the restore pipeline, creates new ones and closes the rest; loss closes tasks dirty. A member holding only
  some input partitions of a stateful task migrates.
- **Failures** (`runtime/errors.ts`, by error class): `corrupted` revives the affected tasks in place (a retriable
  at-least-once send failure, a changelog out of range); `migrate` closes every task dirty and joins again with a new
  run after a backoff (0.5 s doubling to 30 s): fencing, lost partitions, `CommitFailedError`, connection failures and
  other non-fatal Kafka errors, retriable send failures under exactly-once, at-least-once commit timeouts lasting
  `taskTimeoutMs`; `fatal` goes to the uncaught exception handler (`'shutdown'`: state ERROR; `'restart'`: a new
  run). Record failures go to the deserialization, processing and production handlers first (`'fail'` |
  `'continue'`); under exactly-once a send failure never reaches the production handler. Until every task of the
  first assignment is RUNNING, every failure but a corruption rejects `start()`; a corruption revives the task in place
  (a changelog out of range: its checkpoints are invalidated, its restore clears and rebuilds the stores).
- **Global tables**: a consumer per table outside the group (read_committed, no commits): wiped and loaded to the
  end offsets before processing, then followed; an out-of-range position reloads in place while processing waits; a
  row failure fails the start, or halts the app afterwards.
- **close({ timeoutMs })**: graceful (commit, clean close) unless the run failed; network awaits are cut 1 s before
  the deadline, so `close()` returns within its timeout even with the cluster unreachable.
- **Tracing**: `runtime/tracing.ts` publishes one `kafkats:flow:process` operation per processed record
  (`flowDiagnostics`); nothing is built without subscribers. `@kafkats/opentelemetry` subscribes by channel name.

Dependency rule: `dsl -> topology -> processor` (API types); `operators -> processor, state` (and `topology` types),
never `runtime`; `runtime -> topology, processor, state, operators` (through suppliers only); `app -> dsl, topology,
runtime`; `testing -> app/internal, runtime, state`. Nothing imports `node:async_hooks`.

## Architecture

```
src/
├── index.ts               # Public exports of @kafkats/flow; flowVersion is the package version
├── testing.ts             # @kafkats/flow/testing: TestDriver, mocks, helpers
├── state-kit.ts           # @kafkats/flow/state: what a state store provider for a new storage engine is built from
├── codec.ts, topic.ts     # Codecs re-exported from @kafkats/client; topic() with its codecs
├── state.ts               # Store and provider interfaces (KeyValueStore, WindowStore, SessionStore, timestamped stores)
├── changelog.ts           # Re-exports changelog/
├── api/                   # Public types only
│   ├── app.ts             # FlowConfig, FlowApp, StreamState, CloseOptions, TopologyDescription
│   ├── kstream.ts, ktable.ts, grouped.ts   # KStream, BranchedKStream, KTable, GlobalKTable, grouped and windowed streams
│   ├── options.ts         # Consumed, Produced, Grouped, Materialized, StreamJoined, Joined, Repartitioned, SuppressOptions, ...
│   ├── stores.ts          # Stores definitions (StoreDefinition, options)
│   ├── handlers.ts        # Exception handler types and ErrorHandlerContext
│   └── errors.ts          # FlowConfigError, TopologyError, StreamsError and its subclasses
├── app/
│   ├── flow.ts            # flow() and FlowAppImpl: DSL entry points, describe(), start/close through one FlowRuntime per start
│   ├── config.ts          # FlowConfig validation, defaults, removed keys, handler resolution
│   ├── lifecycle.ts       # StreamState, getError(), memoized start and close
│   └── internal.ts        # What the TestDriver needs of an app (topology, settings), behind a symbol
├── dsl/                   # The DSL: resolves options, codecs (format.ts), names (naming.ts) and repartitioning
│   ├── kstream.ts, ktable.ts, kgrouped-*.ts, *-windowed-kstream.ts, global-ktable.ts   # DSL objects
│   ├── context.ts         # What the DSL objects of one app share (builder, changelog defaults, window graces)
│   ├── windows.ts         # TimeWindows, SessionWindows, SlidingWindows, JoinWindows (+ deprecated factories)
│   ├── retention.ts       # Retention of window and session stores (minimums, Materialized.retention)
│   ├── branched.ts        # split() / BranchedKStream
│   ├── deprecation.ts     # One DeprecationWarning per process and deprecated API
│   └── ops/               # One file per operator family: stateless, to, repartition, grouping, aggregate,
│                          # windowed-aggregate, sliding-aggregate, table-source, table-ops, table-group-by, table-join,
│                          # stream-stream-join, stream-table-join, global-join, suppress, process
├── operators/             # Processor suppliers of the DSL (Kafka Streams' KStream*/KTable* processors)
│   ├── stateless.ts, fixed-key.ts                    # filter/map/..., processValues
│   ├── kstream-aggregate.ts, window-aggregate.ts, session-aggregate.ts, sliding-aggregate.ts, windowed-processor.ts
│   ├── emit-final.ts                                 # emit: 'on-window-close'
│   ├── ktable-*.ts, change.ts                        # table nodes, Change<V>, value getters, groupBy repartition map
│   ├── stream-stream-join.ts, join-buffer.ts, outer-join-store.ts, list-value.ts, stream-table-join.ts
│   ├── suppress.ts, suppress-buffer.ts, suppress-entry.ts
│   └── windowed.ts                                   # WindowedCodecs and the base-key partitioner
├── processor/             # Processor API and the per-task processor internals
│   ├── api.ts             # Processor, ProcessorContext, punctuators, TaskId, internal context
│   ├── record.ts          # StreamRecord, FixedKeyRecord
│   ├── context.ts, context-views.ts   # Task and processor contexts; restricted views (fixed key, transformers)
│   ├── node.ts            # ProcessorNode: processing, punctuation, the processing handler route
│   ├── source.ts, sink.ts, output.ts  # Source and sink nodes; TaskOutput port (implemented by RecordCollector)
│   ├── punctuation.ts, time-tracker.ts, metadata.ts   # Punctuation queues, processor clocks, commit metadata format
│   └── timestamps.ts      # TimestampExtractor and the built-in extractors
├── topology/
│   ├── builder.ts, nodes.ts, names.ts, built-topology.ts   # InternalTopologyBuilder, node defs, name rules, build result
│   ├── subtopologies.ts   # Union-find into subtopologies, numbering, source checks
│   ├── partition-counts.ts, internal-topics.ts   # Partition counts, co-partitioning, creation/validation of internal topics
│   ├── null-keys.ts       # Which repartition sinks keep null keys (relaxed joins downstream)
│   └── describe.ts        # describe(): Kafka Streams' TopologyDescription
├── runtime/
│   ├── runtime.ts         # FlowRuntime: start, failure handling (revive, migrate, halt), recovery backoff, stop
│   ├── run.ts             # Run: one group membership (consumer, producer, TaskManager, Scheduler, commits, purge)
│   ├── task-manager.ts    # Assignment -> tasks: create, resume, suspend, close, revive; rebalance hooks
│   ├── task.ts, task-topology.ts, task-offsets.ts, task-id.ts   # StreamTask, its processor graph, offsets, TaskId
│   ├── partition-group.ts, record-queue.ts, record-decoder.ts   # Queues, selection, readiness, decoding, extraction
│   ├── scheduler.ts       # Slices, punctuation, commit barriers, exclusive(), time slicing
│   ├── commit.ts          # CommitCoordinator: at-least-once and exactly-once commits
│   ├── record-collector.ts # Sends of every task, deferred acks, send failure classification, the open transaction
│   ├── restoration.ts, restore-pipelines.ts, start-positions.ts   # Restore jobs, pipelines, 'latest' pins
│   ├── global-tables.ts, global-reader.ts, global-store.ts        # Global tables
│   ├── assignment.ts, purge.ts, prune-timer.ts, partition-count-cache.ts, signals.ts
│   ├── errors.ts          # Failure classification and backoff constants
│   ├── tracing.ts         # kafkats:flow:process tracing channel
│   ├── config.ts          # Resolved internal config types
│   └── io.ts, kafka-io.ts, kafka-consumers.ts, kafka-producer.ts, kafka-admin.ts   # IO ports and their Kafka adapters
├── state/
│   ├── state-directory.ts, state-manager.ts, store-builders.ts   # Store instances across tasks, per-task stacks, Stores
│   ├── transactional.ts, held-deletes.ts, logging.ts, time-clipping.ts, timestamped.ts, handles.ts   # Store layers
│   ├── checkpoints.ts, bulk.ts   # Changelog checkpoints; bulk loads and wipes
│   ├── memory.ts          # inMemory() provider
│   └── ordered.ts, ordered-keys.ts   # Stores over a byte-ordered engine (LMDB, RocksDB providers)
├── changelog/
│   ├── config.ts          # ChangelogConfig, restoration options, changelog and repartition topic configs
│   ├── restorer.ts, replay.ts, writer.ts   # ChangelogRestorer (trust rules, replay), changelog writes
│   └── errors.ts          # ChangelogPartitionMismatchError, SourceTopicNotFoundError, InternalTopicNotFoundError,
│                          # CoPartitioningError, ChangelogDecodeError
└── testing/
    ├── driver.ts, loop.ts, test-io.ts   # TestDriver over the production TaskManager with deterministic ports
    ├── mocks.ts, helpers.ts, globals.ts, partitioner.ts, store-writes.ts
```

## Tests

- `tests/unit/`: per-component suites (`runtime/`, `state/`, `operators/`, `dsl/`, `topology/`, ...) and DSL
  semantics through the `TestDriver`. `tests/unit/support/` has the task harness and fake IO ports.
- `tests/integration/`: against a broker (`test-helpers.ts`: clients, topics, reading committed output, injected
  commit failures); every app owns its client. The log end of a transactionally written topic passes its last commit
  marker only after the broker wrote it: wait for it before comparing positions.
- `tests/chaos/`: the chaos harness (`kill-chaos.ts`, `broker-chaos.ts`, `close-chaos.ts`, `worker.ts`, `verdict.ts`,
  `brokers.sh`, `run-all.sh`). Never remove broker containers; `brokers.sh` only creates, starts or unpauses them.

## Testing Utilities (`@kafkats/flow/testing`)

```typescript
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
import { string, json } from '@kafkats/flow'

const driver = new TestDriver() // takes FlowConfig options (no client), commitEvery, partitions, initialWallClockTimeMs
driver
	.input('orders', { key: string(), value: json<Order>() })
	.filter((_, order) => order !== null && order.total > 100)
	.to('large-orders', { key: string(), value: json() })

await driver.run(async ({ send, output, advanceWallClockTime, getStateStore }) => {
	await send('orders', { total: 250 }, { keyValue: 'o1', keyCodec: string() }) // resolves once fully processed
	expect(output('large-orders', { value: json<Order>() })).toHaveLength(1)
})
```

The driver runs the production task runtime deterministically: records written to topics the topology reads are fed
back in, offsets are committed after every record by default, and the wall clock moves only with
`advanceWallClockTime()`. See `packages/docs/flow/testing.md`.
