# Configuration

Everything an application is configured with is passed to `flow()`. The config is validated at once: an invalid value, a consumer setting flow owns, or an option that was removed throws `FlowConfigError` from `flow()`.

```typescript
import { flow, TimestampExtractors } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

const app = flow({
	applicationId: 'orders-enricher',
	client: { clientId: 'orders-enricher', brokers: ['kafka-1:9092', 'kafka-2:9092'] },
	processingGuarantee: 'exactly_once',
	stateStoreProvider: lmdb({ stateDir: '/var/lib/orders-enricher' }),
	consumer: { autoOffsetReset: 'earliest', sessionTimeoutMs: 45_000 },
	defaultTimestampExtractor: TimestampExtractors.logAndSkipOnInvalid,
	deserializationExceptionHandler: 'continue',
	changelog: { replicationFactor: 3, topicConfigs: { 'min.insync.replicas': '2' } },
})
```

## Options

| Option                            | Default                                      | Description                                                                                                                                                                               |
| --------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `applicationId`                   | required                                     | The consumer group id and the prefix of internal topics, state and the transactional id. Shared by the instances of one application.                                                      |
| `client`                          | required                                     | A `KafkaClient`, or its config. A client the app creates is disconnected by `close()`; yours is not.                                                                                      |
| `processingGuarantee`             | `'at_least_once'`                            | `'at_least_once'` or `'exactly_once'`. See [Fault Tolerance](/flow/fault-tolerance#processing-guarantees).                                                                                |
| `commitIntervalMs`                | `5000` (at-least-once), `100` (exactly-once) | How often offsets (and under exactly-once the transaction) are committed.                                                                                                                 |
| `taskTimeoutMs`                   | `300000`                                     | At-least-once: how long interval commits may keep timing out (or meeting rebalances) before the instance leaves and rejoins the group.                                                    |
| `stateStoreProvider`              | `inMemory()`                                 | Where state stores live: `inMemory()`, `lmdb()` or `rocksdb()`. See [State Stores](/flow/state-stores).                                                                                   |
| `consumer`                        | -                                            | Settings of the group consumer. See [Consumer settings](#consumer-settings).                                                                                                              |
| `producer`                        | -                                            | Settings of the producer. See [Producer settings](#producer-settings).                                                                                                                    |
| `startTimeoutMs`                  | `120000`                                     | How long `start()` waits for the first assignment. Restoration and the loading of global tables are not bounded by it.                                                                    |
| `maxTaskIdleMs`                   | `0`                                          | How long a task with several input partitions waits for data on all of them before it processes out of timestamp order. `-1` never waits. See [Stream time](/flow/windowing#stream-time). |
| `bufferedRecordsPerPartition`     | `1000`                                       | Records buffered per input partition before the partition is paused.                                                                                                                      |
| `repartitionPurgeIntervalMs`      | `30000`                                      | How often records of repartition topics below the committed offsets are deleted. `0` disables purging.                                                                                    |
| `defaultTimestampExtractor`       | `TimestampExtractors.failOnInvalid`          | The timestamp of each consumed record. Per source: `Consumed.timestampExtractor`. See [Timestamps](/flow/windowing#timestamps).                                                           |
| `deserializationExceptionHandler` | `'fail'`                                     | What a record that cannot be decoded does. See [Error Handling](/flow/error-handling).                                                                                                    |
| `processingExceptionHandler`      | `'fail'`                                     | What an error thrown by a processor does.                                                                                                                                                 |
| `productionExceptionHandler`      | `'fail'`                                     | What a record that cannot be encoded or sent does.                                                                                                                                        |
| `uncaughtExceptionHandler`        | `'shutdown'`                                 | What a fatal error does: `'shutdown'` (state `ERROR`), `'restart'`, or a function choosing one.                                                                                           |
| `changelog`                       | -                                            | Internal topics and restoration. See [Changelog settings](#changelog-settings).                                                                                                           |

All durations are milliseconds. Numbers must be integers (`maxTaskIdleMs` may be `-1`; `taskTimeoutMs` and `repartitionPurgeIntervalMs` may be `0`).

## Consumer Settings

`consumer` takes the [consumer config](/client/consumer) of `@kafkats/client`, with these rules:

- The group id is the `applicationId`; `consumer.groupId` cannot be set.
- Flow handles every rebalance itself: `onBeforeRebalance`, `onPartitionsAssigned`, `onPartitionsRevoked` and `onPartitionsLost` cannot be set.
- `partitionAssignmentStrategy` must be `'range'` if given. Range assigns partition N of every topic to the same member, which keeps the input partitions of a task together.
- `autoOffsetReset` is the default reset policy of the sources: `'earliest'` (default), `'latest'` or `'none'`. A source can set its own with `Consumed.offsetReset`; repartition and `through()` topics always start from `'earliest'`. See [Offset reset](/flow/fault-tolerance#offset-reset).
- `maxWaitMs` defaults to `500`, so that an idle partition reports its end soon and tasks waiting on it learn their lag.
- Under `exactly_once`, `isolationLevel` is always `'read_committed'`.
- Offsets are committed by flow (every `commitIntervalMs`, at revocation and at close), not by the consumer's automatic commits.

## Producer Settings

`producer` takes the [producer config](/client/producer) of `@kafkats/client`. Under `exactly_once`:

- `transactionalId` defaults to `<applicationId>-<processId>`, where the process id is a random UUID created by every `start()`. A `transactionalId` you set is used as it is: it must then be unique among the running instances.
- `retries` defaults to `10`; the producer is idempotent and runs one transaction at a time.
- `transactionTimeoutMs` defaults to `10000` and must not be below `commitIntervalMs`, since a transaction stays open for a commit interval.

## Changelog Settings

| Option                                       | Default                          | Description                                                                                                |
| -------------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `changelog.replicationFactor`                | broker default                   | Replication factor of changelog and repartition topics whose store sets none.                              |
| `changelog.topicConfigs`                     | -                                | Topic configs of every changelog and repartition topic, applied over the defaults and below a store's own. |
| `changelog.autoCreate`                       | `true`                           | Whether missing internal topics are created. `false` only validates them: a missing one fails `start()`.   |
| `changelog.restoration.idleTimeoutMs`        | `30000`                          | A restore that makes no progress for this long fails.                                                      |
| `changelog.restoration.initialIdleTimeoutMs` | `idleTimeoutMs`                  | The same bound before the first record of a restore arrived.                                               |
| `changelog.restoration.checkIntervalMs`      | `1000`                           | How often restore progress is checked.                                                                     |
| `changelog.restoration.consumerMaxWaitMs`    | `consumer.maxWaitMs`, else `500` | `maxWaitMs` of restore consumers.                                                                          |
| `changelog.restoration.maxConcurrentTasks`   | `4`                              | How many tasks restore at once (one restore consumer each).                                                |

Per-store changelog settings (topic name, configs, replication factor, disabling the changelog, skipping restoration) go in `Materialized.changelog` or a store definition's `logging`. See [Changelog topics](/flow/state-stores#changelog-topics).

## Removed Options

These options of earlier versions throw `FlowConfigError` naming their replacement:

| Removed                                                     | Replacement                                                                                                                                                                                  |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `numStreamThreads`                                          | Run more instances (processes) of the application. One instance already processes its tasks concurrently.                                                                                    |
| `runEach`, `consumerRun`                                    | `commitIntervalMs` sets the commit interval; tasks process concurrently, so there is no partition concurrency setting.                                                                       |
| `restartOnError`                                            | `uncaughtExceptionHandler`. The default `'shutdown'` stops on fatal errors; `'restart'` rejoins after them. Recoverable failures (lost connections, fencing, failed commits) always recover. |
| `stateDir`                                                  | Pass the directory to the state store provider: `lmdb({ stateDir })`, `rocksdb({ stateDir })`.                                                                                               |
| `consumer.groupId`, rebalance hooks                         | Flow owns the group and its rebalances.                                                                                                                                                      |
| `consumer.partitionAssignmentStrategy` other than `'range'` | Range is required.                                                                                                                                                                           |

## Per-Operator Options

Codecs, names, materialization, windows and join settings are options of the operators, not of the app. See [Operations](/flow/operations) for every options object.
