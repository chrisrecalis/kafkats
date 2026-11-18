# Error Handling

Flow separates failures the way Kafka Streams does:

- **Record failures** in your data or your code (a record that cannot be decoded, a processor that throws, a record that cannot be encoded or sent) go to an **exception handler**, which answers `'fail'` or `'continue'`.
- **Infrastructure failures** (lost connections, fencing, failed commits, lost partitions, timeouts) are recovered from automatically. See [Fault Tolerance](/flow/fault-tolerance#failure-handling).
- **Fatal failures** (a handler answered `'fail'`, misconfiguration, authorization, a bug) go to the **uncaught exception handler**, which stops the app (`'shutdown'`, the default) or restarts it (`'restart'`).

```typescript
const app = flow({
	applicationId: 'orders',
	client,
	deserializationExceptionHandler: 'continue', // skip records that cannot be decoded
	processingExceptionHandler: (ctx, record, error) => {
		logger.warn('dropping a record', { node: ctx.processorNodeId, offset: ctx.offset, error })
		return 'continue'
	},
	productionExceptionHandler: 'fail',
	uncaughtExceptionHandler: error => (error instanceof MyTransientError ? 'restart' : 'shutdown'),
})
```

Each handler option takes `'fail'`, `'continue'` (for the uncaught handler `'shutdown'`, `'restart'`) or a function returning one. A function that throws, or returns anything else, counts as `'fail'` (`'shutdown'`).

## The Handler Context

Every record handler receives an `ErrorHandlerContext`:

| Field                             | Description                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `topic`, `partition`, `offset`    | The input record the task was processing (`null`, `-1`, `-1n` in punctuators) |
| `sourceRawKey`, `sourceRawValue`  | Its bytes as consumed                                                        |
| `timestamp`, `headers`            | Of the record at the failing node                                            |
| `processorNodeId`                 | The node that failed (`null` for send failures)                              |
| `taskId`                          | The task: `{ subtopology, partition, key }`                                  |

## Deserialization Exceptions

`deserializationExceptionHandler(ctx, message, error)`: a source's key or value codec threw on a consumed record. `message` is the raw record (`Buffer` key and value). `'continue'` skips the record (it is logged); `'fail'` (default) stops the app. Global tables use the same handler while they load and follow their topics.

## Processing Exceptions

`processingExceptionHandler(ctx, record, error)`: a processor threw while processing a record, or a punctuator threw (then `record` is `null`). This covers every DSL callback (predicates, mappers, joiners, aggregators, key selectors, topic name extractors) and your own processors. `'continue'` drops the record at the failing node: what earlier nodes did with it (records forwarded and sent, state written) stays. `'fail'` (default) stops the app.

Errors of the runtime itself (Kafka errors, task migration, topology errors, a misused `forward()`) never reach this handler.

## Production Exceptions

`productionExceptionHandler(ctx, record, error, origin)`, where `record` is the record as it was being written (`topic`, `key`, `value` bytes, `timestamp`, `headers`):

- `origin: 'serialization'`: a sink's codec threw, or a sink without a codec got a value that is not a `Buffer`. `'continue'` drops the record.
- `origin: 'send'`: under at-least-once, a send failed after the producer's retries with an error that is not retriable. `'continue'` drops the record (logged), and its offset is committed as processed.

Some send failures are never offered to the handler:

| Failure                                                                  | What happens                                                   |
| ------------------------------------------------------------------------ | -------------------------------------------------------------- |
| Authentication, authorization, invalid topic, oversized commit metadata  | Fatal: no answer can make the send succeed                      |
| Fenced producer                                                          | The instance rejoins the group (task migration)                 |
| Retriable error that outlived the producer's retries, at-least-once      | The affected tasks are revived in place and reprocess from their last commit |
| Any send failure under exactly-once                                      | Retriable: the instance rejoins; otherwise fatal (the transaction cannot commit) |
| A changelog record                                                       | `'continue'` counts as `'fail'`: a dropped changelog record would make the store and its changelog diverge |

## Uncaught Exceptions

`uncaughtExceptionHandler(error)` decides what a fatal failure does:

- `'shutdown'` (default): the app stops processing and moves to `ERROR`; `app.getError()` returns the error. Its tasks close without committing. Call `close()` to release the stores and the client.
- `'restart'`: the failed group membership is abandoned (tasks close dirty, the consumer leaves) and the instance joins again after a backoff, from the committed offsets, as after an infrastructure failure. A deterministic failure (the same record failing again) will repeat.

A failure of a global table always shuts down. Before `start()` resolved, every failure rejects `start()` instead (except a corrupted task, which is revived in place).

## Timestamps

A timestamp extractor that throws, or returns something other than an integer, is fatal. The default extractor (`TimestampExtractors.failOnInvalid`) treats a negative record timestamp as fatal (`InvalidTimestampError`); `TimestampExtractors.logAndSkipOnInvalid` drops such records instead. See [Timestamps](/flow/windowing#timestamps).

## Error Classes

All exported from `@kafkats/flow`:

| Error                              | Raised when                                                                                                     |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `FlowConfigError`                  | `flow()` got an invalid or removed option                                                                        |
| `TopologyError`                    | The topology is invalid: a missing codec, an illegal or duplicate name, an invalid window or option              |
| `StreamsError`                     | Base of the errors raised while the app runs; `taskId` names the task when there is one                          |
| `FailedProcessingError`            | A handler answered `'fail'` or threw; `cause` is the original error                                              |
| `InvalidTimestampError`            | A timestamp extractor returned an invalid timestamp                                                              |
| `NoOffsetResetError`               | A source with reset policy `'none'` has no committed offset, or its position is out of range                    |
| `AssignmentError`                  | An assigned topic is not part of the topology: instances run different topologies                                |
| `TaskMigratedError`                | Fencing, lost partitions, a failed commit, a partial assignment: the instance rejoins (recovered automatically)   |
| `TaskCorruptedError`               | The state or output of tasks is unusable: they are revived in place (recovered automatically)                    |
| `IllegalStateError`                | A closed store or context was used, or a task transition was invalid: a bug                                      |
| `CoPartitioningError`              | Topics that must be co-partitioned have different partition counts (`start()`)                                   |
| `ChangelogPartitionMismatchError`  | An existing internal topic has the wrong partition count, or a source topic of a stateful task grew              |
| `SourceTopicNotFoundError`         | A source topic, or an internal topic with `changelog.autoCreate: false`, does not exist (`start()`)              |
| `ChangelogDecodeError`             | A changelog record cannot be decoded with its store's codecs while restoring                                     |

`TaskMigratedError` and `TaskCorruptedError` are reported by `getError()` as the most recent recoverable error; they do not change `state()`.
