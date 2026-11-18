# Suppression

A table forwards every update. `KTable.suppress()` holds updates back in a buffer and forwards only the latest one of each key, either once its window closed (the **final result** of a windowed aggregation) or once a time limit passed (fewer, later updates). It is Kafka Streams' `suppress()`.

## Final Results of Windows

```typescript
import { TimeWindows } from '@kafkats/flow'

clicks
	.groupByKey()
	.windowedBy(TimeWindows.ofSizeAndGrace('1h', '5m'))
	.count()
	.suppress({ until: 'window-close' })
	.toStream()
	.to('hourly-clicks-final')
```

- Each window is forwarded once, with its final value, when stream time passed the window's end plus the grace period of the windowed aggregation upstream. A deleted window is not forwarded.
- `until: 'window-close'` needs a windowed aggregation upstream; flow finds its grace period when `suppress()` is called (through repartition topics too, and the largest one when several windowed aggregations lead to the table). Otherwise `suppress()` throws `TopologyError`.
- Like everything that depends on time in flow, windows close by stream time: a window's final result is forwarded only once a later record arrives. A key that receives no more records still has its windows closed by other keys' records in the same task.
- The buffer never emits early: a record that overfills it fails with a `StreamsError`, which goes to the [processing exception handler](/flow/error-handling#processing-exceptions); with the default (`'fail'`) the app stops. Bound the buffer (`maxRecords`, `maxBytes`) only if you prefer stopping to growing.

For time and session window aggregations, `emit: 'on-window-close'` gives the same final results without a buffer; see [Emit on window close](/flow/windowing#emit-on-window-close).

## Rate Limiting Updates

```typescript
latestPrices.suppress({ until: 'time-limit', timeLimit: '30s' })
```

A key's latest value is forwarded once `timeLimit` passed (in stream time) since the first update the buffer held for it. Later updates of a buffered key replace its value but keep its place, so a busy key is still forwarded every `timeLimit`. Deletes are forwarded like other updates.

When the buffer is full (more than `maxRecords` keys or `maxBytes` bytes), it forwards its oldest keys early by default (`whenFull: 'emit-early'`); with `whenFull: 'shutdown'` the record fails as for `'window-close'`.

## Options

| Option       | Description                                                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `until`      | `'window-close'` or `'time-limit'`                                                                                           |
| `timeLimit`  | `'time-limit'` only: how long after a key's first buffered update it is forwarded (a [duration](/flow/operations#durations)) |
| `maxRecords` | Most keys buffered. Default: unbounded                                                                                       |
| `maxBytes`   | Most bytes buffered (encoded keys and entries). Default: unbounded                                                           |
| `whenFull`   | `'time-limit'` only: `'emit-early'` (default) or `'shutdown'`. `'window-close'` never emits early                            |
| `name`       | Names the processor, and its buffer store `<name>-store` (else `KTABLE-SUPPRESS-STATE-STORE-<n>`)                            |

## Buffer State

The buffer is a state store of the task, written through to its changelog like any other store, so a task that moves or restarts continues with the same buffered keys, and under exactly-once the buffer commits with the transaction. The time a suppression waits for is measured by its own processor clock, which is committed with the offsets, so a restarted task forwards at the same moments the old one would have.

The buffer keeps the table's keys and values with the table's codecs, so the table must know both (an operator that changed them needs `materialized.key` / `materialized.value`); `suppress()` throws `TopologyError` otherwise.

## Old Values

When a forwarded update goes on to an operator that needs old values (a `KTable.groupBy()` aggregation subtracting them), flow sends the key's value before the buffered updates (what downstream last saw of the key) as the old value. Kafka Streams sends the old value of the latest buffered update, which downstream never saw; flow's choice keeps a subtractor downstream consistent. See [Differences from Kafka Streams](/flow/kafka-streams).

A table lookup of a suppressed table (a join) sees what downstream last saw: the key's value before its buffered updates.
