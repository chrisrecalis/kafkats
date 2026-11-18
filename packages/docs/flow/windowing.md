# Windowing

Windowed aggregations group the records of each key by time. Flow has Kafka Streams' four kinds of windows, with the same factories, grace periods, retention rules, late-record rules and result formats.

| Windows          | Shape                                    | Typical use                        |
| ---------------- | ---------------------------------------- | ---------------------------------- |
| `TimeWindows`    | Fixed size; tumbling or hopping          | Counts per minute, hourly metrics  |
| `SessionWindows` | Activity separated by gaps of inactivity | User sessions                      |
| `SlidingWindows` | Fixed size, sliding with the records     | "N events within 10 minutes"       |
| `JoinWindows`    | A time range around each record          | [Stream-stream joins](/flow/joins) |

```typescript
import { TimeWindows } from '@kafkats/flow'

const perMinute = clicks.groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace('1m')).count()
```

## Time Windows

Windows `[start, start + size)` whose starts are multiples of the advance.

```typescript
// Tumbling: 5-minute windows, no late records accepted
TimeWindows.ofSizeWithNoGrace('5m')

// Tumbling, accepting records up to 30 seconds after the window ended
TimeWindows.ofSizeAndGrace('5m', '30s')

// Hopping: 5-minute windows starting every minute (each record is in 5 windows)
TimeWindows.ofSizeAndGrace('5m', '30s').advanceBy('1m')
```

The size must be positive and the advance positive and at most the size; the factories throw `TopologyError` otherwise. Windows never start before 0.

## Session Windows

Sessions `[start, end]` of a key's records that are at most `gap` apart. A record that falls into the gap between two sessions merges them into one.

```typescript
import { SessionWindows } from '@kafkats/flow'

SessionWindows.ofInactivityGapWithNoGrace('30m')
SessionWindows.ofInactivityGapAndGrace('30m', '5m')
```

When a record extends a session or merges several, the sessions it replaces are retracted downstream (a record with a null value for each) before the new session is sent. Session `aggregate()` takes a merger that combines the aggregates of merged sessions; see [Aggregations](/flow/aggregations#windowed-aggregations).

## Sliding Windows

Windows `[start, start + timeDifference]` (both ends inclusive) that slide with the records: a key gets a window wherever a record enters or leaves the slide, so every distinct set of records at most `timeDifference` apart is aggregated once.

```typescript
import { SlidingWindows } from '@kafkats/flow'

// Payments of a card within any 10 minutes
payments.groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceAndGrace('10m', '1m')).count()
```

Sliding window aggregations drop records with a null value. An aggregator must return a new aggregate rather than modify the one it is given: an aggregate may be copied into a new window.

## Durations

Sizes, gaps, graces and advances are milliseconds or strings with a unit: `'500ms'`, `'30s'`, `'5m'`, `'1h'`, `'1d'`. See [Durations](/flow/operations#durations).

## Timestamps

Every record gets a timestamp when it is consumed, from the source's timestamp extractor (`Consumed.timestampExtractor`, else `FlowConfig.defaultTimestampExtractor`):

| Extractor                                       | Behaviour                                                                                                |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `TimestampExtractors.failOnInvalid` (default)   | The record's timestamp; a negative one stops the app (`InvalidTimestampError`)                           |
| `TimestampExtractors.logAndSkipOnInvalid`       | The record's timestamp; a record with a negative one is dropped                                          |
| `TimestampExtractors.usePartitionTimeOnInvalid` | The record's timestamp; a negative one is replaced by the partition's time (fatal while that is unknown) |
| `TimestampExtractors.wallClock`                 | The time the record is consumed (processing time)                                                        |

A custom extractor receives the decoded record (`topic`, `partition`, `offset`, `key`, `value`, `timestamp`, `headers`) and the partition's time (the largest timestamp seen on the partition, `-1` while unknown):

```typescript
app.stream('orders', {
	key: string(),
	value: json<Order>(),
	timestampExtractor: record => (record.value as Order | null)?.createdAt ?? record.timestamp,
})
```

A negative result drops the record; a result that is not an integer, or an extractor that throws, is a fatal error (see the [uncaught exception handler](/flow/error-handling#uncaught-exceptions)). Downstream, records keep their timestamps; processors set new ones explicitly (`record.withTimestamp()`).

## Stream Time

Windows close by **stream time**, which only moves with the timestamps of the records, never with the wall clock:

- Each input partition of a task has a partition time: the largest timestamp read from it.
- A task processes its records in timestamp order across its input partitions (the smallest head first), and its stream time is the largest timestamp it processed.
- Each windowed operator keeps its own clock: the largest timestamp of the records it saw. Its windows close by that clock.

Processor clocks are committed with the offsets (in the offset commit metadata, as Kafka Streams does), and restored when a task starts again elsewhere, after a rebalance or after a failure. A record is therefore late, or not, regardless of restarts.

A task with several input partitions (a join, a merge) waits for data on all of them before it picks the next record, so that records are processed in timestamp order (KIP-695): it waits for an empty partition while its lag is unknown or positive (records are on the broker), and for `maxTaskIdleMs` (default 0) once the partition is caught up; then it processes what it has. With `-1` it never waits.

## Grace Periods and Late Records

A window **closes** once its operator's stream time passed its end by the grace period. A record whose windows are all closed is **late** and dropped (and logged); stream time does not move back for it.

| Windows          | A record at `ts` is dropped for a window when                                            |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `TimeWindows`    | `window end <= streamTime - grace` (a hopping record may still update its newer windows) |
| `SessionWindows` | the merged session ends before `streamTime - grace - gap`                                |
| `SlidingWindows` | `ts + timeDifference < streamTime - grace` (the whole record)                            |
| `JoinWindows`    | see [Stream-stream joins](/flow/joins#stream-stream-joins)                               |

The grace period is a trade-off: a longer one accepts later records, keeps windows longer in the store, and delays final results.

## Retention

A windowed aggregation's store keeps a window for its retention, measured back from the newest window end it holds. The default, which is also the minimum, keeps what a record can still update:

| Windows          | Default and minimum retention |
| ---------------- | ----------------------------- |
| `TimeWindows`    | size + grace                  |
| `SessionWindows` | gap + grace                   |
| `SlidingWindows` | 2 × time difference + grace   |

`Materialized.retention` keeps windows longer (for lookups through joins, for example); a smaller value throws `TopologyError`:

```typescript
grouped.windowedBy(TimeWindows.ofSizeWithNoGrace('1m')).count({ storeName: 'counts', retention: '1h' })
```

Reads never return a window past its retention, whether or not it was deleted yet. Expired windows are deleted from the local store as the store's clock advances; the window changelog topics have `cleanup.policy=delete,compact` and a `retention.ms` of the store's retention plus one day, so the broker drops them too.

## Emit on Window Close

By default a windowed aggregation forwards every update of a window (`emit: 'on-update'`). With `emit: 'on-window-close'` it forwards only the **final result** of each window, once the window closed:

```typescript
clicks
	.groupByKey()
	.windowedBy(TimeWindows.ofSizeAndGrace('1h', '5m'))
	.count({ emit: 'on-window-close' })
	.toStream()
	.to('hourly-clicks')
```

Closed windows are looked for after records are processed, at most once per second of wall clock for each aggregation of each task: a window closes when a later record moves stream time past its end plus grace. Which windows were emitted is committed with the offsets, so each final result is sent once across restarts (under at-least-once a failure can send it again). This is Kafka Streams' `EmitStrategy.onWindowClose()`. [`suppress({ until: 'window-close' })`](/flow/suppress) gives the same results on any windowed table, through a buffer.

## Windowed Keys

Windowed aggregations produce a `KTable<Windowed<K>, V>`:

```typescript
interface Windowed<K> {
	key: K
	window: { start: number; end: number } // [start, end) for time windows; [start, end] for sessions and sliding windows
}
```

Written to a topic, windowed keys use Kafka Streams' formats, so Java applications can read them with `WindowedSerdes`:

| Windows                  | Key bytes                             | Codec                                         |
| ------------------------ | ------------------------------------- | --------------------------------------------- |
| Time and sliding windows | `[key][start int64 BE]`               | `WindowedCodecs.timeWindowed(keyCodec, size)` |
| Sessions                 | `[key][end int64 BE][start int64 BE]` | `WindowedCodecs.sessionWindowed(keyCodec)`    |

The time-windowed format does not carry the end: decoding computes it from the size (for sliding windows, the time difference). The results of a windowed aggregation know their codec, so `toStream().to(topic)` writes them without options. To read such a topic in another app:

```typescript
import { WindowedCodecs, string, json } from '@kafkats/flow'

// written by .aggregate(..., { value: json<number>() }) over TimeWindows of one hour
app.stream('hourly-revenue', { key: WindowedCodecs.timeWindowed(string(), 3_600_000), value: json<number>() })
```

Windowed records are written to the partition of their **base key**, both by `to()` and through repartition topics, so they stay in the partition where the key's other data is: key-based operations on windowed results need no repartitioning.

## Result Timestamps

| Aggregation     | Timestamp of a result                         |
| --------------- | --------------------------------------------- |
| Time windows    | The largest timestamp of the window's records |
| Session windows | The session's end                             |
| Sliding windows | The largest timestamp of the window's records |

## Null Values

Records with a null key are dropped by every aggregation. Time and session window aggregations pass records with a null value to the aggregator or reducer, and `count()` counts them, as Kafka Streams does; their callbacks type the value `V | null`. A `reduce()` whose first value of a window is null keeps no window for it. Sliding window aggregations drop null values.

## Deprecated Factories

The factories of earlier versions still work, with Kafka Streams' legacy behaviour, and warn once per process:

| Deprecated                                | Behaves as                                                     | Use instead                                                |
| ----------------------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------- |
| `TimeWindows.of(size)`                    | grace `max(24h - size, 0)`: windows accept records a day late  | `ofSizeWithNoGrace(size)` / `ofSizeAndGrace(size, grace)`  |
| `SessionWindows.withInactivityGap(gap)`   | grace `max(24h - gap, 0)`                                      | `ofInactivityGapWithNoGrace` / `ofInactivityGapAndGrace`   |
| `SlidingWindows.of(d)`, `.gracePeriod(g)` | the same windows                                               | `ofTimeDifferenceWithNoGrace` / `ofTimeDifferenceAndGrace` |
| `JoinWindows.of(d)`                       | grace `max(24h - 2d, 0)`, unmatched left/outer results eagerly | `ofTimeDifferenceWithNoGrace` / `ofTimeDifferenceAndGrace` |

The long legacy grace keeps about a day of windows in the store, and delays final results by as much. Switch to the new factories to bound both.

## Example: Hourly Revenue per Product

```typescript
interface Sale {
	productId: string
	amount: number
}

app.stream('sales', { key: string(), value: json<Sale>() })
	.groupBy((key, sale) => sale!.productId, { key: string(), name: 'sales-by-product' })
	.windowedBy(TimeWindows.ofSizeAndGrace('1h', '10m'))
	.aggregate(
		() => 0,
		(productId, sale, revenue) => revenue + (sale?.amount ?? 0),
		{ storeName: 'hourly-revenue', value: json<number>(), emit: 'on-window-close' }
	)
	.toStream()
	.map((windowed, revenue) => [windowed!.key, { hour: windowed!.window.start, revenue }] as const)
	.to('hourly-revenue', { key: string(), value: json() })
```

## Example: User Sessions

```typescript
interface Activity {
	page: string
}

interface Session {
	pages: number
	firstPage: string | null
}

app.stream('activity', { key: string(), value: json<Activity>() })
	.groupByKey()
	.windowedBy(SessionWindows.ofInactivityGapAndGrace('30m', '1m'))
	.aggregate(
		(): Session => ({ pages: 0, firstPage: null }),
		(userId, activity, session) => ({
			pages: session.pages + (activity === null ? 0 : 1),
			firstPage: session.firstPage ?? activity?.page ?? null,
		}),
		(userId, a, b) => ({ pages: a.pages + b.pages, firstPage: a.firstPage ?? b.firstPage }),
		{ storeName: 'sessions', value: json<Session>() }
	)
	.toStream()
	// merged-away sessions are retracted with null values
	.filter((windowed, session) => session !== null)
	.to('user-sessions')
```
