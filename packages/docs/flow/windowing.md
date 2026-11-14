# Windowing

Windowing groups stream records by time for aggregations. kafkats/flow supports time windows (tumbling and hopping) and session windows. Sliding windows are not implemented yet.

## Time Windows

Fixed-size, non-overlapping time buckets:

```typescript
import { flow, topic, TimeWindows } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

// 5-minute tumbling windows
app.stream(clicks).groupByKey().windowedBy(TimeWindows.of('5m')).count()
```

### Window Duration Syntax

| Format    | Duration           |
| --------- | ------------------ |
| `'100ms'` | 100 milliseconds   |
| `'5s'`    | 5 seconds          |
| `'5m'`    | 5 minutes          |
| `'1h'`    | 1 hour             |
| `'1d'`    | 1 day              |
| `300000`  | 300000 ms (number) |

### Hopping Windows

Overlapping windows with custom advance:

```typescript
// 5-minute windows, advancing every 1 minute
TimeWindows.of('5m').advanceBy('1m')
```

```
Window 1: [0:00 - 0:05)
Window 2: [0:01 - 0:06)
Window 3: [0:02 - 0:07)
...
```

## Session Windows

Dynamic windows based on activity gaps:

```typescript
import { SessionWindows } from '@kafkats/flow'

// Session closes after 30 minutes of inactivity
app.stream(userActivity).groupByKey().windowedBy(SessionWindows.withInactivityGap('30m')).count()
```

Sessions:

- Start with first event for a key
- Extend with each new event
- Close after inactivity gap elapses

When a record bridges several sessions, they are merged in time order. `reduce()` always calls the reducer as `(aggregate, value)`. `aggregate()` on session windows needs a `merger` option that combines the aggregates of two sessions when they merge: `aggregate(init, agg, { value, merger: (a, b) => ... })`.

## Sliding Windows

::: warning
`SlidingWindows` is not implemented yet. `windowedBy(SlidingWindows.of(...))` throws. Use hopping `TimeWindows` (`TimeWindows.of(size).advanceBy(advance)`) instead.
:::

Continuous, overlapping windows:

```typescript
import { SlidingWindows } from '@kafkats/flow'

// Look back 5 minutes from each event
app.stream(events).groupByKey().windowedBy(SlidingWindows.of('5m')).count()
```

## Window Operations

Once windowed, use aggregation operations:

```typescript
const windowed = stream.groupByKey().windowedBy(TimeWindows.of('1h'))

// Count per window
windowed.count()

// Combine values per window (the aggregate has the value type)
windowed.reduce((latest, value) => (value.amount > latest.amount ? value : latest))

// Custom aggregation (needs a value codec for the aggregate)
windowed.aggregate(
	() => ({ count: 0, total: 0 }),
	(key, value, agg) => ({
		count: agg.count + 1,
		total: agg.total + value.amount,
	}),
	{ value: json<{ count: number; total: number }>() }
)
```

## Stream Time and Late Records

Windows are driven by event time, not wall-clock time. **Stream time is tracked per source partition**: it is the highest record timestamp seen on that partition. Partitions progress independently, so a partition that is far ahead does not make in-order records on another partition late.

Stream time is used to:

- **Drop late records**: a record is dropped when its window (or session) is already past retention, which is 24 times the window size (or session gap) behind stream time. A record for a window that is still retained is applied normally, and a late session record that can merge into a live session is kept.
- **Expire old windows**: windows and sessions past retention are removed from the store.

Windowed state is partitioned like all other state, see [Partitioned State](/flow/state-stores#partitioned-state).

## Windowed Keys

Windowed aggregations produce `Windowed<K>` keys:

```typescript
interface Windowed<K> {
	key: K
	window: {
		start: number // Window start timestamp (ms)
		end: number // Window end timestamp (ms)
	}
}
```

Access window boundaries in transformations:

```typescript
stream
	.groupByKey()
	.windowedBy(TimeWindows.of('5m'))
	.count()
	.toStream()
	.map(
		(windowedKey, count) =>
			[
				windowedKey.key,
				{
					key: windowedKey.key,
					windowStart: new Date(windowedKey.window.start),
					windowEnd: new Date(windowedKey.window.end),
					count,
				},
			] as const
	)
```

## Grace Periods

`TimeWindows` and `SessionWindows` have no configurable grace period. A late record is still applied as long as its window (or session) is retained, which is 24 times the window size (or session gap) behind stream time; see [Stream Time and Late Records](#stream-time-and-late-records).

## Window Comparison

| Type            | Size    | Overlap    | Use Case                         |
| --------------- | ------- | ---------- | -------------------------------- |
| Time (Tumbling) | Fixed   | No         | Regular metrics, hourly counts   |
| Time (Hopping)  | Fixed   | Yes        | Smoothed averages, sliding stats |
| Session         | Dynamic | No         | User sessions, activity tracking |
| Sliding         | Fixed   | Continuous | Not implemented yet              |

## Example: Hourly Metrics

```typescript
interface MetricEvent {
	metricName: string
	value: number
	timestamp: number
}

interface HourlyMetric {
	metricName: string
	windowStart: Date
	windowEnd: Date
	sum: number
	count: number
	average: number
}

app.stream<string, MetricEvent>(metricsTopic)
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.aggregate(
		() => ({ sum: 0, count: 0 }),
		(key, value, agg) => ({
			sum: agg.sum + value.value,
			count: agg.count + 1,
		}),
		{ value: json<{ sum: number; count: number }>() }
	)
	.toStream()
	.map(
		(windowedKey, agg) =>
			[
				windowedKey.key,
				{
					metricName: windowedKey.key,
					windowStart: new Date(windowedKey.window.start),
					windowEnd: new Date(windowedKey.window.end),
					sum: agg.sum,
					count: agg.count,
					average: agg.sum / agg.count,
				} as HourlyMetric,
			] as const
	)
	.to('hourly-metrics', { key: string(), value: json<HourlyMetric>() })
```

## Example: User Sessions

```typescript
interface UserAction {
	userId: string
	action: string
	timestamp: number
}

interface SessionSummary {
	userId: string
	sessionStart: Date
	sessionEnd: Date
	actionCount: number
	actions: string[]
}

app.stream<string, UserAction>(actionsTopic)
	.groupByKey()
	.windowedBy(SessionWindows.withInactivityGap('30m'))
	.aggregate(
		() => ({ actions: [] as string[] }),
		(key, action, agg) => ({
			actions: [...agg.actions, action.action],
		}),
		{
			value: json<{ actions: string[] }>(),
			// Required for session windows: combines two sessions that merge
			merger: (a, b) => ({ actions: [...a.actions, ...b.actions] }),
		}
	)
	.toStream()
	// A session that grows or merges replaces the old one, which is emitted with a null value
	.filter((_, agg) => agg !== null)
	.map(
		(windowedKey, agg) =>
			[
				windowedKey.key,
				{
					userId: windowedKey.key,
					sessionStart: new Date(windowedKey.window.start),
					sessionEnd: new Date(windowedKey.window.end),
					actionCount: agg.actions.length,
					actions: agg.actions,
				} as SessionSummary,
			] as const
	)
	.to('user-sessions', { key: string(), value: json<SessionSummary>() })
```

## State Management

Windowed operations require state stores. Configure via `stateStoreProvider`:

```typescript
import { inMemory } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

// In-memory (default)
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: inMemory(),
})

// Persistent (LMDB)
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({ stateDir: './state' }),
})
```
