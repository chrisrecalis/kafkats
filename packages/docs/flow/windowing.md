# Windowing

Windowing groups stream records by time for aggregations. kafkats/flow supports three window types.

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

## Sliding Windows

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

// Sum per window
windowed.reduce((sum, value) => sum + value.amount)

// Custom aggregation
windowed.aggregate(
	() => ({ count: 0, total: 0 }),
	(key, value, agg) => ({
		count: agg.count + 1,
		total: agg.total + value.amount,
	})
)
```

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
	.map((windowedKey, count) => ({
		key: windowedKey.key,
		value: {
			key: windowedKey.key,
			windowStart: new Date(windowedKey.window.start),
			windowEnd: new Date(windowedKey.window.end),
			count,
		},
	}))
```

## Grace Periods

Handle late-arriving data:

```typescript
// Accept events up to 10 minutes late
TimeWindows.of('5m').gracePeriod('10m')
```

Without grace period, late events are dropped. With grace period:

- Window remains open longer
- Late events are included in aggregation
- Higher memory usage

## Window Comparison

| Type            | Size    | Overlap    | Use Case                         |
| --------------- | ------- | ---------- | -------------------------------- |
| Time (Tumbling) | Fixed   | No         | Regular metrics, hourly counts   |
| Time (Hopping)  | Fixed   | Yes        | Smoothed averages, sliding stats |
| Session         | Dynamic | No         | User sessions, activity tracking |
| Sliding         | Fixed   | Continuous | Event-relative windows           |

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
		})
	)
	.toStream()
	.map((windowedKey, agg) => ({
		key: windowedKey.key,
		value: {
			metricName: windowedKey.key,
			windowStart: new Date(windowedKey.window.start),
			windowEnd: new Date(windowedKey.window.end),
			sum: agg.sum,
			count: agg.count,
			average: agg.sum / agg.count,
		} as HourlyMetric,
	}))
	.to(hourlyMetricsTopic)
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
		})
	)
	.toStream()
	.map((windowedKey, agg) => ({
		key: windowedKey.key,
		value: {
			userId: windowedKey.key,
			sessionStart: new Date(windowedKey.window.start),
			sessionEnd: new Date(windowedKey.window.end),
			actionCount: agg.actions.length,
			actions: agg.actions,
		} as SessionSummary,
	}))
	.to(sessionsTopic)
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
