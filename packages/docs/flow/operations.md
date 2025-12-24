# Stream Operations

Complete reference of all KStream and KTable operations.

## Options Reference

Many operations accept an `options` object. These are the common ones:

### Consumed (for `app.stream(...)`, `app.table(...)`)

| Option        | Type                               | Description                                |
| ------------- | ---------------------------------- | ------------------------------------------ |
| `key`         | `Codec<K>`                         | Decode keys                                |
| `value`       | `Codec<V>`                         | Decode values                              |
| `offsetReset` | `'earliest' \| 'latest' \| 'none'` | What to do when no committed offset exists |

### Produced (for `to(...)`, `through(...)`)

| Option        | Type       | Description                                        |
| ------------- | ---------- | -------------------------------------------------- | --------------------------------------- |
| `key`         | `Codec<K>` | Encode keys                                        |
| `value`       | `Codec<V>` | Encode values                                      |
| `partitioner` | `(key: K   | null, value: V, partitionCount: number) => number` | Choose a partition for produced records |

### Grouped (for `groupBy(...)`, `groupByKey(...)`)

| Option  | Type       | Description                               |
| ------- | ---------- | ----------------------------------------- |
| `key`   | `Codec<K>` | Key codec for grouping / repartitioning   |
| `value` | `Codec<V>` | Value codec for grouping / repartitioning |

### Materialized (for `toTable(...)` and stateful ops)

| Option      | Type       | Description                                  |
| ----------- | ---------- | -------------------------------------------- |
| `storeName` | `string`   | Store name (and changelog topic name prefix) |
| `key`       | `Codec<K>` | Key codec for the state store                |
| `value`     | `Codec<V>` | Value codec for the state store              |

### Joined (for `join(...)`, `leftJoin(...)`, `outerJoin(...)`)

| Option       | Type                                              | Description                                    |
| ------------ | ------------------------------------------------- | ---------------------------------------------- |
| `key`        | `Codec<K>`                                        | Key codec                                      |
| `value`      | `Codec<V1>`                                       | Left-side value codec                          |
| `otherValue` | `Codec<V2>`                                       | Right-side value codec                         |
| `within`     | `TimeWindows \| SessionWindows \| SlidingWindows` | Join window (required for stream-stream joins) |

## Stateless Operations

These operations don't require state storage:

### map

Transform key and value:

```typescript
stream.map((key, value) => ({
	key: newKey,
	value: newValue,
}))
```

### mapValues

Transform only value:

```typescript
stream.mapValues(value => transformedValue)

// With key access
stream.mapValues((value, key) => ({ ...value, originalKey: key }))
```

### selectKey

Change the key:

```typescript
stream.selectKey((key, value) => value.userId)
```

### filter

Keep matching records:

```typescript
stream.filter((key, value) => condition)
```

### filterNot

Remove matching records:

```typescript
stream.filterNot((key, value) => condition)
```

### flatMap

One-to-many transformation:

```typescript
stream.flatMap((key, value) => [
	{ key: k1, value: v1 },
	{ key: k2, value: v2 },
])
```

### flatMapValues

One-to-many value transformation:

```typescript
stream.flatMapValues(value => [v1, v2, v3])
```

### peek

Side effects without transformation:

```typescript
stream.peek((key, value) => {
	console.log(key, value)
})
```

## Grouping Operations

### groupByKey

Group by existing key:

```typescript
const grouped = stream.groupByKey()
```

### groupBy

Group by computed key:

```typescript
const grouped = stream.groupBy((key, value) => value.category)

// With options
const grouped = stream.groupBy((key, value) => value.category, { key: string() })
```

## Aggregation Operations

Available on KGroupedStream and KGroupedTable:

### count

Count records per key:

```typescript
grouped.count()
// Returns KTable<K, number>
```

### reduce

Reduce to single value:

```typescript
grouped.reduce((agg, value) => agg + value.amount)
```

### aggregate

Custom aggregation:

```typescript
grouped.aggregate(
	() => ({ sum: 0, count: 0 }), // initializer
	(key, value, agg) => ({
		// aggregator
		sum: agg.sum + value.amount,
		count: agg.count + 1,
	})
)
```

## Windowed Operations

### windowedBy

Apply time window to grouped stream:

```typescript
import { TimeWindows } from '@kafkats/flow'

grouped.windowedBy(TimeWindows.of('5m'))
// Returns WindowedKGroupedStream
```

### sessionWindowedBy

Apply session window:

```typescript
import { SessionWindows } from '@kafkats/flow'

grouped.sessionWindowedBy(SessionWindows.withInactivityGap('30m'))
```

## Join Operations

### join (inner)

Inner join - both sides must have matching key:

```typescript
import { TimeWindows } from '@kafkats/flow'

// Stream-stream join
stream1.join(stream2, (v1, v2) => combined, {
	within: TimeWindows.of('5m'),
})

// Stream-table join
stream.join(table, (streamValue, tableValue) => combined)

// Table-table join
table1.join(table2, (v1, v2) => combined)
```

### leftJoin

Left join - keep all left records:

```typescript
stream.leftJoin(table, (streamValue, tableValue) => ({
	...streamValue,
	extra: tableValue?.field ?? 'default',
}))
```

### outerJoin

Outer join - keep all records from both:

```typescript
stream1.outerJoin(
	stream2,
	(v1, v2) => ({
		left: v1,
		right: v2,
	}),
	{ within: TimeWindows.of('5m') }
)
```

## Branching Operations

### branch

Split into multiple streams:

```typescript
const [premium, standard] = stream.branch(
	(key, value) => value.tier === 'premium',
	(key, value) => value.tier === 'standard'
)
```

### merge

Combine streams:

```typescript
const merged = stream1.merge(stream2, stream3)
```

## Output Operations

### to

Terminal - write to topic:

```typescript
stream.to(outputTopic)

// With options
const customPartitioner = (key, _value, partitionCount) => {
	if (key === null) return 0
	return String(key).length % partitionCount
}

stream.to(outputTopic, { partitioner: customPartitioner })
```

### through

Write and continue processing:

```typescript
stream
  .through(intermediateTopic)
  .filter(...)
  .to(finalTopic)
```

## Conversion Operations

### toTable

Convert stream to table:

```typescript
const table = stream.toTable()
```

### toStream

Convert table to stream:

```typescript
const stream = table.toStream()
```

## Chaining Operations

Operations can be chained fluently:

```typescript
app.stream(inputTopic)
	.filter((_, v) => v.valid)
	.mapValues(v => transform(v))
	.selectKey((_, v) => v.userId)
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.count()
	.toStream()
	.map((windowedKey, count) => ({
		key: windowedKey.key,
		value: { userId: windowedKey.key, count, window: windowedKey.window },
	}))
	.to(outputTopic)
```

## Operation Categories

| Category    | Operations                         | State Required |
| ----------- | ---------------------------------- | -------------- |
| Transform   | map, mapValues, selectKey, flatMap | No             |
| Filter      | filter, filterNot                  | No             |
| Side Effect | peek                               | No             |
| Group       | groupByKey, groupBy                | No             |
| Aggregate   | count, reduce, aggregate           | Yes            |
| Window      | windowedBy, sessionWindowedBy      | Yes            |
| Join        | join, leftJoin, outerJoin          | Yes            |
| Branch      | branch, merge                      | No             |
| Output      | to, through                        | No             |
| Convert     | toTable, toStream                  | Maybe          |
