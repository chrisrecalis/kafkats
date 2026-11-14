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

| Option        | Type                                                                   | Description                             |
| ------------- | ---------------------------------------------------------------------- | --------------------------------------- |
| `key`         | `Codec<K>`                                                             | Encode keys                             |
| `value`       | `Codec<V>`                                                             | Encode values                           |
| `partitioner` | `(key: K \| null, value: V \| null, partitionCount: number) => number` | Choose a partition for produced records |

Without `key`/`value`, the codecs the stream carries are used. `mapValues()`, `flatMapValues()` and `map()` drop the value codec, and `map()` and `selectKey()` drop the key codec.

Records written with `to()` or `through()` keep the timestamp of the input record.

### Grouped (for `groupBy(...)`, `groupByKey(...)`)

| Option  | Type       | Description                                                                               |
| ------- | ---------- | ----------------------------------------------------------------------------------------- |
| `key`   | `Codec<K>` | Key codec for grouping / repartitioning (required for `groupBy()`)                        |
| `value` | `Codec<V>` | Value codec for grouping / repartitioning                                                 |
| `name`  | `string`   | Name of the internal repartition topic, `<applicationId>-<name>-repartition`. Recommended |

### Materialized (for `toTable(...)` and stateful ops)

| Option      | Type                         | Description                                                  |
| ----------- | ---------------------------- | ------------------------------------------------------------ |
| `storeName` | `string`                     | Store name (and changelog topic name prefix)                 |
| `key`       | `Codec<K>`                   | Key codec for the state store                                |
| `value`     | `Codec<V>`                   | Value codec for the state store (required for `aggregate()`) |
| `changelog` | `boolean \| ChangelogConfig` | Changelog for the store (default: enabled)                   |

### Joined (for `join(...)`, `leftJoin(...)`, `outerJoin(...)`)

| Option       | Type                                              | Description                                                             |
| ------------ | ------------------------------------------------- | ----------------------------------------------------------------------- |
| `key`        | `Codec<K>`                                        | Key codec                                                               |
| `value`      | `Codec<V1>`                                       | Left-side value codec                                                   |
| `otherValue` | `Codec<V2>`                                       | Right-side value codec                                                  |
| `within`     | `TimeWindows \| SessionWindows \| SlidingWindows` | Join window (required for stream-stream joins; `SlidingWindows` throws) |
| `changelog`  | `boolean \| ChangelogConfig`                      | Changelog for the join window stores (default: enabled)                 |

## Stateless Operations

These operations don't require state storage:

### map

Transform key and value:

```typescript
stream.map((key, value) => [newKey, newValue] as const)
```

### mapValues

Transform only value:

```typescript
stream.mapValues(value => transformedValue)
```

### selectKey

Change the key:

```typescript
stream.selectKey((value, key) => value.userId)
```

`selectKey()` and `map()` mark the stream as re-keyed. If it is followed by `groupByKey()`, a join or `toTable()`, it is [repartitioned](/flow/streams#repartitioning) through an internal topic, which needs key and value codecs.

### filter

Keep matching records:

```typescript
stream.filter((key, value) => condition)
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
// The new key needs a codec. Records are repartitioned through an internal topic
const grouped = stream.groupBy((key, value) => value.category, {
	key: string(),
	name: 'by-category', // stable topic name: <applicationId>-by-category-repartition
})
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

Reduce to single value (the aggregate has the value type; `KGroupedTable.reduce()` also takes a subtractor):

```typescript
grouped.reduce((agg, value) => agg + value)
```

### aggregate

Custom aggregation (`KGroupedTable.aggregate()` also takes a subtractor):

```typescript
grouped.aggregate(
	() => ({ sum: 0, count: 0 }), // initializer
	(key, value, agg) => ({
		// aggregator
		sum: agg.sum + value.amount,
		count: agg.count + 1,
	}),
	{ value: json<{ sum: number; count: number }>() } // codec for the aggregate (required)
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

Session windows also use `windowedBy()`:

```typescript
import { SessionWindows } from '@kafkats/flow'

grouped.windowedBy(SessionWindows.withInactivityGap('30m'))
```

## Join Operations

The inputs of a join must be [co-partitioned](/flow/state-stores#co-partitioning), otherwise `start()` throws `CoPartitioningError`. A [global table](/flow/tables#global-tables) can be joined from a stream without that requirement.

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
const merged = stream1.merge(stream2).merge(stream3)
```

## Output Operations

### to

Terminal - write to topic:

```typescript
stream.to('output-topic')

// With options
const customPartitioner = (key, _value, partitionCount) => {
	if (key === null) return 0
	return String(key).length % partitionCount
}

stream.to('output-topic', { partitioner: customPartitioner })
```

### through

Write to a topic and continue from what is read back from it. It is a real repartition point, and the topic must already exist:

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
	.selectKey(v => v.userId)
	.groupByKey({ key: string(), value: json<Valid>(), name: 'valid-by-user' })
	.windowedBy(TimeWindows.of('1h'))
	.count()
	.toStream()
	.map(
		(windowedKey, count) =>
			[windowedKey.key, { userId: windowedKey.key, count, window: windowedKey.window }] as const
	)
	.to('user-counts', { key: string(), value: json() })
```

## Operation Categories

| Category    | Operations                               | State Required                               |
| ----------- | ---------------------------------------- | -------------------------------------------- |
| Transform   | map, mapValues, selectKey, flatMapValues | No                                           |
| Filter      | filter                                   | No                                           |
| Side Effect | peek                                     | No                                           |
| Group       | groupByKey, groupBy                      | Repartition topic after a re-key / `groupBy` |
| Aggregate   | count, reduce, aggregate                 | Yes                                          |
| Window      | windowedBy                               | Yes                                          |
| Join        | join, leftJoin, outerJoin                | Yes                                          |
| Branch      | branch, merge                            | No                                           |
| Output      | to, through                              | No                                           |
| Convert     | toTable, toStream                        | Maybe                                        |
