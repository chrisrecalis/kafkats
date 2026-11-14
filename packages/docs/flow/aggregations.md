# Aggregations

Aggregations combine multiple records into summary results. They're available on grouped streams and tables.

## Grouping First

Before aggregating, group records by key:

```typescript
// Group by existing key
const grouped = stream.groupByKey()

// Group by computed key (needs a key codec, and repartitions through an internal topic)
const grouped = stream.groupBy((key, value) => value.category, { key: string(), name: 'by-category' })
```

State is kept per partition, so all records of a key must be processed in one partition. `groupBy()` always writes the records through an internal topic named `<applicationId>-<name>-repartition`, and `groupByKey()` does the same after `selectKey()` or `map()`. Use `name` to give the topic a stable name. See [Repartitioning](/flow/streams#repartitioning).

Aggregation results are `KTable`s that can be joined with streams and with other tables.

## Count

Count records per key:

```typescript
const counts = stream.groupByKey().count()
// Returns KTable<K, number>
```

### Windowed Count

```typescript
import { TimeWindows } from '@kafkats/flow'

const hourlyCounts = stream.groupByKey().windowedBy(TimeWindows.of('1h')).count()
```

## Reduce

Combine values into one using a reducer. The aggregate has the same type as the values:

```typescript
// Sum amounts per key
const totals = stream
	.mapValues(value => value.amount)
	.groupByKey({ value: json<number>() })
	.reduce((sum, amount) => sum + amount)
```

The reducer receives:

- Previous aggregated value (or first value)
- Current value
- Returns new aggregated value

For session windows, existing sessions are merged in time order, and the reducer is always called as `(aggregate, value)`, so it also works for reducers that are not commutative.

## Aggregate

Custom aggregation with initializer. `aggregate()` needs a `value` codec for the aggregate in its options:

```typescript
interface Stats {
	count: number
	sum: number
	min: number
	max: number
}

const stats = stream.groupByKey().aggregate<Stats>(
	// Initializer - creates empty aggregate
	() => ({ count: 0, sum: 0, min: Infinity, max: -Infinity }),

	// Aggregator - combines value into aggregate
	(key, value, agg) => ({
		count: agg.count + 1,
		sum: agg.sum + value.amount,
		min: Math.min(agg.min, value.amount),
		max: Math.max(agg.max, value.amount),
	}),

	// Materialized options - codec for the aggregate
	{ value: json<Stats>() }
)
```

## Table Aggregations

KGroupedTable has different aggregation semantics.

::: warning
`KTable.groupBy()` writes each row's contribution through an internal repartition topic keyed by the grouped key (like `KStream.groupBy()`), so its aggregates are correct across instances and threads. Under `at_least_once`, a redelivered table update can be added or subtracted twice; use `exactly_once` when the aggregate must be exact.
:::

```typescript
// Return the new [key, value]; the new key needs a codec
const grouped = table.groupBy((key, value) => [value.category, value], { key: string() })

// Must provide both adder and subtractor
const counts = grouped.aggregate(
	() => 0,
	(key, value, agg) => agg + 1, // add
	(key, value, agg) => agg - 1, // subtract (when key changes/deleted)
	{ value: json<number>() }
)
```

## Materialization

Store aggregation results in a named store (its changelog topic is `<applicationId>-my-counts-store-changelog`):

```typescript
const counts = stream.groupByKey().count({
	storeName: 'my-counts-store',
})
```

`FlowApp` does not expose its state stores for querying. Write the results to a topic with `toStream().to()` instead.

## Delivery Guarantees

With the default `at_least_once`, a record can be processed again after a crash, so counts and sums can over-count. Use `processingGuarantee: 'exactly_once'` when that is not acceptable. See [Fault Tolerance](/flow/fault-tolerance).

## Converting Results

Aggregations return KTable. Convert to stream for output:

```typescript
stream.groupByKey().count().toStream().to('counts')
```

## Example: Real-time Analytics

```typescript
interface PageView {
	page: string
	userId: string
	timestamp: number
	duration: number
}

interface PageStats {
	page: string
	views: number
	uniqueUsers: Set<string>
	totalDuration: number
	avgDuration: number
}

app.stream<string, PageView>(pageViewsTopic)
	.groupBy((_, v) => v.page, { key: string(), name: 'page-views-by-page' })
	.windowedBy(TimeWindows.of('15m'))
	.aggregate<{ views: number; users: string[]; totalDuration: number }>(
		() => ({ views: 0, users: [], totalDuration: 0 }),
		(page, view, agg) => ({
			views: agg.views + 1,
			users: agg.users.includes(view.userId) ? agg.users : [...agg.users, view.userId],
			totalDuration: agg.totalDuration + view.duration,
		}),
		{ value: json<{ views: number; users: string[]; totalDuration: number }>() }
	)
	.toStream()
	.map(
		(windowedKey, agg) =>
			[
				windowedKey.key,
				{
					page: windowedKey.key,
					views: agg.views,
					uniqueUsers: agg.users.length,
					totalDuration: agg.totalDuration,
					avgDuration: agg.totalDuration / agg.views,
				},
			] as const
	)
	.to('page-stats', { key: string(), value: json() })
```

## Example: Running Totals

```typescript
interface Transaction {
	accountId: string
	amount: number
	type: 'credit' | 'debit'
}

interface AccountBalance {
	accountId: string
	balance: number
	transactionCount: number
}

app.stream<string, Transaction>(transactionsTopic)
	.groupByKey()
	.aggregate<AccountBalance>(
		() => ({ accountId: '', balance: 0, transactionCount: 0 }),
		(accountId, txn, agg) => ({
			accountId,
			balance: agg.balance + (txn.type === 'credit' ? txn.amount : -txn.amount),
			transactionCount: agg.transactionCount + 1,
		}),
		{ value: json<AccountBalance>() }
	)
	.toStream()
	.to('balances')
```

## Performance Considerations

1. **State size** - Aggregations maintain state per key
2. **Windowing** - Limits state by time
3. **Compaction** - Enable log compaction on output topics
4. **Serialization** - Use efficient codecs for aggregate values

```typescript
// Name the store
const counts = stream.groupByKey().count({ storeName: 'counts' })

// The store backend is configured for the whole app
const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	// Use LMDB for persistence
	stateStoreProvider: lmdb({ stateDir: './state' }),
})
```
