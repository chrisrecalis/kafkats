# Aggregations

Aggregations combine multiple records into summary results. They're available on grouped streams and tables.

## Grouping First

Before aggregating, group records by key:

```typescript
// Group by existing key
const grouped = stream.groupByKey()

// Group by computed key
const grouped = stream.groupBy((key, value) => value.category)
```

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

Combine values into one using a reducer:

```typescript
// Sum amounts per key
const totals = stream.groupByKey().reduce((sum, value) => sum + value.amount)
```

The reducer receives:

- Previous aggregated value (or first value)
- Current value
- Returns new aggregated value

## Aggregate

Custom aggregation with initializer:

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
	})
)
```

## Table Aggregations

KGroupedTable has different aggregation semantics:

```typescript
const grouped = table.groupBy((key, value) => value.category)

// Must provide both adder and subtractor
const counts = grouped.aggregate(
	() => 0,
	(key, value, agg) => agg + 1, // add
	(key, value, agg) => agg - 1 // subtract (when key changes/deleted)
)
```

## Materialization

Store aggregation results in a named store:

```typescript
const counts = stream.groupByKey().count({
	materialized: {
		storeName: 'my-counts-store',
	},
})

// Query the store later
const store = app.getStore<string, number>('my-counts-store')
const count = await store.get('some-key')
```

## Converting Results

Aggregations return KTable. Convert to stream for output:

```typescript
stream.groupByKey().count().toStream().to(countsTopic)
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
	.selectKey((_, v) => v.page)
	.groupByKey()
	.windowedBy(TimeWindows.of('15m'))
	.aggregate<{ views: number; users: string[]; totalDuration: number }>(
		() => ({ views: 0, users: [], totalDuration: 0 }),
		(page, view, agg) => ({
			views: agg.views + 1,
			users: agg.users.includes(view.userId) ? agg.users : [...agg.users, view.userId],
			totalDuration: agg.totalDuration + view.duration,
		})
	)
	.toStream()
	.mapValues((agg, windowedKey) => ({
		page: windowedKey.key,
		views: agg.views,
		uniqueUsers: agg.users.length,
		totalDuration: agg.totalDuration,
		avgDuration: agg.totalDuration / agg.views,
	}))
	.to(pageStatsTopic)
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
		})
	)
	.toStream()
	.to(balancesTopic)
```

## Performance Considerations

1. **State size** - Aggregations maintain state per key
2. **Windowing** - Limits state by time
3. **Compaction** - Enable log compaction on output topics
4. **Serialization** - Use efficient codecs for aggregate values

```typescript
// Configure state store for better performance
const counts = stream.groupByKey().count({
	materialized: {
		storeName: 'counts',
		// Use LMDB for persistence
		storeProvider: lmdb({ stateDir: './state' }),
	},
})
```
