# Aggregations

Aggregations fold the records of each key into a table: `count()`, `reduce()` and `aggregate()`, on grouped streams, windowed streams and grouped tables. Every aggregation keeps its results in a state store backed by a changelog topic, and forwards every update of a result downstream.

## Grouping First

```typescript
// By the current key: repartitions only if keys changed upstream
const byUser = clicks.groupByKey()

// By a new key: always repartitions; give the new key's codec
const byPage = clicks.groupBy((userId, click) => click!.page, { key: string(), name: 'clicks-by-page' })
```

`Grouped.name` names the repartition topic `<applicationId>-<name>-repartition`. All aggregations built from one named grouping share it; aggregations of an unnamed grouping each get their own, named after their `storeName` if they have one. See [Repartitioning](/flow/streams#repartitioning).

Aggregations of a stream drop records with a null key. Non-windowed and sliding window aggregations also drop records with a null value; time and session window aggregations pass them on (see [Null values](/flow/windowing#null-values)).

## count

```typescript
const clicksPerUser = byUser.count()
const named = byUser.count({ name: 'count-clicks', storeName: 'clicks-per-user' })
```

Counts are numbers, stored and written as 8-byte big-endian integers (Kafka Streams' `Serdes.Long`), so Java applications read them as longs. Pass `value` for another encoding:

```typescript
byUser.count({ value: json<number>() })
```

## reduce

Combines the values of a key, starting with the first one. The result has the value type and its codec:

```typescript
const maxAmount = orders.groupByKey().reduce((max, order) => (order.amount > max.amount ? order : max))
```

## aggregate

Folds the values into an aggregate of any type, starting from an initializer. An aggregate of another type than the values needs its codec:

```typescript
interface Stats {
	count: number
	total: number
}

const stats = orders.groupByKey().aggregate(
	(): Stats => ({ count: 0, total: 0 }),
	(customerId, order, stats) => ({ count: stats.count + 1, total: stats.total + order.amount }),
	{ storeName: 'order-stats', value: json<Stats>() }
)
```

Return a new aggregate instead of modifying the one you are given.

## Result Timestamps

Every result carries the largest timestamp of the records aggregated into it: `max(record timestamp, previous result's timestamp)`, as in Kafka Streams. Windowed results: see [Result timestamps](/flow/windowing#result-timestamps).

## Windowed Aggregations

`windowedBy()` aggregates per window. The result is a table keyed by `Windowed<K>`:

```typescript
import { TimeWindows, SessionWindows, SlidingWindows } from '@kafkats/flow'

const perMinute = byUser.windowedBy(TimeWindows.ofSizeAndGrace('1m', '10s')).count()

const sessions = byUser
	.windowedBy(SessionWindows.ofInactivityGapWithNoGrace('30m'))
	.aggregate(
		() => 0,
		(userId, click, clicks) => clicks + 1,
		(userId, a, b) => a + b, // merges the aggregates of sessions a record joins together
		{ value: json<number>() }
	)

const bursts = byUser.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace('10s')).count()
```

- Session `aggregate()` takes a **merger** as its third argument. Session `reduce()` merges sessions with its reducer, and `count()` adds their counts.
- Windowed aggregations take `retention` (how long windows are kept) and `emit: 'on-window-close'` (only final results). See [Windowing](/flow/windowing).
- `suppress({ until: 'window-close' })` turns any windowed table into final results. See [Suppression](/flow/suppress).

## Table Aggregations

`KTable.groupBy()` regroups the rows of a table by a new key. Every update of a row **subtracts** its old value from the group it leaves and **adds** its new value to the group it joins, so aggregations of a grouped table take an adder and a subtractor:

```typescript
interface Employee {
	department: string
	salary: number
}

const employees = app.table('employees', { key: string(), value: json<Employee>() })

const payroll = employees
	.groupBy((id, e) => [e.department, e.salary] as const, { key: string(), value: json<number>(), name: 'by-department' })
	.aggregate(
		() => 0,
		(department, salary, total) => total + salary, // adder
		(department, salary, total) => total - salary, // subtractor
		{ storeName: 'payroll', value: json<number>() }
	)

const headcount = employees
	.groupBy((id, e) => [e.department, e] as const, { key: string(), value: json<Employee>() })
	.count()
```

- The mapper may change the value type; give `Grouped.value` for the new values (the table's value codec otherwise), and always `Grouped.key`.
- A row that stays in its group sends one update; one that moves sends a subtraction to the old group, then an addition to the new one. A deleted row is subtracted from its group.
- `KGroupedTable.aggregate()` always needs `value`, the aggregate's codec. `reduce(adder, subtractor)` keeps the value type.
- When a group empties, its aggregate stays: a count goes to 0, a sum to whatever the subtractor returns. No tombstone is sent (Kafka Streams' behaviour). Filter the results downstream if empty groups should disappear:

```typescript
headcount.filter((department, count) => count > 0)
```

## Materialization

Every aggregation has a store, a timestamped key-value store (window and session stores for windowed aggregations) named by `storeName` or generated (`count-store-<n>`, `reduce-store-<n>`, `aggregate-store-<n>`, `window-count-store-<n>`, `table-count-store-<n>`, ...). The store is backed by `<applicationId>-<storeName>-changelog` unless `changelog: false`. Name the stores of an application you will keep changing: a generated name can shift when operators are added.

```typescript
byUser.count({
	storeName: 'clicks-per-user',
	changelog: { replicationFactor: 3, topicConfigs: { 'min.insync.replicas': '2' } },
})
```

## Delivery Guarantees

Aggregations read their current value, update it and write it back for every record. Under at-least-once, the records processed since the last commit are processed again after a failure, and a replayed record is aggregated twice (counted twice, added twice). Under exactly-once, the store writes, their changelog records, the results and the input offsets commit together. See [Fault Tolerance](/flow/fault-tolerance).

## Results as a Stream

```typescript
clicksPerUser.toStream().to('clicks-per-user')
```

Every update of the table is a record. A deleted result is a record with a null value.

## Example: Running Totals per Customer

```typescript
interface Order {
	customerId: string
	amount: number
}

app.stream('orders', { key: string(), value: json<Order>() })
	.groupBy((orderId, order) => order!.customerId, { key: string(), name: 'orders-by-customer' })
	.aggregate(
		() => ({ orders: 0, total: 0 }),
		(customerId, order, totals) => ({ orders: totals.orders + 1, total: totals.total + order.amount }),
		{ storeName: 'customer-totals', value: json<{ orders: number; total: number }>() }
	)
	.toStream()
	.to('customer-totals')
```
