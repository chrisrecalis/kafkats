# KTable

A KTable represents a changelog stream where each key has a latest value. It's like a continuously-updated database table.

## Creating a Table

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
})

// From a topic
const usersTable = app.table(usersTopic)

// Global table (fully replicated to all instances)
const configTable = app.globalTable(configTopic)
```

## Table vs Stream

| Aspect     | KStream        | KTable               |
| ---------- | -------------- | -------------------- |
| Semantics  | Event log      | Latest value per key |
| Null value | Regular record | Delete (tombstone)   |
| Use case   | Events, logs   | State, lookups       |

## Transformation Operations

### mapValues

Transform table values:

```typescript
usersTable.mapValues(user => ({
	name: user.name,
	email: user.email,
}))
```

### filter

Keep only matching entries:

```typescript
usersTable.filter((key, user) => user.isActive)
```

When a row stops matching (for example the user becomes inactive), `filter()` emits a **tombstone** for that key, so downstream tables and aggregations drop the row. The function is never called for tombstones. Likewise, `mapValues()` never calls your function for a tombstone: a deletion stays a deletion.

The results of `filter()` and `mapValues()` are not materialized, so they cannot be used as the lookup side of a join. See [Known Limitations](/flow/fault-tolerance#known-limitations).

### filterNot

Remove matching entries:

```typescript
usersTable.filterNot((key, user) => user.deleted)
```

## Conversion

### toStream

Convert table to stream:

```typescript
const stream = usersTable.toStream()

// Now you can use stream operations
stream.mapValues(user => ({ event: 'user_updated', user })).to(userEventsTopic)
```

## Grouping

### groupBy

Group table by a new key:

```typescript
const byCountry = usersTable.groupBy((key, user) => [user.country, user], { key: string() })
// Returns KGroupedTable
```

::: info
`KTable.groupBy()` writes each row's contribution through an internal repartition topic keyed by the grouped key (like `KStream.groupBy()`), so its aggregates are correct across instances and threads. Under `at_least_once`, a redelivered table update can be added or subtracted twice; use `exactly_once` when the aggregate must be exact.
:::

## Joins

Both tables of a table-table join must be [co-partitioned](/flow/state-stores#co-partitioning): same partition count and same partitioning of keys. Otherwise `start()` throws `CoPartitioningError`. Tables created with `app.table()`, and tables from `count()`, `reduce()` and `aggregate()`, can be joined.

### join (inner join)

Join with another table:

```typescript
const ordersWithUsers = ordersTable.join(usersTable, (order, user) => ({
	orderId: order.id,
	userName: user.name,
	total: order.total,
}))
```

### leftJoin

Left join - keep all left records:

```typescript
const ordersWithOptionalUser = ordersTable.leftJoin(usersTable, (order, user) => ({
	orderId: order.id,
	userName: user?.name ?? 'Unknown',
	total: order.total,
}))
```

### outerJoin

Outer join - keep all records from both:

```typescript
const merged = table1.outerJoin(table2, (left, right) => ({
	fromLeft: left?.value,
	fromRight: right?.value,
}))
```

## Global Tables

A global table is fully replicated to every application instance. Unlike `app.table()`, it is not read through the consumer group:

- every instance reads **all partitions** of the topic itself
- the table is **fully loaded before processing starts**, then kept up to date by following the topic
- no changelog is written, because the topic is the source of truth
- there is no co-partitioning requirement, so the stream and the topic can have different partition counts

```typescript
// Define a config topic
const configTopic = topic('app-config', {
	key: string(),
	value: json<ConfigValue>(),
})

// Create global table
const configTable = app.globalTable(configTopic)

// Use as the lookup side of a stream join
app.stream(eventsTopic)
	.join(configTable, (event, config) => ({
		...event,
		settings: config,
	}))
	.to(enrichedEventsTopic)
```

A global table can **only** be used as the lookup side of a stream join (`join()` or `leftJoin()`). Any other operation (`toStream()`, `mapValues()`, `filter()`, `groupBy()`, table-table joins) throws.

The topic must exist when the app starts. If loading the table makes no progress for `changelog.restoration.idleTimeoutMs` (default 30 seconds), `start()` fails.

::: tip When to Use Global Tables

- Small, slowly-changing data (config, reference data)
- Data needed for every partition
- Lookup tables for enrichment
- Joining topics that are not co-partitioned
  :::

## Example: User Enrichment

```typescript
interface User {
	id: string
	name: string
	email: string
	tier: 'free' | 'premium'
}

interface Order {
	orderId: string
	userId: string
	items: string[]
	total: number
}

interface EnrichedOrder {
	orderId: string
	userId: string
	userName: string
	userTier: string
	items: string[]
	total: number
	discount: number
}

// Users table from compacted topic
const usersTable = app.table<string, User>(usersTopic)

// Orders stream
app.stream<string, Order>(ordersTopic)
	// Rekey by userId for join
	.selectKey((_, order) => order.userId)
	// Join with users. The re-keyed stream is repartitioned first, which needs the key codec
	.join(
		usersTable,
		(order, user) =>
			({
				orderId: order.orderId,
				userId: order.userId,
				userName: user.name,
				userTier: user.tier,
				items: order.items,
				total: order.total,
				discount: user.tier === 'premium' ? order.total * 0.1 : 0,
			}) as EnrichedOrder,
		{ key: string() }
	)
	.to(enrichedOrdersTopic)
```

## Table State

Tables maintain state internally. Access the underlying store:

```typescript
// Materialize table to a named store
const users = app.table(usersTopic, {
	materialized: { storeName: 'users-store' },
})

// Query the store (after app is running)
const store = app.getStore<string, User>('users-store')
const user = await store.get('user-123')
```

## Tombstones (Deletions)

In tables, a null value means "delete this key":

```typescript
// Delete a user by sending null
await producer.send(usersTopic, [{ key: 'user-123', value: null }])

// The key is removed from the table
```
