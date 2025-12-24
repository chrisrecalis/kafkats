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
const byCountry = usersTable.groupBy((key, user) => user.country)
// Returns KGroupedTable
```

## Joins

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

Global tables are fully replicated to all application instances, useful for small lookup tables:

```typescript
// Define a config topic
const configTopic = topic('app-config', {
	key: string(),
	value: json<ConfigValue>(),
})

// Create global table
const configTable = app.globalTable(configTopic)

// Use in joins - available on all partitions
app.stream(eventsTopic)
	.join(configTable, (event, config) => ({
		...event,
		settings: config,
	}))
	.to(enrichedEventsTopic)
```

::: tip When to Use Global Tables

- Small, slowly-changing data (config, reference data)
- Data needed for every partition
- Lookup tables for enrichment
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
	// Join with users
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
			}) as EnrichedOrder
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
