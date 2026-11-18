# KTable

A `KTable` is a changelog read as a table: for each key, the latest value. A record with a null value (a tombstone) deletes its key; flow treats `undefined` as `null` (Kafka Streams has only `null`), so a value or callback result of `undefined` deletes too. Downstream of a table, every change is an update of a row, with its new value and, where an operator needs it, its old one.

## Creating a Table

```typescript
import { topic, string, json } from '@kafkats/flow'

const users = topic('users', { key: string(), value: json<User>() })

const table = app.table(users)
const named = app.table(users, { materialized: { storeName: 'users-by-id' } })
```

`app.table()` takes the options of [`app.stream()`](/flow/operations#consumed) and a `materialized` option. A table read from a topic is always materialized in a key-value store, named by `materialized.storeName` (else `table-<topic>-<n>`) and backed by its own changelog topic `<applicationId>-<storeName>-changelog`. Records with a null key are skipped.

A table can also come from a stream (`stream.toTable()`), an aggregation (`count()`, `reduce()`, `aggregate()`), or another table.

## Stream vs Table

| KStream                                  | KTable                                  |
| ---------------------------------------- | --------------------------------------- |
| Every record is an event                 | Every record updates the row of its key |
| Two records of a key are two events      | The second record replaces the first    |
| A null value is a record with null value | A null value deletes the key            |

## Transformations

The callbacks of `filter()`, `filterNot()`, `mapValues()`, `groupBy()` and the table joins never see a deleted row: a delete is passed on as a delete without calling them, and their parameters are typed without `null`.

### filter, filterNot

```typescript
const active = table.filter((key, user) => user.active)
const inactive = table.filterNot((key, user) => user.active)
```

A row the predicate stops accepting is deleted from the result table (a tombstone is sent downstream).

### mapValues

```typescript
const emails = table.mapValues(user => user.email)
const labels = table.mapValues((key: string, user: User) => `${key}: ${user.name}`)
```

A `null` (or `undefined`) result deletes the key. As with streams, a mapper declaring two parameters receives the key.

### transformValues

A stateful value transformer (Kafka Streams' `ValueTransformerWithKey`) that can read state stores and schedule punctuators, and sees deletes as `null` values. See the [Processor API](/flow/processor-api#ktable-transformvalues).

### Materializing a Derived Table

`filter()`, `filterNot()`, `mapValues()`, `transformValues()` and the table joins take `{ name, materialized }`. Without a `materialized.storeName`, the result is not stored: a lookup of it (a join) computes its value from its parent's. With one, the result is kept in its own store and changelog:

```typescript
const vip = customers.filter((id, c) => c.tier === 'vip', {
	name: 'vip-customers',
	materialized: { storeName: 'vip-customers' },
})
```

A materialized result whose values change type needs `materialized.value` (the codec of the new values).

## Conversion

### toStream

Every update of the table as a record; a delete becomes a record with a null value.

```typescript
table.toStream().to('user-updates')

// With a new key: a key-based operation downstream repartitions first
table.toStream((key, user) => user?.country ?? 'unknown')
```

## Grouping

`groupBy()` maps every row to a new key and value, for aggregations by that key. It always repartitions, and needs the codec of the new key:

```typescript
const usersPerCountry = users
	.groupBy((id, user) => [user.country, user] as const, { key: string(), value: json<User>(), name: 'by-country' })
	.count()
```

The value codec defaults to the table's, so a mapper that changes the value type needs `value` too. Every update of a row subtracts its old value from the group it leaves and adds its new value to the group it joins. See [Table aggregations](/flow/aggregations#table-aggregations).

## Joins

Two tables join on their keys and give a table. Both must be co-partitioned (same partition count, same partitioning); tables are never repartitioned for a join.

```typescript
const profiles = users.join(addresses, (user, address) => ({ ...user, address }))
const withOrders = users.leftJoin(orderTotals, (user, total) => ({ ...user, total: total ?? 0 }))
const everything = users.outerJoin(addresses, (user, address) => ({ user, address }))
```

Any table can be joined, derived ones included. See [Table-table joins](/flow/joins#table-table-joins).

## suppress

`suppress()` holds back a table's updates: until a window closes (final results of windowed aggregations) or until a time limit passed (rate limiting). See [Suppression](/flow/suppress).

```typescript
table.suppress({ until: 'time-limit', timeLimit: '30s' })
```

## GlobalKTable

`app.globalTable()` returns a table every instance reads in full, outside the consumer group:

```typescript
const products = app.globalTable('products', {
	key: string(),
	value: json<Product>(),
	materialized: { storeName: 'products' },
})

orders.join(
	products,
	(orderId, order) => order.productId, // the key looked up in the global table
	(order, product) => ({ ...order, productName: product.name })
)
```

- `start()` loads every global table from the beginning of its topic up to the end the topic had when the load started, before any record is processed; afterwards the table follows its topic while the app runs.
- Its store is `materialized.storeName` (else `global-table-<topic>-<n>`, see `globalTable.storeName`) and has no changelog: the topic is one. It is reloaded from the topic at every start.
- A global table is only usable as the lookup side of a stream join (`KStream.join` / `leftJoin` with a key selector). Since every instance has all of it, the stream needs no co-partitioning and no repartitioning. See [Stream-GlobalKTable joins](/flow/joins#stream-globalktable-joins).
- Processors can read a global table's store by name through their context, without connecting to it.
- A row that cannot be applied fails `start()` during the load, and stops the app afterwards. A position out of range (records deleted before they were read) reloads the table in place while processing waits.

## Example: User Enrichment

```typescript
interface User {
	name: string
	tier: 'free' | 'pro'
}

interface PageView {
	page: string
}

const users = app.table('users', { key: string(), value: json<User>() })

app.stream('page-views', { key: string(), value: json<PageView>() })
	.leftJoin(users, (view, user) => ({ ...view, userName: user?.name ?? 'anonymous', tier: user?.tier ?? 'free' }))
	.to('enriched-page-views', { key: string(), value: json() })
```

The stream and the table are keyed by user id and must be co-partitioned. Each page view looks up the user's current row when it is processed; a later update of the user does not change views already joined.
