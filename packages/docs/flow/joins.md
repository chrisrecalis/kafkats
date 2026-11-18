# Joins

| Left side | Right side   | Joins             | Result  | Kinds              | Co-partitioning |
| --------- | ------------ | ----------------- | ------- | ------------------ | --------------- |
| KStream   | KStream      | within a window   | KStream | inner, left, outer | required        |
| KStream   | KTable       | on the key        | KStream | inner, left        | required        |
| KStream   | GlobalKTable | on a selected key | KStream | inner, left        | not needed      |
| KTable    | KTable       | on the key        | KTable  | inner, left, outer | required        |

Co-partitioned topics have the same partition count and the same partitioning of keys, so that a key is in the same partition on both sides. Joins that need it put both sides in one task per partition, and `start()` throws `CoPartitioningError` when the counts differ. A stream side whose key changed upstream is repartitioned first (by the join's key), so only the topics you read have to agree.

Joiners take the two values (Kafka Streams' `ValueJoiner`), whatever parameters the function declares. For a joiner that also receives the record's key (`ValueJoinerWithKey`), stream-table and stream-global table joins have `joinWithKey()` and `leftJoinWithKey()`.

## Stream-Stream Joins

Two streams join records with the same key whose timestamps are close enough:

```typescript
import { JoinWindows } from '@kafkats/flow'

const paidOrders = orders.join(
	payments,
	(order, payment) => ({ ...order, paidAt: payment.at }),
	JoinWindows.ofTimeDifferenceAndGrace('10m', '1m'),
	{ name: 'orders-payments' } // StreamJoined: name, storeName, codecs, changelog
)
```

### JoinWindows

`JoinWindows.ofTimeDifferenceWithNoGrace(d)` / `ofTimeDifferenceAndGrace(d, grace)`: a left record at `t` joins right records with timestamps in `[t - d, t + d]`. `before()` and `after()` make the window asymmetric:

```typescript
// payments up to 1 hour after the order, none before it
JoinWindows.ofTimeDifferenceWithNoGrace('1h').before(0)
// one of before/after may be negative, as long as before + after >= 0
JoinWindows.ofTimeDifferenceWithNoGrace('1h').before('-5m')
```

A left record at `t` matches right records in `[t - before, t + after]`; a right record at `t` matches left records in `[t - after, t + before]`.

### Results

- Each match is joined once, when its second record arrives, with the timestamp `max(left, right)` and the headers of the arriving record.
- **Inner join**: only matches.
- **Left join** (`leftJoin`): also every left record that matched nothing, joined with `null`. **Outer join** (`outerJoin`): unmatched records of both sides.
- Unmatched records are joined with `null` only once their window closed (for a left record at `t`: when stream time passed `t + after + grace`), since a match can arrive until then. They wait in a shared store, and are emitted, at their own timestamp, when a later record moves stream time past their window; the check is throttled to once per second of wall clock. An unmatched record whose window already ended when it arrived is joined with `null` at once.
- Records with a null (or undefined) value are dropped. Records with a null key are dropped by inner joins and joined with `null` at once by the outer sides of left and outer joins (KIP-962).

Since unmatched results wait for stream time, a quiet stream emits them late. In tests, send a later record to move stream time.

### State

Each side's records are kept in a window store for `before + after + grace`, the time a record can still be matched. Left and outer joins keep their unmatched records in a third, shared store until they are emitted. Stores are named `<storeName>-this-join-store`, `<storeName>-other-join-store` and `<storeName>-<left|outer>-shared-join-store` when `StreamJoined.storeName` (else `name`) is given, and are backed by changelog topics unless `changelog: false`.

### Deprecated forms

The overloads of earlier versions still work:

```typescript
// deprecated: Joined with `within`
orders.join(payments, joiner, { within: TimeWindows.of('10m') })
// deprecated: JoinWindows.of
orders.join(payments, joiner, JoinWindows.of('10m'))
```

Both behave as Kafka Streams' legacy `JoinWindows.of`: a grace of `max(24h - 2 × d, 0)`, so each side keeps about a day of records, and left and outer joins emit unmatched records **eagerly**, at once, also when a match arrives later (which then adds the matched result). `within` takes the size of `TimeWindows` (advance and grace are ignored) or the gap of `SessionWindows`; `SlidingWindows` are rejected. A one-time deprecation warning names the replacement: pass `JoinWindows.ofTimeDifferenceWithNoGrace(d)` or `ofTimeDifferenceAndGrace(d, grace)` as the third argument and the remaining `Joined` settings as `StreamJoined`.

## Stream-Table Joins

Each stream record looks up the table's current value for its key:

```typescript
const enriched = orders.join(customers, (order, customer) => ({ ...order, customerName: customer.name }))

const withOptional = orders.leftJoin(customers, (order, customer) => ({
	...order,
	customerName: customer?.name ?? 'unknown',
}))

// the joiner also receives the key
orders.joinWithKey(customers, (customerId, order, customer) => ({ customerId, name: customer.name }))
```

- Only stream records produce results, with the stream record's key, timestamp and headers. An update of the table produces nothing; it changes what later stream records find.
- The table is read as it is when the stream record is processed. Within a task, records are processed in timestamp order across the stream and table partitions (see [Stream time](/flow/windowing#stream-time)), so a table update with an earlier timestamp is applied before a stream record with a later one, as long as both have arrived.
- Inner join: records without a table value are dropped. Left join: they are joined with `null`.
- Records with a null (or undefined) value are dropped. Records with a null key are dropped by the inner join and joined with `null` by the left join (KIP-962).
- Any table can be looked up, also a derived one that is not materialized (`filter()`, `mapValues()`, `transformValues()`, joins): its value is computed from its parent's.
- A stream whose key changed is repartitioned first; `Joined.name` names the repartition topic and the join node, and `Joined.key` / `value` give the codecs it needs.

## Stream-GlobalKTable Joins

A global table is fully replicated to every instance, so a stream can look up any key in it, not only its own:

```typescript
const products = app.globalTable('products', { key: string(), value: json<Product>() })

const withProduct = orders.join(
	products,
	(orderId, order) => order.productId, // key selector: which row to look up
	(order, product) => ({ ...order, productName: product.name })
)

const withOptionalProduct = orders.leftJoin(
	products,
	(orderId, order) => order.productId,
	(order, product) => ({ ...order, productName: product?.name ?? null })
)
```

- The key selector receives the record's key and value and returns the global table's key. With `joinWithKey()` and `leftJoinWithKey()` the joiner also receives the record's key (not the selected one).
- No co-partitioning and no repartitioning: the stream keeps its partitioning. A key change upstream stays pending, so a key-based operation after the join still repartitions.
- Only stream records produce results, with their key, timestamp and headers. The global table is read as it is when the record is processed; it is not synchronized with the stream's time.
- Inner join: records the table has no value for, and records whose selector returns `null` or `undefined`, are dropped. Left join: both are joined with `null`. Records with a null (or undefined) value are dropped.

The two-argument form `join(globalTable, joiner)` is deprecated: it looks up the record's own key. Pass `(key) => key` as the selector instead.

## Table-Table Joins

Two tables join on their keys into a table that changes whenever either side does:

```typescript
const profiles = users.join(addresses, (user, address) => ({ ...user, address }))
const withBalance = users.leftJoin(balances, (user, balance) => ({ ...user, balance: balance ?? 0 }))
const merged = users.outerJoin(addresses, (user, address) => ({ user, address }))
```

- An update of either side joins the new value with the other side's current value. Inner: only when both sides have a value; left: when the left side has one; outer: when either has one (`null` for the missing side).
- A delete on either side deletes the result where the join no longer has one, or updates it with `null` for the deleted side (a right delete in a left join, any delete in an outer join).
- A joiner returning `null` deletes the result.
- Results carry `max(timestamp of the update, timestamp of the other side's value)`.
- Any table can be joined, derived ones included. Pass `{ name, materialized: { storeName } }` to store the result (it can then be looked up from its store instead of being recomputed). Tables are never repartitioned for a join: both must be co-partitioned.

## Co-partitioning

```typescript
// orders has 8 partitions, customers 4: the join cannot be co-partitioned
orders.join(customers, joiner) // start() throws CoPartitioningError

// Repartition the stream to the table's partition count first
orders.repartition({ name: 'orders-for-join', numberOfPartitions: 4 }).join(customers, joiner)
```

A re-keyed stream is repartitioned to the right count automatically: its repartition topic takes the partition count of the topics it is joined with. Producers of the topics must partition keys the same way (the default partitioner of `@kafkats/client` and the Java client both use murmur2 of the serialized key).

## Example: Orders, Payments and Customers

```typescript
interface Order {
	customerId: string
	amount: number
}
interface Payment {
	orderId: string
	at: number
}
interface Customer {
	name: string
}

const orders = app.stream('orders', { key: string(), value: json<Order>() })
const payments = app.stream('payments', { key: string(), value: json<Payment>() })
const customers = app.table('customers', { key: string(), value: json<Customer>() })

orders
	// orders and payments are keyed by order id
	.leftJoin(
		payments,
		(order, payment) => ({ ...order, paid: payment !== null }),
		JoinWindows.ofTimeDifferenceAndGrace('30m', '5m'),
		{ name: 'orders-payments', otherValue: json<Payment>() }
	)
	// re-keyed by customer: the stream-table join repartitions it
	.selectKey((orderId, order) => order!.customerId)
	.join(customers, (order, customer) => ({ ...order, customer: customer.name }), {
		name: 'with-customer',
		key: string(),
		value: json(),
	})
	.to('orders-enriched', { key: string(), value: json() })
```
