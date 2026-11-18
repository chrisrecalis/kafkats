# KStream

A `KStream` is an unbounded stream of key-value records. Each record is an independent event: two records with the same key are two events, not an update of one row (that is a [KTable](/flow/tables)).

Keys and values of a stream may be `null`: a record without a key, or a tombstone. Every stream callback takes `(key, value)` and sees `K | null` and `V | null`.

## Creating a Stream

```typescript
import { topic, string, json } from '@kafkats/flow'

const orders = topic('orders', { key: string(), value: json<Order>() })

const stream = app.stream(orders)
// or with a topic name and codecs:
const same = app.stream('orders', { key: string(), value: json<Order>() })
```

The second argument is a `Consumed`: codecs (`key`, `value`; missing ones read raw `Buffer`s), a `timestampExtractor`, an `offsetReset` policy for partitions without a committed offset, and a `name` for the source node. See [Operations](/flow/operations#consumed).

## Stateless Operations

### filter, filterNot

```typescript
stream.filter((key, order) => order !== null && order.amount > 100)
stream.filterNot((key, order) => order?.status === 'test')
```

### map

Changes the key and the value. A key-based operation downstream [repartitions](#repartitioning) first.

```typescript
stream.map((key, order) => [order!.customerId, order!.amount] as const)
```

### mapValues, mapValuesWithKey

Changes the value and keeps the key, so no repartitioning is needed. `mapValues()` calls its mapper with the value alone (Kafka Streams' `ValueMapper`), whatever parameters the function declares, so `stream.mapValues(JSON.stringify)` stringifies the value; `mapValuesWithKey()` calls it with the key and the value (`ValueMapperWithKey`).

```typescript
stream.mapValues(order => order!.amount)
stream.mapValuesWithKey((key, order) => `${key}:${order?.amount}`)
```

### flatMap, flatMapValues, flatMapValuesWithKey

Zero or more records per record. `flatMap` may change keys (and so requires repartitioning downstream); `flatMapValues` and `flatMapValuesWithKey` keep the record's key. Any iterable works.

```typescript
stream.flatMap((key, order) => order!.items.map(item => [item.sku, item] as const))
stream.flatMapValues(order => order!.items)
stream.flatMapValuesWithKey((key, order) => order!.items.map(item => `${key}:${item.sku}`))
```

### selectKey

Picks a new key from `(key, value)`. A key-based operation downstream repartitions first.

```typescript
stream.selectKey((key, order) => order!.customerId)
```

### peek, foreach

`peek` calls an action for every record and passes the record on; `foreach` is the terminal form.

```typescript
stream.peek((key, order) => console.log('order', key)).to('orders-seen')
stream.foreach((key, order) => metrics.increment('orders'))
```

### merge

The records of two streams of the same app, as one stream:

```typescript
const all = webOrders.merge(storeOrders)
```

The result keeps the codecs both streams share (the same codec objects), and needs repartitioning if either side does.

## Splitting a Stream

`split()` routes each record to the first branch whose predicate matches it, else to the default branch if there is one (Kafka Streams' `BranchedKStream`):

```typescript
const branches = stream
	.split({ name: 'orders-' })
	.branch((key, order) => order!.amount >= 1000, { name: 'large' })
	.branch((key, order) => order!.country !== 'US', { name: 'international' })
	.defaultBranch({ name: 'other' })

branches['orders-large']!.to('large-orders')
branches['orders-international']!.to('international-orders')
branches['orders-other']!.to('domestic-orders')
```

- The result maps `<split name><branch name>` to each branch. A branch without a name is named by its position (`1`, `2`, ...; the default branch `0`). Without a split name, the prefix is the generated name of the split node (`KSTREAM-BRANCH-<n>`), so name the split to look branches up.
- `chain` builds on a branch in place; what it returns replaces the branch in the result, and a chain that returns nothing (one ending in `to()`) leaves the branch out:

```typescript
stream
	.split({ name: 'route-' })
	.branch((key, order) => order!.priority, { chain: s => s.to('priority-orders') })
	.noDefaultBranch() // records no predicate matched are dropped
```

- Every branch must be added before `defaultBranch()` or `noDefaultBranch()`; adding one later throws `TopologyError`.

`branch(...predicates)` is deprecated: it returns an array of streams, one per predicate, and drops records no predicate matches.

## Output

### to

Writes every record to a topic:

```typescript
stream.to('processed-orders', { key: string(), value: json<Order>() })
```

The options are a `Produced`: codecs (the stream's codecs otherwise; a stream without one can write only `Buffer`s), a `partitioner`, and a `name` for the sink node. The record keeps its timestamp and headers.

The topic can be chosen per record with a topic name extractor, which receives the key, the value and the record's context (input `topic`, `partition`, `offset`, and the record's `timestamp` and `headers`):

```typescript
stream.to((key, order, context) => `orders-${order!.region}`, { key: string(), value: json<Order>() })
```

An extractor that returns anything but a non-empty string fails the record (it goes to the [processing exception handler](/flow/error-handling#processing-exceptions)).

A `partitioner` returns the partition of a record, or `undefined` for the producer's default partitioning of the encoded key. Records whose key codec is windowed (the results of windowed aggregations) go to the partition of their base key by default; see [Windowed keys](/flow/windowing#windowed-keys).

### repartition

Writes the stream to an internal topic and reads it back, even if keys did not change:

```typescript
stream
	.selectKey((key, order) => order!.customerId)
	.repartition({
		name: 'orders-by-customer', // topic <applicationId>-orders-by-customer-repartition
		key: string(),
		value: json<Order>(),
		numberOfPartitions: 12,
	})
```

`Repartitioned` options: `name` (else `repartition-<n>` is generated), codecs (the stream's otherwise; both are required), `numberOfPartitions` (default: the largest partition count of the topics the stream comes from, or the count of the topics it is co-partitioned with downstream, which an explicit count must then equal), and a `partitioner`. Key-based operations after `repartition()` do not repartition again.

### through (deprecated)

`through(topic, produced)` writes to a topic you created and continues with what is read back from it. Use `repartition()`, or `to()` and `app.stream()`. A `through()` topic must exist; it is read from `'earliest'` without a committed offset, and unlike a repartition topic it keeps records with a null key.

## Grouping

Aggregations start from a grouped stream. See [Aggregations](/flow/aggregations).

```typescript
// By the current key: repartitions only if keys changed upstream
stream.groupByKey()

// By a new key: always repartitions; pass the new key's codec
stream.groupBy((key, order) => order!.customerId, { key: string(), name: 'by-customer' })
```

`Grouped` options: `key` and `value` codecs, and a `name` that names the repartition topic (`<applicationId>-<name>-repartition`), shared by every aggregation built from the grouping.

## Conversion

### toTable

Reads the stream as a table: the latest value per key, a null (or undefined) value deleting the key. Records with a null key are dropped. The table is materialized in a store; pass a `Materialized` to name it or set its codecs.

```typescript
const latestOrders = stream.toTable({ storeName: 'latest-orders' })
```

## Joins

Streams join other streams within a window, tables on the current value of the key, and global tables on a key you select. See [Joins](/flow/joins).

```typescript
orders.join(payments, (order, payment) => ({ order, payment }), JoinWindows.ofTimeDifferenceWithNoGrace('10m'))
orders.leftJoin(customers, (order, customer) => ({ ...order, customer }))
orders.join(
	products,
	(key, order) => order.productId,
	(order, product) => ({ ...order, product })
)
```

## Custom Processing

`process()` and `processValues()` run your own processors, with state stores and punctuators. See the [Processor API](/flow/processor-api).

## Repartitioning

Operations that work per key (aggregations, joins with streams and tables, `toTable()`) need every record of a key in the same partition of their input. When the key of a stream may have changed since it was read (after `map()`, `flatMap()`, `selectKey()`, `process()`, `KTable.toStream(mapper)`, or a `groupBy()`), the operation first writes the stream to an internal repartition topic, partitioned by the new key, and reads it back.

| Changes keys (repartition downstream)                                   | Keeps the key                                                                                         |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `map`, `flatMap`, `selectKey`, `process`, `toStream(mapper)`, `groupBy` | `filter`, `filterNot`, `mapValues`, `flatMapValues`, `peek`, `split` branches, `processValues`, joins |

- The repartition topic is `<applicationId>-<name>-repartition`. Its name comes from the operator's `name` (`Grouped.name`, `Joined.name`, `Repartitioned.name`, ...), else from an aggregation's `storeName`, else a generated `repartition-<n>`. Name repartitioning operations: generated names depend on the order of the DSL calls, and a renamed topic is a new topic.
- Repartitioning needs key and value codecs: the operator's options, else the codecs the stream still knows. An operation that needs one nobody gave throws `TopologyError` naming it.
- Records with a null key are dropped at a repartition topic, unless a left or outer join is downstream of it (which joins them with null, as in Kafka Streams).
- Repartition topics are created by `start()` (or validated with `changelog.autoCreate: false`) with Kafka Streams' configs (`cleanup.policy=delete`, `retention.ms=-1`), read from `'earliest'` without a committed offset, and purged up to the committed offsets every `repartitionPurgeIntervalMs`.
- A named grouping repartitions once for all the aggregations built from it; an unnamed one once per aggregation, as in Kafka Streams. Joins with a [global table](/flow/joins#stream-globalktable-joins) never repartition.

See [Topology and Naming](/flow/topology) for how repartition topics cut the topology into subtopologies.
