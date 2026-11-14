# Joins

Joins combine data from multiple streams or tables based on matching keys.

## Join Types

| Join  | Left Record | Right Record | Output              |
| ----- | ----------- | ------------ | ------------------- |
| Inner | Required    | Required     | When both present   |
| Left  | Required    | Optional     | When left present   |
| Outer | Optional    | Optional     | When either present |

## Join Options

Joins accept an optional `options` object (type `Joined<K, V1, V2>`):

| Option       | Type                                              | Description                                    |
| ------------ | ------------------------------------------------- | ---------------------------------------------- |
| `within`     | `TimeWindows \| SessionWindows \| SlidingWindows` | Join window (required for stream-stream joins) |
| `key`        | `Codec<K>`                                        | Key codec override                             |
| `value`      | `Codec<V1>`                                       | Left-side value codec override                 |
| `otherValue` | `Codec<V2>`                                       | Right-side value codec override                |

If a stream was re-keyed with `selectKey()` or `map()`, the join repartitions it first, which needs its key and value codecs. Pass them here when they cannot be inferred. See [Repartitioning](/flow/streams#repartitioning).

## Stream-Stream Joins

Join two streams within a time window:

```typescript
import { TimeWindows } from '@kafkats/flow'

const clicks = app.stream(clicksTopic)
const impressions = app.stream(impressionsTopic)

const clicksWithImpressions = clicks.join(
	impressions,
	(click, impression) => ({
		clickId: click.id,
		impressionId: impression.id,
		clicked: true,
	}),
	{ within: TimeWindows.of('5m') } // Events must be within 5 minutes
)
```

Several records with the same key and the same timestamp on one side are all kept, and each of them can match.

::: warning Left and outer joins emit eagerly
A stream-stream `leftJoin()` or `outerJoin()` emits the unmatched result (`null` for the other side) as soon as a record arrives without a match. If a matching record arrives later within the window, a **second** record is emitted with the match. Downstream consumers should expect both.
:::

### Window Requirement

Stream-stream joins require a time window because streams are unbounded:

```typescript
// Join clicks and purchases within 1 hour
clicks.join(purchases, joiner, { within: TimeWindows.of('1h') })
```

### Join Types

```typescript
// Inner join - both must have matching event
clicks.join(purchases, joiner, { within: TimeWindows.of('1h') })

// Left join - emit for all clicks, purchase may be null
clicks.leftJoin(
	purchases,
	(click, purchase) => ({
		click,
		purchased: purchase !== null,
	}),
	{ within: TimeWindows.of('1h') }
)

// Outer join - emit for either click or purchase
clicks.outerJoin(
	purchases,
	(click, purchase) => ({
		click,
		purchase,
	}),
	{ within: TimeWindows.of('1h') }
)
```

## Stream-Table Joins

Join a stream with a table for enrichment:

```typescript
const orders = app.stream(ordersTopic)
const users = app.table(usersTopic)

const enrichedOrders = orders.join(users, (order, user) => ({
	orderId: order.id,
	userName: user.name,
	userEmail: user.email,
	total: order.total,
}))
```

### Key Matching

Both sides must have the same key for joining:

```typescript
// Orders keyed by orderId, users keyed by userId
// Need to rekey orders first
orders
	.selectKey((_, order) => order.userId) // Rekey by userId
	.join(users, joiner, { key: string() })
```

The re-keyed stream is written to an internal repartition topic and read back, so every order for a user is processed in the same partition as that user's row. The `key` codec is needed because `selectKey()` does not carry one over. See [Repartitioning](/flow/streams#repartitioning).

### Left Join

Keep all stream records even without table match:

```typescript
orders.leftJoin(users, (order, user) => ({
	orderId: order.id,
	userName: user?.name ?? 'Guest',
	total: order.total,
}))
```

## Table-Table Joins

Join two tables:

```typescript
const users = app.table(usersTopic)
const profiles = app.table(profilesTopic)

const fullUsers = users.join(profiles, (user, profile) => ({
	...user,
	...profile,
}))
```

### Changelog Semantics

Table-table joins update when either side changes:

```typescript
// When user updates → output updates
// When profile updates → output updates
```

## Global Table Joins

Join with a global table (fully replicated):

```typescript
const config = app.globalTable(configTopic)

app.stream(eventsTopic)
	.join(config, (event, configValue) => ({
		...event,
		setting: configValue.setting,
	}))
	.to(enrichedEventsTopic)
```

Every instance reads all partitions of the topic itself and loads the table fully before processing starts, so there is no co-partitioning requirement. A global table can only be the lookup side of a stream `join()` or `leftJoin()`.

Global tables are useful for:

- Small, reference data
- Lookup tables needed everywhere
- Configuration data
- Joining with topics that have a different partition count

::: tip
A stream that was re-keyed with `selectKey()` or `map()` is repartitioned before a join with another stream or a table; pass the `key` and `value` codecs in the join options for it. Joins with a global table never repartition.
:::

## Co-partitioning Requirement

State is kept per partition, so for a join to find its match, both sides must be **co-partitioned**:

- the same number of partitions
- the same partitioning logic: records with the same key are in the same partition index

This applies to both sides of a stream-stream or table-table join, and to the stream and the table of a stream-table join. If the partition counts differ, `start()` throws `CoPartitioningError`:

```typescript
import { CoPartitioningError } from '@kafkats/flow'

try {
	await app.start()
} catch (err) {
	if (err instanceof CoPartitioningError) {
		console.error(err.partitionCounts) // Map { 'orders' => 6, 'users' => 3 }
	}
}
```

Flow can only check partition counts. It cannot check that both topics were written with the same partitioner.

To fix a mismatch, repartition one side with `through()` into a topic that has the right partition count (it must exist), or use a [global table](#global-table-joins):

```typescript
orders
	.through(ordersByUserTopic, { key: string(), value: json<Order>() }) // same partition count as users
	.join(users, joiner)
```

Streams that were re-keyed with `selectKey()` or `map()` are repartitioned automatically, so you only need `through()` when the partition counts differ.

## Example: Order Enrichment Pipeline

```typescript
interface Order {
	orderId: string
	userId: string
	productId: string
	quantity: number
	price: number
}

interface User {
	userId: string
	name: string
	tier: 'bronze' | 'silver' | 'gold'
}

interface Product {
	productId: string
	name: string
	category: string
}

interface EnrichedOrder {
	orderId: string
	userName: string
	userTier: string
	productName: string
	productCategory: string
	quantity: number
	price: number
	discount: number
}

// Tables for lookup
const users = app.table<string, User>(usersTopic)
const products = app.globalTable<string, Product>(productsTopic)

// Process orders
app.stream<string, Order>(ordersTopic)
	// First join with users (by userId). The re-keyed stream is repartitioned, so give it codecs
	.selectKey((_, order) => order.userId)
	.join(users, (order, user) => ({ order, user }), { key: string() })

	// Then join with products (by productId, from a global table)
	.selectKey((_, { order }) => order.productId)
	.join(
		products,
		({ order, user }, product) =>
			({
				orderId: order.orderId,
				userName: user.name,
				userTier: user.tier,
				productName: product.name,
				productCategory: product.category,
				quantity: order.quantity,
				price: order.price,
				discount: user.tier === 'gold' ? 0.15 : user.tier === 'silver' ? 0.1 : 0.05,
			}) as EnrichedOrder,
		{ key: string(), value: json<{ order: Order; user: User }>() }
	)

	.to(enrichedOrdersTopic)
```

## Example: Session Attribution

```typescript
import { TimeWindows } from '@kafkats/flow'

interface PageView {
	sessionId: string
	page: string
	timestamp: number
}

interface Conversion {
	sessionId: string
	product: string
	amount: number
	timestamp: number
}

interface Attribution {
	sessionId: string
	pages: string[]
	product: string
	amount: number
}

const pageViews = app.stream<string, PageView>(pageViewsTopic)
const conversions = app.stream<string, Conversion>(conversionsTopic)

// Attribute conversions to page views within 30 minutes
pageViews
	.groupByKey()
	.aggregate(
		() => ({ pages: [] as string[] }),
		(sessionId, pv, agg) => ({
			pages: [...agg.pages, pv.page],
		})
	)
	.toStream()
	.join(
		conversions,
		(pageViewAgg, conversion) =>
			({
				sessionId: conversion.sessionId,
				pages: pageViewAgg.pages,
				product: conversion.product,
				amount: conversion.amount,
			}) as Attribution,
		{ within: TimeWindows.of('30m') }
	)
	.to(attributionsTopic)
```

## Performance Tips

1. **Order matters** - Join smaller table to larger stream
2. **Use global tables** - For small lookup data
3. **Rekey sparingly** - Repartitioning writes every record to an internal topic and reads it back
4. **Window size** - Smaller windows = less state
5. **Materialization** - Name stores for queryability
