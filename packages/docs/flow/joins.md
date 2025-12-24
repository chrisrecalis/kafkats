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
	.join(users, joiner)
```

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

Global tables are useful for:

- Small, reference data
- Lookup tables needed everywhere
- Configuration data

## Co-partitioning Requirement

For joins to work correctly, both sides must be **co-partitioned**:

- Same number of partitions
- Same partitioning logic

```typescript
// Both topics must have same partition count and key type
const orders = topic('orders', { key: string(), ... })
const users = topic('users', { key: string(), ... })  // Same key type
```

If not co-partitioned, rekey through an intermediate topic:

```typescript
orders
	.selectKey((_, o) => o.userId)
	.through(rekeyedOrdersTopic) // Repartition
	.join(users, joiner)
```

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
	// First join with users (by userId)
	.selectKey((_, order) => order.userId)
	.join(users, (order, user) => ({ order, user }))

	// Then join with products (by productId)
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
			}) as EnrichedOrder
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
3. **Rekey sparingly** - Repartitioning is expensive
4. **Window size** - Smaller windows = less state
5. **Materialization** - Name stores for queryability
