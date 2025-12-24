# Usage

## Basic Usage

```typescript
import { zodCodec } from '@kafkats/flow-codec-zod'
import { z } from 'zod'

// Simple schema
const MessageSchema = z.object({
	type: z.string(),
	payload: z.unknown(),
	timestamp: z.number(),
})

const codec = zodCodec(MessageSchema)
```

## With Flow Topics

```typescript
import { flow, topic } from '@kafkats/flow'
import { string } from '@kafkats/client'
import { zodCodec } from '@kafkats/flow-codec-zod'
import { z } from 'zod'

const OrderSchema = z.object({
	orderId: z.string(),
	userId: z.string(),
	items: z.array(
		z.object({
			productId: z.string(),
			quantity: z.number().int().positive(),
			price: z.number().positive(),
		})
	),
	total: z.number().positive(),
	status: z.enum(['pending', 'confirmed', 'shipped', 'delivered']),
})

const orders = topic('orders', {
	key: string(),
	value: zodCodec(OrderSchema),
})

const app = flow({
	applicationId: 'order-processor',
	client: { clientId: 'order-processor', brokers: ['localhost:9092'] },
})

// Types are fully inferred
app.stream(orders)
	.filter((_, order) => order.status === 'pending')
	.mapValues(order => ({ ...order, status: 'confirmed' as const }))
	.to(confirmedOrdersTopic)
```

## With Client Producer/Consumer

```typescript
import { KafkaClient, topic, string } from '@kafkats/client'
import { zodCodec } from '@kafkats/codec-zod'
import { z } from 'zod'

const EventSchema = z.object({
	type: z.string(),
	data: z.record(z.unknown()),
})

const events = topic('events', {
	key: string(),
	value: zodCodec(EventSchema),
})

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

// Producer - validates on send
const producer = client.producer()
await producer.send(events, [
	{
		key: 'event-1',
		value: { type: 'click', data: { page: '/home' } }, // Valid
	},
])

// Consumer - validates on receive
const consumer = client.consumer({ groupId: 'my-group' })
await consumer.runEach(events, async message => {
	// message.value is validated Event type
	console.log(message.value.type)
})
```

## Complex Schemas

### Nested Objects

```typescript
const AddressSchema = z.object({
	street: z.string(),
	city: z.string(),
	country: z.string(),
	zipCode: z.string(),
})

const CustomerSchema = z.object({
	id: z.string().uuid(),
	name: z.string().min(1),
	email: z.string().email(),
	addresses: z.array(AddressSchema),
	primaryAddressIndex: z.number().int().min(0),
})
```

### Unions and Discriminated Unions

```typescript
// Simple union
const IdSchema = z.union([z.string(), z.number()])

// Discriminated union (recommended)
const EventSchema = z.discriminatedUnion('type', [
	z.object({ type: z.literal('click'), page: z.string() }),
	z.object({ type: z.literal('purchase'), amount: z.number() }),
	z.object({ type: z.literal('signup'), email: z.string().email() }),
])

const eventCodec = zodCodec(EventSchema)
```

### Optional and Nullable

```typescript
const ProfileSchema = z.object({
	username: z.string(),
	bio: z.string().optional(),
	avatarUrl: z.string().url().nullable(),
	metadata: z.record(z.string()).default({}),
})
```

### Transformations

```typescript
const DateEventSchema = z.object({
	type: z.string(),
	// Transform string to Date on decode
	timestamp: z
		.string()
		.datetime()
		.transform(s => new Date(s)),
})

// Note: Transforms affect the inferred type
type DateEvent = z.infer<typeof DateEventSchema>
// { type: string; timestamp: Date }
```

## Error Handling

Zod throws detailed errors on validation failure:

```typescript
import { ZodError } from 'zod'

try {
	codec.decode(invalidBuffer)
} catch (error) {
	if (error instanceof ZodError) {
		console.log('Validation errors:')
		for (const issue of error.issues) {
			console.log(`  ${issue.path.join('.')}: ${issue.message}`)
		}
	}
}
```

### Graceful Error Handling

```typescript
// Create a codec that returns null on error
const safeCodec = {
	encode: (value: Order) => {
		const result = OrderSchema.safeParse(value)
		if (!result.success) {
			console.error('Encode failed:', result.error)
			return Buffer.alloc(0)
		}
		return Buffer.from(JSON.stringify(result.data))
	},
	decode: (buf: Buffer) => {
		try {
			const data = JSON.parse(buf.toString())
			const result = OrderSchema.safeParse(data)
			if (!result.success) {
				console.error('Decode failed:', result.error)
				return null
			}
			return result.data
		} catch {
			return null
		}
	},
}
```

## Schema Evolution

Handle schema changes gracefully:

```typescript
// V1 schema
const UserV1 = z.object({
	id: z.string(),
	name: z.string(),
})

// V2 schema with backward compatibility
const UserV2 = z.object({
	id: z.string(),
	name: z.string(),
	email: z.string().email().optional(), // New optional field
})

// V2 can decode V1 messages
```

## Performance Tips

1. **Reuse schemas** - Define schemas once, reuse everywhere
2. **Use `.strict()`** - Fail on extra properties
3. **Avoid heavy transforms** - Keep decode lightweight
4. **Consider caching** - For repeated validations

```typescript
// Good: Define once
const UserSchema = z.object({...}).strict()
const userCodec = zodCodec(UserSchema)

// Use everywhere
const topic1 = topic('users-v1', { value: userCodec })
const topic2 = topic('users-v2', { value: userCodec })
```
