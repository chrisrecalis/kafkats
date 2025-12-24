# Codecs

Codecs handle serialization and deserialization of message keys and values. kafkats provides built-in codecs and supports custom implementations.

## Built-in Codecs

### String Codec

Encodes/decodes UTF-8 strings:

```typescript
import { string } from '@kafkats/client'

const codec = string()
codec.encode('hello') // Buffer
codec.decode(buffer) // 'hello'
```

### JSON Codec

Encodes/decodes JSON with TypeScript generics:

```typescript
import { json } from '@kafkats/client'

interface User {
	id: string
	name: string
}

const codec = json<User>()
codec.encode({ id: '1', name: 'Alice' }) // Buffer (JSON string)
codec.decode(buffer) // { id: '1', name: 'Alice' }
```

### Buffer Codec

Passthrough for raw binary data:

```typescript
import { buffer } from '@kafkats/client'

const codec = buffer()
codec.encode(data) // Same Buffer
codec.decode(buf) // Same Buffer
```

## Using Codecs with Topics

Define typed topics with codecs:

```typescript
import { topic, string, json } from '@kafkats/client'

interface OrderEvent {
	orderId: string
	status: 'created' | 'shipped' | 'delivered'
}

const orders = topic('orders', {
	key: string(),
	value: json<OrderEvent>(),
})

// Type-safe producer
await producer.send(orders, [{ key: 'order-123', value: { orderId: 'order-123', status: 'created' } }])

// Type-safe consumer
await consumer.runEach(orders, async message => {
	// message.key: string
	// message.value: OrderEvent
})
```

## Custom Codecs

Create custom codecs for any serialization format:

```typescript
import { codec } from '@kafkats/client'

// Simple custom codec
const intCodec = codec<number>(
	n => {
		const buf = Buffer.alloc(4)
		buf.writeInt32BE(n)
		return buf
	},
	buf => buf.readInt32BE()
)
```

### Protocol Buffers Example

```typescript
import { codec } from '@kafkats/client'
import { User } from './generated/user_pb.js'

const userCodec = codec<User>(
	user => Buffer.from(user.serializeBinary()),
	buf => User.deserializeBinary(buf)
)

const users = topic('users', {
	key: string(),
	value: userCodec,
})
```

### Avro Example

```typescript
import { codec } from '@kafkats/client'
import avro from 'avsc'

const userType = avro.Type.forSchema({
	type: 'record',
	name: 'User',
	fields: [
		{ name: 'id', type: 'string' },
		{ name: 'name', type: 'string' },
	],
})

const avroCodec = codec<{ id: string; name: string }>(
	user => userType.toBuffer(user),
	buf => userType.fromBuffer(buf)
)
```

## Codec Interface

A codec must implement the `Codec<T>` interface:

```typescript
interface Codec<T> {
	encode(value: T): Buffer
	decode(buffer: Buffer): T
}
```

## Custom Value Codecs

For simpler cases, define a value codec inline:

```typescript
import { topic } from '@kafkats/client'

const events = topic('events', {
	value: {
		encode: (value: MyType) => Buffer.from(JSON.stringify(value)),
		decode: (buf: Buffer) => JSON.parse(buf.toString()) as MyType,
	},
})
```

## Key-Only or Value-Only Codecs

You can specify codecs for just keys or just values:

```typescript
// Key codec only (value stays as Buffer)
const keyed = topic('keyed', {
	key: string(),
})

// Value codec only (key stays as Buffer)
const valued = topic('valued', {
	value: json<MyEvent>(),
})
```

## Null Handling

Codecs receive/return `null` for missing values:

```typescript
const nullableCodec = codec<string | null>(
	value => (value === null ? Buffer.alloc(0) : Buffer.from(value)),
	buf => (buf.length === 0 ? null : buf.toString())
)
```

## Using with Zod

For runtime validation, use [@kafkats/flow-codec-zod](/flow-codec-zod/):

```typescript
import { zodCodec } from '@kafkats/flow-codec-zod'
import { z } from 'zod'

const UserSchema = z.object({
	id: z.string(),
	email: z.string().email(),
})

const userCodec = zodCodec(UserSchema)
// Validates on both encode and decode
```

## Performance Considerations

- **Reuse codecs** - Create codec instances once, reuse them
- **Buffer pooling** - For high-throughput, consider pooling buffers
- **Pre-size buffers** - Calculate buffer size upfront when possible

```typescript
// Efficient: pre-sized buffer
const efficientCodec = codec<number[]>(
	arr => {
		const buf = Buffer.alloc(4 * arr.length)
		arr.forEach((n, i) => buf.writeInt32BE(n, i * 4))
		return buf
	},
	buf => {
		const arr: number[] = []
		for (let i = 0; i < buf.length; i += 4) {
			arr.push(buf.readInt32BE(i))
		}
		return arr
	}
)
```
