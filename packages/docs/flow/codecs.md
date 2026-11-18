# Codecs

Codecs turn keys and values into bytes and back. They are provided by `@kafkats/client` and re-exported by `@kafkats/flow`, so `import { string, json, buffer, codec } from '@kafkats/flow'` works as well.

## Built-in Codecs

### string

UTF-8 string encoding:

```typescript
import { string } from '@kafkats/client'

const codec = string()
```

### json

JSON serialization with TypeScript generics:

```typescript
import { json } from '@kafkats/client'

interface User {
	id: string
	name: string
}

const codec = json<User>()
```

### buffer

Raw buffer passthrough:

```typescript
import { buffer } from '@kafkats/client'

const codec = buffer()
```

## Using with Topics

```typescript
import { topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const userEvents = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})
```

## Custom Codecs

A custom codec is any object that implements the `Codec` interface below. (`codec` itself is a namespace of the built-in codecs: `codec.string()`, `codec.json<T>()`, `codec.buffer()`.)

```typescript
import type { Codec } from '@kafkats/client'

const intCodec: Codec<number> = {
	encode: n => {
		const buf = Buffer.alloc(4)
		buf.writeInt32BE(n)
		return buf
	},
	decode: buf => buf.readInt32BE(),
}
```

### Codec Interface

```typescript
interface Codec<T> {
	encode(value: T): Buffer
	decode(buffer: Buffer): T
}
```

## Use Cases

### In Topic Definitions

```typescript
import { topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const orders = topic('orders', {
	key: string(),
	value: json<Order>(),
})

app.stream(orders)
	.filter((key, order) => order !== null && order.total > 100)
	.to('large-orders') // the stream still knows both codecs
```

### In TestDriver

```typescript
import { TestDriver } from '@kafkats/flow/testing'
import { string, json } from '@kafkats/client'

const driver = new TestDriver()

driver.input('orders', {
	key: string(),
	value: json<Order>(),
})
```

### In State Stores

```typescript
import { Stores, string, json } from '@kafkats/flow'

const sessions = Stores.keyValueStore({ name: 'sessions', key: string(), value: json<Session>() })
```

Stores of the DSL take their codecs from the operator (`Materialized.key` / `value`) or from the stream.

## Protocol Buffers

```typescript
import { topic } from '@kafkats/flow'
import { string, type Codec } from '@kafkats/client'
import { User } from './generated/user_pb.js'

const userCodec: Codec<User> = {
	encode: user => Buffer.from(user.serializeBinary()),
	decode: buf => User.deserializeBinary(buf),
}

const usersTopic = topic('users', {
	key: string(),
	value: userCodec,
})
```

## Avro

```typescript
import type { Codec } from '@kafkats/client'
import avro from 'avsc'

const userType = avro.Type.forSchema({
	type: 'record',
	name: 'User',
	fields: [
		{ name: 'id', type: 'string' },
		{ name: 'name', type: 'string' },
	],
})

const avroCodec: Codec<{ id: string; name: string }> = {
	encode: user => userType.toBuffer(user),
	decode: buf => userType.fromBuffer(buf),
}
```

## Zod Validation

Use [@kafkats/codec-zod](/flow-codec-zod/) for runtime validation:

```typescript
import { zodCodec } from '@kafkats/codec-zod'
import { z } from 'zod'

const UserSchema = z.object({
	id: z.string(),
	email: z.string().email(),
	age: z.number().min(0),
})

const userCodec = zodCodec(UserSchema)
// Validates on decode (encode serializes the value as JSON without validating)
```

## Null Keys and Values

Codecs never see `null`: a record without a key, or with a null value (a tombstone), is `null` in the stream, and a `null` key or value is written as a missing key or a tombstone. A codec only encodes and decodes actual values.

## Composite Codecs

Combine multiple codecs:

```typescript
import type { Codec } from '@kafkats/client'

interface KeyValue {
	timestamp: number
	value: string
}

const kvCodec: Codec<KeyValue> = {
	encode: kv => {
		const valueBuf = Buffer.from(kv.value)
		const buf = Buffer.alloc(8 + valueBuf.length)
		buf.writeBigInt64BE(BigInt(kv.timestamp))
		valueBuf.copy(buf, 8)
		return buf
	},
	decode: buf => ({
		timestamp: Number(buf.readBigInt64BE()),
		value: buf.subarray(8).toString(),
	}),
}
```

## Codecs Flow Picks

Operators find the codecs they need in their options first, then in what the stream still knows (see [How codecs are resolved](/flow/operations#how-codecs-are-resolved)). A few results have codecs of their own:

| Result                                  | Codec                                                                                       |
| --------------------------------------- | ------------------------------------------------------------------------------------------- |
| `count()` values                        | 8-byte big-endian integers (Kafka Streams' `Serdes.Long`); `value` in the options to change |
| Keys of time and sliding window results | `WindowedCodecs.timeWindowed(keyCodec, windowSize)`: `[key][start int64 BE]`                |
| Keys of session window results          | `WindowedCodecs.sessionWindowed(keyCodec)`: `[key][end int64 BE][start int64 BE]`           |

To read counts written by another flow or Kafka Streams application:

```typescript
import type { Codec } from '@kafkats/flow'

const int64: Codec<number> = {
	encode: n => {
		const buf = Buffer.alloc(8)
		buf.writeBigInt64BE(BigInt(n))
		return buf
	},
	decode: buf => Number(buf.readBigInt64BE()),
}

app.stream('clicks-per-user', { key: string(), value: int64 })
```

See [Windowed keys](/flow/windowing#windowed-keys) for the windowed formats.
