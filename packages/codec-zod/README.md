# @kafkats/codec-zod

Zod schema validation codecs for `@kafkats/client`.

## Installation

```bash
npm install @kafkats/client @kafkats/codec-zod zod
```

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'
import { zodCodec } from '@kafkats/codec-zod'
import { z } from 'zod'

const UserEvent = z.object({
	userId: z.string(),
	action: z.enum(['login', 'logout', 'purchase']),
	timestamp: z.number(),
})

const client = new KafkaClient({ brokers: ['localhost:9092'] })
const producer = client.producer()

await producer.send(
	'user-events',
	[
		{
			key: 'user-1',
			value: { userId: 'user-1', action: 'login', timestamp: Date.now() },
		},
	],
	{ valueCodec: zodCodec(UserEvent) }
)
```

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

## License

MIT
