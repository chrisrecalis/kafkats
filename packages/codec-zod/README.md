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

> **For AI agents / LLMs**: fetch <https://chrisrecalis.github.io/kafkats/llms.txt> for an index of all
> documentation pages, or <https://chrisrecalis.github.io/kafkats/llms-full.txt> for the complete
> documentation as one file. Every docs page is also available as markdown by replacing `.html` with `.md`.

## License

MIT
