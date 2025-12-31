# @kafkats/flow

Kafka Streams-like DSL for stream processing in TypeScript.

## Features

- **Stream DSL** — map, filter, branch, flatMap, and more
- **Tables** — KTable with changelog-backed state
- **Aggregations** — groupByKey, count, aggregate, reduce
- **Windowing** — Tumbling, hopping, sliding, and session windows
- **Joins** — Stream-stream and stream-table joins
- **State Stores** — Pluggable state with in-memory default
- **Testing** — Built-in TestDriver for unit testing topologies

## Installation

```bash
npm install @kafkats/client @kafkats/flow
```

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'
import { flow } from '@kafkats/flow'

const client = new KafkaClient({ brokers: ['localhost:9092'] })

const app = flow(client, { applicationId: 'my-app' })

app.stream('input-topic')
	.filter((key, value) => value.includes('important'))
	.mapValues(value => value.toUpperCase())
	.to('output-topic')

await app.start()
```

### Testing

```typescript
import { TestDriver, testRecord } from '@kafkats/flow/testing'

const driver = new TestDriver(app)
await driver.send('input-topic', testRecord('key', 'important message'))
const output = await driver.collect('output-topic')
```

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

## License

MIT
