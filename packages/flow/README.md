# @kafkats/flow

Kafka Streams-like DSL for stream processing in TypeScript.

## Features

- **Stream DSL** — map, filter, branch, flatMapValues, and more
- **Tables** — KTable with changelog-backed state
- **Aggregations** — groupByKey, count, aggregate, reduce
- **Windowing** — Tumbling, hopping, and session windows
- **Joins** — Stream-stream and stream-table joins
- **State Stores** — Pluggable state with in-memory default, partitioned per source partition
- **Fault tolerance** — At-least-once or exactly-once processing, changelog restoration, workers that restart after recoverable failures
- **Testing** — Built-in TestDriver for unit testing topologies

## Installation

```bash
npm install @kafkats/client @kafkats/flow
```

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'
import { flow, string } from '@kafkats/flow'

const client = new KafkaClient({ clientId: 'my-app', brokers: ['localhost:9092'] })

const app = flow({ applicationId: 'my-app', client })

app.stream('input-topic', { value: string() })
	.filter((key, value) => value.includes('important'))
	.mapValues(value => value.toUpperCase())
	.to('output-topic', { value: string() })

await app.start()
```

### Partitioning and failures

State is kept per source partition, so topics that feed one stateful operation must have the same partition count (otherwise `start()` throws `CoPartitioningError`). Re-keyed streams are repartitioned through an internal topic before aggregations and joins; pass key and value codecs and a stable `name` in `groupBy(fn, { key, value, name })`.

A worker that hits a failure never continues past it. Recoverable failures restart the worker from the last committed offsets (disable with `restartOnError: false`); errors thrown by your code and fatal Kafka errors set `app.state()` to `'ERROR'`, and `app.getError()` returns the error. See the [fault tolerance guide](https://chrisrecalis.github.io/kafkats/flow/fault-tolerance).

### Testing

```typescript
import { TestDriver } from '@kafkats/flow/testing'
import { string } from '@kafkats/flow'

const driver = new TestDriver()

driver
	.input('input-topic', { value: string() })
	.mapValues(value => value.toUpperCase())
	.to('output-topic', { value: string() })

await driver.run(async ({ send, output }) => {
	await send('input-topic', 'important message', { codec: string(), key: Buffer.from('key') })
	expect(output('output-topic', { value: string() })[0]?.value).toBe('IMPORTANT MESSAGE')
})
```

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

> **For AI agents / LLMs**: fetch <https://chrisrecalis.github.io/kafkats/llms.txt> for an index of all
> documentation pages, or <https://chrisrecalis.github.io/kafkats/llms-full.txt> for the complete
> documentation as one file. Every docs page is also available as markdown by replacing `.html` with `.md`.

## License

MIT
