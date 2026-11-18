# @kafkats/flow

Kafka Streams-like DSL for stream processing in TypeScript.

## Features

- **Stream DSL** — map, filter, flatMap, split/branch, merge, repartition, and more
- **Tables** — KTable and GlobalKTable with changelog-backed state, value getters and `suppress()`
- **Aggregations** — groupByKey, groupBy, count, aggregate, reduce
- **Windowing** — tumbling, hopping, sliding and session windows with grace periods, emit on window close
- **Joins** — stream-stream (with deferred left/outer results), stream-table, stream-global table and table-table joins
- **Processor API** — `process()`, `processValues()`, custom state stores, stream-time and wall-clock punctuators
- **State Stores** — pluggable state (in-memory by default; LMDB and RocksDB providers), one instance per task
- **Fault tolerance** — Kafka Streams' task runtime and commit protocol, at-least-once or exactly-once, changelog restoration, recovery without restarting the process
- **Testing** — built-in TestDriver for unit testing topologies

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
	.filter((key, value) => value !== null && value.includes('important'))
	.mapValues(value => value?.toUpperCase() ?? null)
	.to('output-topic', { value: string() })

await app.start()
```

Values (and keys) are `null` for tombstones, so stream callbacks receive `V | null`.

### Partitioning and failures

Flow runs Kafka Streams' task model: one task per input partition of each part of the topology, with its own state, stream time and offsets. Topics that feed one stateful operation must have the same partition count (otherwise `start()` throws `CoPartitioningError`). Re-keyed streams are repartitioned through an internal topic before aggregations and joins; pass key and value codecs and a stable `name` in `groupBy(fn, { key, value, name })`.

Failures are handled as in Kafka Streams, without restarting the process:

- A task whose output or state cannot be trusted (a send that failed after the producer's retries under at-least-once, a changelog that lost records) is revived in place from its last commit.
- Infrastructure failures (fencing, a failed commit, a lost connection) make the instance leave the group and join again after a backoff, resuming from the committed offsets.
- A record that cannot be deserialized, processed or produced goes to its handler (`deserializationExceptionHandler`, `processingExceptionHandler`, `productionExceptionHandler`: `'fail'` by default, or `'continue'`).
- Anything else is fatal and goes to `uncaughtExceptionHandler`: `'shutdown'` (the default) sets `app.state()` to `'ERROR'`, `'restart'` joins the group again.

`app.getError()` returns the error. See the [fault tolerance guide](https://chrisrecalis.github.io/kafkats/flow/fault-tolerance); upgrading from 0.13 (`restartOnError` and the worker model were removed): see the [upgrade guide](https://chrisrecalis.github.io/kafkats/flow/upgrading).

### Testing

```typescript
import { TestDriver } from '@kafkats/flow/testing'
import { string } from '@kafkats/flow'

const driver = new TestDriver()

driver
	.input('input-topic', { value: string() })
	.mapValues(value => value?.toUpperCase() ?? null)
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
