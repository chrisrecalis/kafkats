# @kafkats/flow-state-lmdb

Persistent LMDB state stores for `@kafkats/flow`.

## Features

- **Persistent State** — Survives application restarts
- **Memory-Mapped** — Fast reads via LMDB
- **All Store Types** — KeyValue, Window, and Session stores

## Installation

```bash
npm install @kafkats/flow @kafkats/flow-state-lmdb
```

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'
import { flow } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

const client = new KafkaClient({ brokers: ['localhost:9092'] })

const app = flow(client, {
	applicationId: 'my-app',
	stateStoreProvider: lmdb({ stateDir: './state' }),
})

app.stream('events').groupByKey().count().toStream().to('counts')

await app.start()
```

## Options

```typescript
lmdb({
	stateDir: './state', // Directory for state files
	mapSize: 1024 ** 3, // Max database size (default: 1GB)
	maxDbs: 100, // Max number of stores (default: 100)
})
```

## Documentation

Full documentation at [chrisrecalis.github.io/kafkats](https://chrisrecalis.github.io/kafkats)

## License

MIT
