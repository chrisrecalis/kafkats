# @kafkats/flow-state-rocksdb

Persistent RocksDB state stores for `@kafkats/flow`.

## Features

- **Persistent State** — Survives application restarts; restoration resumes from a checkpoint
- **Write-Optimized** — Writes go to a write-ahead log and an in-memory table and do not wait for the disk
- **Larger Than Memory** — State is kept in compressed, sorted files
- **All Store Types** — KeyValue, Window, and Session stores

## Installation

```bash
npm install @kafkats/flow @kafkats/flow-state-rocksdb
```

Requires Node.js 22.18 or later. The RocksDB binding ([`@harperfast/rocksdb-js`](https://github.com/HarperFast/rocksdb-js)) ships prebuilt binaries for Linux, macOS and Windows.

## Usage

```typescript
import { KafkaClient } from '@kafkats/client'
import { flow, string } from '@kafkats/flow'
import { rocksdb } from '@kafkats/flow-state-rocksdb'

const client = new KafkaClient({ clientId: 'my-app', brokers: ['localhost:9092'] })

const app = flow({
	applicationId: 'my-app',
	client,
	stateStoreProvider: rocksdb({ stateDir: './state' }),
})

app.stream('events', { key: string(), value: string() }).groupByKey().count().toStream().to('counts')

await app.start()
```

## Options

```typescript
rocksdb({
	stateDir: './state', // Directory of the RocksDB database
	rocksdb: {
		// Optional: passed to RocksDB when the database is opened
		writeBufferSize: 64 * 1024 * 1024, // in-memory table size before it is written to a file (default 16 MB)
		compression: 'zstd', // default 'lz4'
	},
})
```

All stores of an application share one RocksDB database, so memory and file settings apply to the application as a whole, however many stores and partitions it has. Only one process can have a `stateDir` open at a time.

## Documentation

See the [documentation site](https://chrisrecalis.github.io/kafkats/flow-state-rocksdb/).

## License

MIT
