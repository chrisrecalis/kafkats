# @kafkats/flow-state-rocksdb

Persistent state stores for `@kafkats/flow`, backed by [RocksDB](https://rocksdb.org/).

## Installation

```bash
pnpm add @kafkats/flow @kafkats/flow-state-rocksdb
```

Requires Node.js 22.18 or later. The RocksDB binding, [`@harperfast/rocksdb-js`](https://github.com/HarperFast/rocksdb-js), installs a prebuilt binary for Linux, macOS and Windows.

## Usage

```typescript
import { flow } from '@kafkats/flow'
import { string } from '@kafkats/client'
import { rocksdb } from '@kafkats/flow-state-rocksdb'

const app = flow({
	applicationId: 'word-count',
	client: { clientId: 'word-count', brokers: ['localhost:9092'] },
	stateStoreProvider: rocksdb({ stateDir: './state' }),
})

app.stream('words', { key: string(), value: string() }).groupByKey().count().toStream().to('counts')

await app.start()
```

## When to Choose RocksDB

|                          | RocksDB                                     | LMDB                                 |
| ------------------------ | ------------------------------------------- | ------------------------------------ |
| Writes                   | Appended to a log; do not wait for the disk | One disk transaction per write batch |
| Reads                    | Memory, then sorted files                   | Memory-mapped B-tree, fastest        |
| State size               | Larger than memory, compressed              | Bounded by `mapSize`                 |
| Processes per `stateDir` | One                                         | One writer                           |
| Node.js                  | 22.18 or later                              | 22 or later                          |

Prefer RocksDB for write-heavy topologies, aggregations under `at_least_once` in particular, and for state that outgrows memory. Prefer LMDB for read-heavy lookups on state that fits in memory.

## Configuration

```typescript
rocksdb({
	stateDir: './state', // Directory of the RocksDB database
	writeBufferSize: 64 * 1024 * 1024, // Optional: bytes collected in memory before a file is written (default 16 MiB)
	compression: 'zstd', // Optional: 'none' | 'lz4' | 'zstd' | 'snappy' (default 'lz4')
	maxOpenFiles: 4096, // Optional: data files kept open (default: from the process limit)
})
```

| Option            | Description                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------------- |
| `stateDir`        | Directory of the database. Created if missing; its parent must exist. One process at a time can have it open. |
| `writeBufferSize` | Bytes of writes collected in memory before they are written to a file (default 16 MiB).                       |
| `compression`     | Compression of the data files: `'none'`, `'lz4'` (default), `'zstd'` or `'snappy'`.                           |
| `maxOpenFiles`    | Data files kept open. Derived from the process's open-file limit by default.                                  |

All stores of an application, and every partition of each, share one RocksDB database. Memory and file settings therefore apply to the application as a whole and do not grow with the number of stores or partitions.

## Durability and Restarts

- Every store is backed by its changelog topic, as with any provider. The local database is a cache of it that survives restarts.
- Flow records a checkpoint per changelog partition and syncs RocksDB's write-ahead log before trusting it. After a restart, restoration replays only what was written after the checkpoint.
- After a crash RocksDB recovers a prefix of the writes. A checkpoint is always written after the data it stands for, so it never survives without that data.
- Deleting `stateDir` is safe: the state is rebuilt from the changelog on the next start.

## Custom Engines

The store logic used by this package and by the LMDB provider is exported from `@kafkats/flow/state`: `OrderedKeyValueStore`, `OrderedWindowStore`, `OrderedSessionStore` and `OrderedChangelogCheckpointStore` implement flow's store interfaces on top of an `OrderedByteStore`, a map of byte keys to byte values that iterates in byte order. A provider for another engine only has to implement that interface.
