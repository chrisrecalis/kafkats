# @kafkats/flow-state-rocksdb

RocksDB-backed state store provider for @kafkats/flow.

## Commands

```bash
pnpm build      # Build the package
pnpm test       # Run tests (broker tests need KAFKA_BROKERS, otherwise they are skipped)
pnpm typecheck  # Type checking
```

## Overview

```typescript
import { flow } from '@kafkats/flow'
import { rocksdb } from '@kafkats/flow-state-rocksdb'

const app = flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
	stateStoreProvider: rocksdb({ stateDir: './state' }),
})
```

## Design

- **Binding**: `@harperfast/rocksdb-js` (prebuilt binaries, Node >= 22.18). The Level `rocksdb` package is discontinued.
- **Store logic is not here.** Key layouts, window/session queries, expiry and checkpoints are the `Ordered*Store` classes of `@kafkats/flow` (`packages/flow/src/state/ordered.ts`), shared with the LMDB provider. `src/rocksdb.ts` only adapts a RocksDB database to their `OrderedByteStore` interface.
- **One keyspace, a prefix per store.** All stores live in the default column family. Every key starts with a 4-byte keyspace id: 0 is the provider's own (store name -> id, next id, changelog checkpoints), each store gets the next id when it is first created. A column family per store partition was measured and rejected: creating 400 took 23 seconds.
- **Ids are assigned in the database, synchronously**, so several provider instances of one process on the same directory cannot hand out the same id. Across processes RocksDB's lock file allows one opener.
- **Durability**: `flush()` of a store or of the checkpoint store syncs the write-ahead log, which all stores and checkpoints share. A crash keeps a prefix of the writes, so a checkpoint never survives without the store data written before it.
- **`approximateNumEntries()`** is an estimate from table statistics, but 0 exactly when the store is empty (flow uses "empty" to decide whether a checkpoint can be trusted).
- `close()` closes the database; it is reopened on demand.

## Tests

The suite mirrors `flow-state-lmdb/tests` (ordering, bounds, parity with the in-memory stores, restart cycles, SIGKILL crash tests) plus `rocksdb-provider.test.ts` for the keyspace handling.
