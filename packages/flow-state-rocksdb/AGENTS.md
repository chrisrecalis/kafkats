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
- **Store logic is not here.** Key layouts, window/session queries, expiry and checkpoints are the `Ordered*Store` classes of `@kafkats/flow` (`packages/flow/src/state/ordered.ts`), shared with the LMDB provider. This package only adapts a RocksDB database to their `OrderedByteStore` interface:
    - `src/rocksdb.ts`: the provider (opens and closes the database, creates stores).
    - `src/keyspace.ts`: which keys belong to which store, and the byte store over one store's keys.
    - `src/coalesced.ts`: runs the write-ahead log sync once for all flushes that ask during another sync.
- **One keyspace, a prefix per store.** All stores live in the default column family. Every key starts with a 4-byte keyspace id: 0 is the provider's own (store name -> id, next id, changelog checkpoints), each store gets the next id when it is first created. A column family per store partition was measured and rejected: creating 400 took 23 seconds.
- **Ids are assigned in the database, in a transaction**, so several provider instances on the same directory (in one process or in worker threads) cannot hand out the same id. Across processes RocksDB's lock file allows one opener.
- **Durability**: `flush()` of a store or of the checkpoint store syncs the write-ahead log, which all stores and checkpoints share (one sync serves every flush that arrives while another runs; `disableWAL` cannot be set). A crash keeps a prefix of the writes, so a checkpoint never survives without the store data written before it.
- **`approximateNumEntries()`** is an estimate from table statistics, but 0 exactly when the store is empty (flow uses "empty" to decide whether a checkpoint can be trusted).
- `close()` closes the database; it is reopened on demand.

## Tests

- `tests/provider-contract.test.ts` runs the shared suites of `@kafkats/flow-state-testkit` (ordering, bounds, parity with the in-memory stores, restart cycles, SIGKILL crash tests), the same ones the LMDB provider runs.
- `tests/rocksdb-provider.test.ts` covers what is specific to this provider: keyspaces, shared directories, sync coalescing, close.
