# @kafkats/flow-state-lmdb

LMDB-backed state store provider for @kafkats/flow.

## Commands

```bash
pnpm build      # Build the package
pnpm test       # Run tests
pnpm typecheck  # Type checking
```

## Overview

Provides persistent, memory-mapped state stores using LMDB (Lightning Memory-Mapped Database).

```typescript
import { flow } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

const app = flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({ stateDir: './state' }),
})
```

## Store Types

- **LMDBKeyValueStore** - Basic key-value store
- **LMDBWindowStore** - Time-windowed state (keys ordered by time)
- **LMDBSessionStore** - Session window state (keys ordered by key then time)

## Key Ordering

Window stores use `[windowStart][windowEnd][key]` ordering for efficient time-range queries.
Session stores use `[key][windowStart][windowEnd]` ordering for efficient key-based queries.

## Configuration

```typescript
lmdb({
	stateDir: './state', // Required: directory for LMDB files
	mapSize: 1024 * 1024 * 1024, // Optional: max DB size (default 1GB)
	maxDbs: 4096, // Optional: max named databases (default 4096)
})
```

## Partitioned State

`@kafkats/flow` keeps one slice of every state store per source partition, so each slice is its own named LMDB database: `<storeName>.p<partition>` (hence the `maxDbs` default of 4096). Changelog checkpoints are kept in a separate database (`__kafkats_flow_changelog_checkpoints__`) and let a restart resume restoration incrementally. Flow ignores a checkpoint (wipes the store and rebuilds it from the changelog) when it is outside the changelog's offset range or the local store is empty.

`close()` closes the LMDB environment; it is reopened on demand, so an app using the provider can be started again after `close()`.

## Native Dependency

This package requires the `lmdb` npm package which includes native bindings.
Prebuilt binaries are available for most platforms.
