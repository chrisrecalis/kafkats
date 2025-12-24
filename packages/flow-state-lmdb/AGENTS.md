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
	maxDbs: 100, // Optional: max named databases (default 100)
})
```

## Native Dependency

This package requires the `lmdb` npm package which includes native bindings.
Prebuilt binaries are available for most platforms.
