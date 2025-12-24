# @kafkats/flow-state-lmdb

LMDB-backed persistent state stores for @kafkats/flow.

## Features

- **Persistent Storage** - State survives restarts
- **High Performance** - Memory-mapped, zero-copy reads
- **ACID Transactions** - Consistent state updates
- **Low Memory** - Data lives on disk, cached by OS
- **Battle-tested** - LMDB powers OpenLDAP, used by many projects

## Installation

```bash
pnpm add @kafkats/flow-state-lmdb
```

::: warning Native Dependencies
This package includes native bindings. Ensure you have build tools installed.
:::

## Quick Example

```typescript
import { flow } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({
		stateDir: './state',
	}),
})

// Aggregations now persist to disk
app.stream(clicks)
	.groupByKey()
	.count() // Stored in LMDB
	.toStream()
	.to(countsTopic)

await app.start()
```

## When to Use

| Use Case             | Recommendation      |
| -------------------- | ------------------- |
| Development          | In-memory (default) |
| Testing              | In-memory           |
| Production           | LMDB                |
| Large state          | LMDB                |
| Fast restarts needed | LMDB                |

## Store Types

LMDB provides all three store types:

- **LMDBKeyValueStore** - For tables and aggregations
- **LMDBWindowStore** - For windowed aggregations
- **LMDBSessionStore** - For session windows

## Next Steps

- [Configuration](/flow-state-lmdb/configuration) - Setup options
- [Store Types](/flow-state-lmdb/stores) - Detailed store documentation
