# Configuration

## Basic Setup

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
```

## Options

```typescript
lmdb({
	// Required: Directory for LMDB files
	stateDir: './state',

	// Optional: Maximum database size (default: 1GB)
	mapSize: 1024 * 1024 * 1024,

	// Optional: Maximum number of named databases (default: 4096; one is used per state store partition)
	maxDbs: 4096,
})
```

| Option     | Type     | Default              | Description                       |
| ---------- | -------- | -------------------- | --------------------------------- |
| `stateDir` | `string` | -                    | Directory for LMDB database files |
| `mapSize`  | `number` | `1024 * 1024 * 1024` | Maximum database size in bytes    |
| `maxDbs`   | `number` | `4096`               | Maximum number of named databases |

### stateDir

Directory where LMDB stores its files:

```typescript
lmdb({ stateDir: './data/kafka-state' })
```

The directory will be created if it doesn't exist. Structure:

```
./data/kafka-state/
├── data.mdb      # Main database file
└── lock.mdb      # Lock file
```

### mapSize

Maximum size of the database. LMDB pre-allocates virtual address space:

```typescript
// 1GB (default)
lmdb({ stateDir: './state', mapSize: 1024 * 1024 * 1024 })

// 10GB for larger state
lmdb({ stateDir: './state', mapSize: 10 * 1024 * 1024 * 1024 })

// 100GB for very large state
lmdb({ stateDir: './state', mapSize: 100n * 1024n * 1024n * 1024n })
```

::: tip
Start with a generous mapSize. It's virtual memory, not actual disk usage. Increasing it later requires a restart.
:::

### maxDbs

Maximum number of named databases:

```typescript
// 4096 databases (default)
lmdb({ stateDir: './state' })

// More for many stores or partitions
lmdb({ stateDir: './state', maxDbs: 16384 })
```

Every state store has one database per task partition (`<applicationId>.<storeName>.p<partition>`), so count:

- every store of the topology (KTable materializations, aggregations, window, session, suppression and join stores, `Stores` definitions) times the input partitions an instance may hold. A partition that moved to another instance keeps its database open (cleared after `stateCleanupDelayMs`), so size for every partition an instance held since it started, up to all of them;
- one database for the changelog checkpoints;
- one per global table.

Six stores over 32 input partitions need up to 6 × 32 + 1 = 193 databases on the instance that ends up holding every partition. When the limit is reached, opening the next store fails with `MDB_DBS_FULL` during an assignment.

## Environment-Based Configuration

```typescript
import { flow, inMemory } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'

const stateProvider =
	process.env.NODE_ENV === 'production'
		? lmdb({
				stateDir: process.env.STATE_DIR || '/var/lib/kafka-state',
				mapSize: 10 * 1024 * 1024 * 1024,
			})
		: inMemory()

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: stateProvider,
})
```

## Directory Structure

Organize state by application:

```
/var/lib/kafka-state/
├── order-processor/
│   ├── data.mdb
│   └── lock.mdb
├── user-analytics/
│   ├── data.mdb
│   └── lock.mdb
└── inventory-tracker/
    ├── data.mdb
    └── lock.mdb
```

```typescript
const app = flow({
	applicationId: 'order-processor',
	client: { clientId: 'order-processor', brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({
		stateDir: `/var/lib/kafka-state/${applicationId}`,
	}),
})
```

## Docker Configuration

Mount a volume for state persistence:

```yaml
# docker-compose.yml
services:
    stream-processor:
        image: my-app:latest
        volumes:
            - kafka-state:/var/lib/kafka-state
        environment:
            - STATE_DIR=/var/lib/kafka-state

volumes:
    kafka-state:
```

## Kubernetes Configuration

Use a PersistentVolumeClaim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: kafka-state-pvc
spec:
    accessModes:
        - ReadWriteOnce
    resources:
        requests:
            storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
    name: stream-processor
spec:
    replicas: 1 # Must be 1 for RWO PVC
    template:
        spec:
            containers:
                - name: app
                  volumeMounts:
                      - name: state
                        mountPath: /var/lib/kafka-state
            volumes:
                - name: state
                  persistentVolumeClaim:
                      claimName: kafka-state-pvc
```

::: warning Single Writer
LMDB supports single-writer, multiple-reader. Don't run multiple instances with the same stateDir.
:::

## Cleanup

Remove old state:

```bash
# Stop the application first
rm -rf ./state/data.mdb ./state/lock.mdb
```

State will be rebuilt from Kafka changelog topics on next start.
