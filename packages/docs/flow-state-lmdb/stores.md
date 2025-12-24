# Store Types

@kafkats/flow-state-lmdb provides three store types matching the flow state interfaces.

## LMDBKeyValueStore

For simple key-value storage (tables, aggregations):

```typescript
interface KeyValueStore<K, V> {
	get(key: K): Promise<V | undefined>
	put(key: K, value: V): Promise<void>
	delete(key: K): Promise<void>
	all(): AsyncIterable<{ key: K; value: V }>
	range(from: K, to: K): AsyncIterable<{ key: K; value: V }>
}
```

### Use Cases

- Table materializations
- Non-windowed aggregations
- Lookup data

### Key Format

Keys are stored as-is using the key codec:

```
[encoded_key] → [encoded_value]
```

## LMDBWindowStore

For time-windowed state:

```typescript
interface WindowStore<K, V> {
	get(key: K, windowStart: number): Promise<V | undefined>
	put(key: K, value: V, windowStart: number): Promise<void>
	fetch(
		key: K,
		from: number,
		to: number
	): AsyncIterable<{
		windowStart: number
		value: V
	}>
	fetchAll(
		from: number,
		to: number
	): AsyncIterable<{
		key: K
		windowStart: number
		value: V
	}>
}
```

### Use Cases

- Tumbling window aggregations
- Hopping window aggregations
- Sliding window aggregations

### Key Format

Keys are ordered for efficient window queries:

```
[windowStart:8bytes][windowEnd:8bytes][key] → [value]
```

This ordering enables:

- Efficient range scans by time
- Fast lookups for specific key + window
- Ordered iteration by window time

## LMDBSessionStore

For session-windowed state:

```typescript
interface SessionStore<K, V> {
	get(key: K, sessionStart: number, sessionEnd: number): Promise<V | undefined>
	put(key: K, value: V, sessionStart: number, sessionEnd: number): Promise<void>
	findSessions(
		key: K,
		from: number,
		to: number
	): AsyncIterable<{
		sessionStart: number
		sessionEnd: number
		value: V
	}>
}
```

### Use Cases

- Session window aggregations
- Activity-based grouping

### Key Format

Keys are ordered by key first, then time:

```
[key][sessionStart:8bytes][sessionEnd:8bytes] → [value]
```

This ordering enables:

- Fast session lookup for a key
- Efficient merging of adjacent sessions
- Range queries for a key's sessions

## Store Creation

Stores are created automatically by the flow runtime:

```typescript
// This creates an LMDBKeyValueStore internally
const counts = stream.groupByKey().count({
	materialized: { storeName: 'user-counts' },
})

// This creates an LMDBWindowStore internally
const windowedCounts = stream
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.count({
		materialized: { storeName: 'hourly-counts' },
	})
```

## Manual Store Access

Create stores directly for custom use:

```typescript
import { lmdb } from '@kafkats/flow-state-lmdb'
import { string, json } from '@kafkats/flow'

const provider = lmdb({ stateDir: './state' })

// KeyValue store
const kvStore = provider.createKeyValueStore('my-kv', {
	keyCodec: string(),
	valueCodec: json<MyValue>(),
})

await kvStore.put('key1', { data: 'value1' })
const value = await kvStore.get('key1')

// Window store
const windowStore = provider.createWindowStore('my-windows', {
	keyCodec: string(),
	valueCodec: json<number>(),
	windowSize: 3600000, // 1 hour in ms
})

const windowStart = Math.floor(Date.now() / 3600000) * 3600000
await windowStore.put('user1', 42, windowStart)

// Cleanup
await provider.close()
```

## Performance Characteristics

### LMDBKeyValueStore

| Operation | Complexity   | Notes           |
| --------- | ------------ | --------------- |
| get       | O(log n)     | B+ tree lookup  |
| put       | O(log n)     | Single write    |
| delete    | O(log n)     | Tombstone write |
| all       | O(n)         | Full scan       |
| range     | O(log n + k) | k = results     |

### LMDBWindowStore

| Operation | Complexity   | Notes                |
| --------- | ------------ | -------------------- |
| get       | O(log n)     | Composite key lookup |
| put       | O(log n)     | Single write         |
| fetch     | O(log n + k) | Time-range scan      |
| fetchAll  | O(log n + k) | Time-range scan      |

### LMDBSessionStore

| Operation    | Complexity   | Notes                |
| ------------ | ------------ | -------------------- |
| get          | O(log n)     | Composite key lookup |
| put          | O(log n)     | May merge sessions   |
| findSessions | O(log n + k) | Key + time scan      |

## Memory Usage

LMDB is memory-mapped:

- **Reads**: OS pages data into memory on demand
- **Writes**: Buffered then synced to disk
- **Cache**: OS manages the page cache

Monitor with:

```bash
# Database size
du -sh ./state/data.mdb

# Memory-mapped usage
cat /proc/$(pgrep -f my-app)/maps | grep data.mdb
```

## Durability

LMDB provides:

- **Atomic commits**: All or nothing
- **Crash safety**: Never corrupted on crash
- **Sync writes**: `MDB_NOSYNC` is NOT used

Data is durable immediately after `put()` returns.

## Compaction

LMDB does not automatically compact. To reclaim space:

```bash
# 1. Stop the application
# 2. Copy database with mdb_copy
mdb_copy -c ./state/data.mdb ./state/compacted.mdb

# 3. Replace original
mv ./state/compacted.mdb ./state/data.mdb

# 4. Restart application
```

For production, schedule periodic compaction during low-traffic periods.
