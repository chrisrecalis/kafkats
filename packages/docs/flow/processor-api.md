# Processor API

The DSL covers the common operations. For anything else, `process()` and `processValues()` run your own processors inside the topology, and `KTable.transformValues()` runs your own value transformers on a table. Processors can keep state in typed stores, schedule punctuators on stream time or wall-clock time, and forward any number of records. This is Kafka Streams' Processor API, with TypeScript functions and options objects instead of builder classes.

## A Processor

```typescript
import { StreamRecord, type Processor, type ProcessorContext } from '@kafkats/flow'

/** Forwards a record only when its value differs from the key's previous one. */
function changesOnly(): Processor<string | null, number | null, string, number> {
	let context!: ProcessorContext<string, number>
	const last = new Map<string, number>()
	return {
		init(ctx) {
			context = ctx
		},
		async process(record) {
			if (record.key === null || record.value === null) return
			if (last.get(record.key) === record.value) return
			last.set(record.key, record.value)
			await context.forward(new StreamRecord(record.key, record.value, record.timestamp, record.headers))
		},
	}
}

app.stream('readings', { key: string(), value: json<number>() })
	.process(changesOnly)
	.to('changes', { key: string(), value: json() })
```

(The `Map` above is for illustration: state that must survive restarts and rebalances belongs in a [state store](#state-stores).)

- `process(supplier, options)` takes a **supplier**: a function returning a new processor each time it is called. Every task creates its own processor with it, calls `init(context)`, then `process(record)` for each record, and `close()` when the task closes. Flow calls the supplier twice when the processor is registered and throws `TopologyError` if it returns the same object.
- `init`, `process` and `close` may be synchronous or return a promise. A task processes its records one at a time: the next record waits until `process()` (and everything it forwarded) settled.
- `process()` may change keys and values, so downstream knows neither codec and a key-based operation repartitions first.

## Records

Processors receive and forward `StreamRecord`s, immutable records with a key, a value, a timestamp (milliseconds) and headers:

```typescript
const record = new StreamRecord('key', { amount: 1 }, Date.now(), { source: Buffer.from('api') })
record.withKey('other').withValue({ amount: 2 }).withTimestamp(0).withHeaders({})
```

`context.recordMetadata()` gives the topic, partition and offset of the input record the task is processing (`null` in punctuators).

## The Processor Context

| Member                                     | Description                                                                                        |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `forward(record, childName?)`              | Sends a record to the processor's children (or to one of them by name), depth-first.               |
| `schedule(intervalMs, type, fn)`           | Schedules a punctuator; returns a `Cancellable`.                                                   |
| `getStateStore(definition \| name)`        | A state store the processor is connected to (typed when given its definition).                     |
| `commit()`                                 | Requests a commit of every task at the next commit point, without waiting for the commit interval. |
| `recordMetadata()`                         | Topic, partition, offset of the input record; `null` in `init()` and punctuators.                  |
| `currentStreamTimeMs()`                    | The task's stream time; `-1` while unknown.                                                        |
| `currentSystemTimeMs()`                    | The wall clock, as of the start of the current processing slice.                                   |
| `applicationId`, `taskId`, `processorName` | Where the processor runs; `taskId` is `{ subtopology, partition, key }`.                           |

### Forwarding

`forward()` returns `void` when the children processed the record synchronously and a promise otherwise. **Await or return it** before the next `forward()` and before `process()` or the punctuator settles. Forwarding from `init()` or `close()`, while a previous forward of the same processor is pending, or after `process()` settled, fails with an error.

```typescript
async process(record) {
	for (const item of record.value!.items) {
		await context.forward(record.withKey(item.sku).withValue(item))
	}
}
```

### Punctuators

```typescript
init(ctx) {
	// Every 10 s of stream time: driven by record timestamps
	ctx.schedule(10_000, 'stream-time', async streamTime => { /* ... */ })
	// Every 10 s of wall-clock time: runs even without input
	const heartbeat = ctx.schedule(10_000, 'wall-clock', async now => {
		await ctx.forward(new StreamRecord('heartbeat', now, now))
	})
	// heartbeat.cancel() stops it, also from inside the punctuator
}
```

- **Stream time** punctuators fire as the task's stream time advances, first once stream time is known; missed intervals are skipped, not replayed. A punctuator receives the current stream time, not the due time.
- **Wall-clock** punctuators first fire one interval after they were scheduled, and receive the wall-clock time.
- Punctuators may forward records (give them a timestamp explicitly) and use stores. They run between records, never during one, and their errors go to the [processing exception handler](/flow/error-handling#processing-exceptions) with a `null` record.

## State Stores

`Stores` creates typed store definitions. Connect a processor to a store by naming the definition in its `stores` option, and read it with `context.getStateStore(definition)`:

```typescript
import {
	Stores,
	StreamRecord,
	type Processor,
	type ProcessorContext,
	type TimestampedKeyValueStore,
} from '@kafkats/flow'

const lastSeen = Stores.timestampedKeyValueStore({ name: 'last-seen', key: string(), value: json<Reading>() })

function dedupe(): Processor<string | null, Reading | null, string, Reading> {
	let context!: ProcessorContext<string, Reading>
	let store!: TimestampedKeyValueStore<string, Reading>
	return {
		init(ctx) {
			context = ctx
			store = ctx.getStateStore(lastSeen)
		},
		async process(record) {
			if (record.key === null || record.value === null) return
			const previous = await store.get(record.key)
			if (previous !== undefined && previous.value.id === record.value.id) return
			await store.put(record.key, { value: record.value, timestamp: record.timestamp })
			await context.forward(new StreamRecord(record.key, record.value, record.timestamp, record.headers))
		},
	}
}

readings.process(dedupe, { name: 'dedupe', stores: [lastSeen] })
```

| Factory                                    | Store                                                           |
| ------------------------------------------ | --------------------------------------------------------------- |
| `Stores.keyValueStore(options)`            | `KeyValueStore<K, V>`                                           |
| `Stores.timestampedKeyValueStore(options)` | `TimestampedKeyValueStore<K, V>`: values `{ value, timestamp }` |
| `Stores.windowStore(options)`              | `WindowStore<K, V>`                                             |
| `Stores.timestampedWindowStore(options)`   | `TimestampedWindowStore<K, V>`                                  |
| `Stores.sessionStore(options)`             | `SessionStore<K, V>`                                            |

Options: `name` (unique in the app; it keys the persisted state and names the changelog `<applicationId>-<name>-changelog`), `key` and `value` codecs, and `logging` (`true` by default, `false`, or a [`ChangelogConfig`](/flow/state-stores#per-store-settings)). Window stores also take `retention` and `windowSize`, session stores `retention`; durations are validated when the definition is made.

- Every task has its own instance of the store, for its partition, logged to the same partition of the changelog and restored from it.
- Processors connected to one store run in the same task per partition, so the topics they read must be co-partitioned.
- A definition is registered the first time it is named; naming it in several processors connects them all to one store. `app.addStateStore(definition)` registers a store ahead, and processors can then connect to it by name: `{ stores: ['last-seen'] }`. A store of the DSL can be connected by its `storeName` as well. Two different stores of one name throw `TopologyError`.
- A window or session store returns, as in Kafka Streams, only the windows ending less than `retention` before the newest window end written to it. Call `expireOldWindows(time)` / `expireOldSessions(time)` (from a stream-time punctuator, for example) to reclaim the space of older ones; the changelog drops them once they are `retention` plus one day old.
- Global tables' stores are readable from any processor by name, without connecting to them (read-only, values `{ value, timestamp }`).

Under exactly-once, store writes commit with the task's transaction; see [Exactly-once state](/flow/state-stores#exactly-once-state).

## processValues

`processValues()` runs a processor that cannot change keys. It receives and forwards `FixedKeyRecord`s, which can only be derived from the input (`withValue()`, `withTimestamp()`, `withHeaders()`), so the key codec is kept and no repartitioning is needed downstream:

```typescript
import type { FixedKeyProcessor, FixedKeyProcessorContext } from '@kafkats/flow'

function enrich(): FixedKeyProcessor<string | null, Order | null, Order & { checkedAt: number }> {
	let context!: FixedKeyProcessorContext<string | null, Order & { checkedAt: number }>
	return {
		init(ctx) {
			context = ctx
		},
		process(record) {
			if (record.value === null) return
			return context.forward(record.withValue({ ...record.value, checkedAt: context.currentSystemTimeMs() }))
		},
	}
}

orders.processValues(enrich, { name: 'enrich' })
```

## KTable.transformValues

`transformValues()` maps every update of a table with a `ValueTransformerWithKey`, which can read stores and schedule punctuators but not forward: its result is the key's new value, `null` deleting it. Unlike `mapValues()`, it also sees deletes (as a `null` value).

```typescript
import type { ValueTransformerWithKey } from '@kafkats/flow'

const versions = Stores.keyValueStore({ name: 'versions', key: string(), value: json<number>() })

function versioned(): ValueTransformerWithKey<string, Profile, Profile & { version: number }> {
	let store!: KeyValueStore<string, number>
	return {
		init(ctx) {
			store = ctx.getStateStore(versions)
		},
		async transform(key, profile) {
			if (profile === null) return null
			const version = ((await store.get(key)) ?? 0) + 1
			await store.put(key, version)
			return { ...profile, version }
		},
	}
}

const profiles = app.table('profiles', { key: string(), value: json<Profile>() })
profiles.transformValues(versioned, {
	stores: [versions],
	materialized: { storeName: 'versioned-profiles', value: json() },
})
```

Without `materialized.storeName`, the result is not stored, and a lookup of it (a join) transforms the parent's current value with a transformer of its own; the transformer then also runs in the joining task, connected to the same stores. A transformer whose results depend on more than the current value should be materialized.

## Errors

An error thrown by `process()`, a punctuator or a transformer goes to the [processing exception handler](/flow/error-handling#processing-exceptions); one thrown by `init()` or `close()` is fatal and goes to the [uncaught exception handler](/flow/error-handling#uncaught-exceptions). Using a store the processor is not connected to throws `TopologyError`, which is fatal too.

## Example: Alert After Three Failures in a Minute

```typescript
interface Login {
	success: boolean
}

const failures = Stores.windowStore({
	name: 'login-failures',
	key: string(),
	value: json<number>(),
	windowSize: '1m',
	retention: '10m',
})

function bruteForceAlerts(): Processor<string | null, Login | null, string, string> {
	let context!: ProcessorContext<string, string>
	let store!: WindowStore<string, number>
	return {
		init(ctx) {
			context = ctx
			store = ctx.getStateStore(failures)
			ctx.schedule(60_000, 'stream-time', async time => {
				await store.expireOldWindows(time)
			})
		},
		async process(record) {
			if (record.key === null || record.value === null || record.value.success) return
			const start = record.timestamp - (record.timestamp % 60_000)
			const window = { key: record.key, windowStart: start, windowEnd: start + 60_000 }
			const count = ((await store.get(window)) ?? 0) + 1
			await store.put(window, count)
			if (count === 3) {
				await context.forward(new StreamRecord(record.key, 'three failed logins in a minute', record.timestamp))
			}
		},
	}
}

app.stream('logins', { key: string(), value: json<Login>() })
	.process(bruteForceAlerts, { name: 'brute-force', stores: [failures] })
	.to('alerts', { key: string(), value: string() })
```
