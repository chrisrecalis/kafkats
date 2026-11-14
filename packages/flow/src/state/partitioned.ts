import type { KeyValueStore, SessionStore, WindowStore, WindowedKey } from '@/state.js'
import { recordContextStorage, type TransactionalBuffer } from '@/runtime/context.js'

/** The layers making up one partition of a state store. */
export interface StoreStack<S, R> {
	/** What processors talk to (changelog logging + transactional buffering on top of `raw`). */
	store: S
	/** The provider's store. Restoration writes here directly. */
	raw: R
	/** Transactional buffer layer of this partition. */
	buffer: TransactionalBuffer
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyKV = KeyValueStore<any, any>

/** Name of the provider-level store backing one partition of a logical store. */
export function partitionStoreName(storeName: string, partition: number): string {
	return `${storeName}.p${partition}`
}

/**
 * The per-partition instances of one logical state store.
 *
 * Like a Kafka Streams task, each input partition owns its own slice of state. That is what makes
 * state safe to move between instances: partition N's state is exactly what changelog partition N
 * rebuilds, it can be wiped and restored without touching other partitions, and it can be dropped
 * when the partition is revoked instead of lingering as stale data.
 */
export class StorePartitions<S extends AnyKV, R extends AnyKV> {
	private readonly stacks = new Map<number, { stack: StoreStack<S, R>; ready: Promise<void> }>()

	constructor(
		readonly name: string,
		private readonly factory: (partition: number) => StoreStack<S, R>,
		/** When true there is a single app-wide instance instead of one per partition. */
		private readonly shared = false
	) {}

	/** Get (creating and initializing if needed) the stack for a partition. */
	async forPartition(requested: number): Promise<StoreStack<S, R>> {
		const partition = this.shared ? 0 : requested
		let entry = this.stacks.get(partition)
		if (!entry) {
			const stack = this.factory(partition)
			entry = { stack, ready: stack.raw.init() }
			this.stacks.set(partition, entry)
		}
		await entry.ready
		return entry.stack
	}

	/** The store for the partition of the record currently being processed. */
	async current(): Promise<S> {
		const ctx = recordContextStorage.getStore()
		if (this.shared) {
			return (await this.forPartition(0)).store
		}
		if (!ctx) {
			throw new Error(
				`State store '${this.name}' was written outside of record processing; ` +
					'writes need a record context to select the state partition'
			)
		}
		return (await this.forPartition(ctx.partition)).store
	}

	/** The current record's store, or null when called outside of record processing. */
	async currentOrNull(): Promise<S | null> {
		if (this.shared) return (await this.forPartition(0)).store
		const ctx = recordContextStorage.getStore()
		if (!ctx) return null
		return (await this.forPartition(ctx.partition)).store
	}

	/** Stores of every materialized partition (for reads outside of record processing). */
	async allStores(): Promise<S[]> {
		const stores: S[] = []
		for (const partition of [...this.stacks.keys()].sort((a, b) => a - b)) {
			stores.push((await this.forPartition(partition)).store)
		}
		return stores
	}

	materializedPartitions(): number[] {
		return [...this.stacks.keys()]
	}

	/** Forget a partition's in-process store instance (its contents are left to the caller). */
	forget(partition: number): void {
		this.stacks.delete(partition)
	}
}

/** Remove every entry from a store. */
export async function clearStore<K, V>(store: KeyValueStore<K, V>): Promise<void> {
	const keys: K[] = []
	for await (const [key] of store.all()) {
		keys.push(key)
	}
	for (const key of keys) {
		await store.delete(key)
	}
}

async function* concat<T>(
	partitions: StorePartitions<never, never>,
	scan: (store: never) => AsyncIterable<T>
): AsyncIterable<T> {
	const current = await partitions.currentOrNull()
	if (current) {
		yield* scan(current)
		return
	}
	for (const store of await partitions.allStores()) {
		yield* scan(store)
	}
}

/**
 * KeyValueStore facade that routes every call to the state partition of the record currently
 * being processed. Reads made outside of record processing see all local partitions.
 */
export class PartitionedKeyValueStore<K, V> implements KeyValueStore<K, V> {
	constructor(readonly partitions: StorePartitions<KeyValueStore<K, V>, KeyValueStore<K, V>>) {}

	get name(): string {
		return this.partitions.name
	}

	async get(key: K): Promise<V | undefined> {
		const current = await this.partitions.currentOrNull()
		if (current) return current.get(key)
		for (const store of await this.partitions.allStores()) {
			const value = await store.get(key)
			if (value !== undefined) return value
		}
		return undefined
	}

	async put(key: K, value: V): Promise<void> {
		await (await this.partitions.current()).put(key, value)
	}

	async delete(key: K): Promise<void> {
		await (await this.partitions.current()).delete(key)
	}

	all(): AsyncIterable<[K, V]> {
		return concat(this.partitions as never, (store: KeyValueStore<K, V>) => store.all())
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		return concat(this.partitions as never, (store: KeyValueStore<K, V>) => store.range(from, to))
	}

	async approximateNumEntries(): Promise<number> {
		let total = 0
		for (const store of await this.partitions.allStores()) {
			total += await store.approximateNumEntries()
		}
		return total
	}

	async init(): Promise<void> {
		// Partitions are initialized lazily, when first assigned or used.
	}

	async flush(): Promise<void> {
		for (const store of await this.partitions.allStores()) {
			await store.flush()
		}
	}

	async close(): Promise<void> {
		// The provider owns and closes the underlying stores.
	}
}

export class PartitionedWindowStore<K, V> implements WindowStore<K, V> {
	constructor(readonly partitions: StorePartitions<WindowStore<K, V>, WindowStore<K, V>>) {}

	get name(): string {
		return this.partitions.name
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		const current = await this.partitions.currentOrNull()
		if (current) return current.get(key)
		for (const store of await this.partitions.allStores()) {
			const value = await store.get(key)
			if (value !== undefined) return value
		}
		return undefined
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await (await this.partitions.current()).put(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		await (await this.partitions.current()).delete(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: WindowStore<K, V>) => store.all())
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: WindowStore<K, V>) => store.range(from, to))
	}

	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: WindowStore<K, V>) => store.fetch(key, timeFrom, timeTo))
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: WindowStore<K, V>) => store.fetchAll(timeFrom, timeTo))
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: WindowStore<K, V>) =>
			store.fetchRange(keyFrom, keyTo, timeFrom, timeTo)
		)
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		// Stream time is per partition, so expiry only ever applies to the current partition.
		return (await this.partitions.current()).expireOldWindows(currentTime)
	}

	async approximateNumEntries(): Promise<number> {
		let total = 0
		for (const store of await this.partitions.allStores()) {
			total += await store.approximateNumEntries()
		}
		return total
	}

	async init(): Promise<void> {}

	async flush(): Promise<void> {
		for (const store of await this.partitions.allStores()) {
			await store.flush()
		}
	}

	async close(): Promise<void> {}
}

export class PartitionedSessionStore<K, V> implements SessionStore<K, V> {
	constructor(readonly partitions: StorePartitions<SessionStore<K, V>, SessionStore<K, V>>) {}

	get name(): string {
		return this.partitions.name
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		const current = await this.partitions.currentOrNull()
		if (current) return current.get(key)
		for (const store of await this.partitions.allStores()) {
			const value = await store.get(key)
			if (value !== undefined) return value
		}
		return undefined
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await (await this.partitions.current()).put(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		await (await this.partitions.current()).delete(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: SessionStore<K, V>) => store.all())
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: SessionStore<K, V>) => store.range(from, to))
	}

	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return concat(this.partitions as never, (store: SessionStore<K, V>) =>
			store.findSessions(key, earliestStart, latestEnd)
		)
	}

	async remove(key: K): Promise<void> {
		await (await this.partitions.current()).remove(key)
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		return (await this.partitions.current()).expireOldSessions(currentTime)
	}

	async approximateNumEntries(): Promise<number> {
		let total = 0
		for (const store of await this.partitions.allStores()) {
			total += await store.approximateNumEntries()
		}
		return total
	}

	async init(): Promise<void> {}

	async flush(): Promise<void> {
		for (const store of await this.partitions.allStores()) {
			await store.flush()
		}
	}

	async close(): Promise<void> {}
}
