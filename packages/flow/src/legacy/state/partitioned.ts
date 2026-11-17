import type { KeyValueStore, SessionStore, WindowStore, WindowedKey } from '@/state.js'
import { recordContextStorage, type TransactionalBuffer } from '@/legacy/runtime/context.js'

/** The layers making up one partition of a state store. */
export interface StoreStack<S, R> {
	/** What processors talk to (changelog logging + transactional buffering on top of `raw`). */
	store: S
	/** The provider's store. Restoration writes here directly. */
	raw: R
	/** Transactional buffer layer of this partition. */
	buffer: TransactionalBuffer
}

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
export class StorePartitions<S, R extends { init(): Promise<void> }> {
	private readonly stacks = new Map<number, { stack: StoreStack<S, R>; ready: Promise<void>; initialized: boolean }>()

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
			const created = { stack, ready: stack.raw.init(), initialized: false }
			created.ready.then(
				() => (created.initialized = true),
				() => {}
			)
			entry = created
			this.stacks.set(partition, entry)
		}
		await entry.ready
		return entry.stack
	}

	/**
	 * The current record's store if its partition is already initialized, else undefined (also
	 * outside of record processing). Lets the per-record path skip the asynchronous lookup.
	 */
	initializedCurrent(): S | undefined {
		const partition = this.shared ? 0 : recordContextStorage.getStore()?.partition
		if (partition === undefined) return undefined
		const entry = this.stacks.get(partition)
		return entry?.initialized ? entry.stack.store : undefined
	}

	/** The store for the partition of the record currently being processed. */
	async current(): Promise<S> {
		const store = await this.currentOrNull()
		if (!store) {
			throw new Error(
				`State store '${this.name}' was written outside of record processing; ` +
					'writes need a record context to select the state partition'
			)
		}
		return store
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

/**
 * Store facade that routes every call to the state partition of the record currently being
 * processed. Reads made outside of record processing see all local partitions.
 */
abstract class PartitionedStore<K, V, S extends KeyValueStore<K, V>> implements KeyValueStore<K, V> {
	constructor(protected readonly partitions: StorePartitions<S, S>) {}

	get name(): string {
		return this.partitions.name
	}

	async get(key: K): Promise<V | undefined> {
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.currentOrNull())
		if (current) return current.get(key)
		for (const store of await this.partitions.allStores()) {
			const value = await store.get(key)
			if (value !== undefined) return value
		}
		return undefined
	}

	async put(key: K, value: V): Promise<void> {
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.current())
		await current.put(key, value)
	}

	async delete(key: K): Promise<void> {
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.current())
		await current.delete(key)
	}

	all(): AsyncIterable<[K, V]> {
		return this.scan(store => store.all())
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		return this.scan(store => store.range(from, to))
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

	/** Scan the current record's partition, or every local partition outside of record processing. */
	protected async *scan<T>(fn: (store: S) => AsyncIterable<T>): AsyncIterable<T> {
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.currentOrNull())
		if (current) {
			yield* fn(current)
			return
		}
		for (const store of await this.partitions.allStores()) {
			yield* fn(store)
		}
	}
}

export class PartitionedKeyValueStore<K, V> extends PartitionedStore<K, V, KeyValueStore<K, V>> {}

export class PartitionedWindowStore<K, V>
	extends PartitionedStore<WindowedKey<K>, V, WindowStore<K, V>>
	implements WindowStore<K, V>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(store => store.fetch(key, timeFrom, timeTo))
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(store => store.fetchAll(timeFrom, timeTo))
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(store => store.fetchRange(keyFrom, keyTo, timeFrom, timeTo))
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		// Stream time is per partition, so expiry only ever applies to the current partition.
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.current())
		return current.expireOldWindows(currentTime)
	}
}

export class PartitionedSessionStore<K, V>
	extends PartitionedStore<WindowedKey<K>, V, SessionStore<K, V>>
	implements SessionStore<K, V>
{
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(store => store.findSessions(key, earliestStart, latestEnd))
	}

	async remove(key: K): Promise<void> {
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.current())
		await current.remove(key)
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		// Stream time is per partition, so expiry only ever applies to the current partition.
		const current = this.partitions.initializedCurrent() ?? (await this.partitions.current())
		return current.expireOldSessions(currentTime)
	}
}
