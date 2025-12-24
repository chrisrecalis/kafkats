import type { KeyValueStore, WindowStore, SessionStore, WindowedKey } from '@/state.js'
import type { ChangelogWriter } from '@/changelog.js'

/**
 * A KeyValueStore wrapper that writes mutations to a changelog topic.
 *
 * All put/delete operations are written to Kafka for durability and recovery.
 * Read operations delegate directly to the inner store.
 */
export class ChangelogBackedKeyValueStore<K, V> implements KeyValueStore<K, V> {
	/** The underlying store (exposed for restoration purposes) */
	readonly innerStore: KeyValueStore<K, V>

	constructor(
		inner: KeyValueStore<K, V>,
		private readonly writer: ChangelogWriter<K, V>
	) {
		this.innerStore = inner
	}

	get name(): string {
		return this.innerStore.name
	}

	async get(key: K): Promise<V | undefined> {
		return this.innerStore.get(key)
	}

	async put(key: K, value: V): Promise<void> {
		await this.innerStore.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: K): Promise<void> {
		await this.innerStore.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[K, V]> {
		return this.innerStore.all()
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		return this.innerStore.range(from, to)
	}

	async approximateNumEntries(): Promise<number> {
		return this.innerStore.approximateNumEntries()
	}

	async init(): Promise<void> {
		return this.innerStore.init()
	}

	async flush(): Promise<void> {
		return this.innerStore.flush()
	}

	async close(): Promise<void> {
		return this.innerStore.close()
	}
}

/**
 * A WindowStore wrapper that writes mutations to a changelog topic.
 */
export class ChangelogBackedWindowStore<K, V> implements WindowStore<K, V> {
	constructor(
		private readonly inner: WindowStore<K, V>,
		private readonly writer: ChangelogWriter<WindowedKey<K>, V>
	) {}

	get name(): string {
		return this.inner.name
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		return this.inner.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await this.inner.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		await this.inner.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.all()
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.range(from, to)
	}

	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetch(key, timeFrom, timeTo)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetchAll(timeFrom, timeTo)
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo)
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		// Note: We don't write tombstones for expired windows as they're
		// automatically cleaned up by Kafka's retention/compaction
		return this.inner.expireOldWindows(currentTime)
	}

	async approximateNumEntries(): Promise<number> {
		return this.inner.approximateNumEntries()
	}

	async init(): Promise<void> {
		return this.inner.init()
	}

	async flush(): Promise<void> {
		return this.inner.flush()
	}

	async close(): Promise<void> {
		return this.inner.close()
	}
}

/**
 * A SessionStore wrapper that writes mutations to a changelog topic.
 */
export class ChangelogBackedSessionStore<K, V> implements SessionStore<K, V> {
	constructor(
		private readonly inner: SessionStore<K, V>,
		private readonly writer: ChangelogWriter<WindowedKey<K>, V>
	) {}

	get name(): string {
		return this.inner.name
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		return this.inner.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await this.inner.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		await this.inner.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.all()
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.range(from, to)
	}

	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.findSessions(key, earliestStart, latestEnd)
	}

	async remove(key: K): Promise<void> {
		// Find all sessions for this key and write tombstones
		const toDelete: WindowedKey<K>[] = []
		for await (const [windowedKey] of this.inner.findSessions(key, 0, Number.MAX_SAFE_INTEGER)) {
			toDelete.push(windowedKey)
		}

		await this.inner.remove(key)

		for (const k of toDelete) {
			await this.writer.writeTombstone(k)
		}
	}

	async approximateNumEntries(): Promise<number> {
		return this.inner.approximateNumEntries()
	}

	async init(): Promise<void> {
		return this.inner.init()
	}

	async flush(): Promise<void> {
		return this.inner.flush()
	}

	async close(): Promise<void> {
		return this.inner.close()
	}
}
