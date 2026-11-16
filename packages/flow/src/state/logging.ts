import type { ChangelogWriter } from '@/changelog/writer.js'
import type { KeyValueStore, SessionStore, WindowedKey, WindowStore } from '@/state.js'

/**
 * Logs every write of a store to its changelog. The store below is written first (local first): a crash between the
 * two leaves the local store behind the changelog, which the replay repairs, never the changelog ahead of a store that
 * disagrees with it. Changelog sends are not awaited; reads go to the store below.
 */
abstract class ChangelogBackedStore<K, V, S extends KeyValueStore<K, V>> implements KeyValueStore<K, V> {
	constructor(
		protected readonly inner: S,
		protected readonly writer: ChangelogWriter<K, V>
	) {}

	get name(): string {
		return this.inner.name
	}

	get(key: K): Promise<V | undefined> {
		return this.inner.get(key)
	}

	async put(key: K, value: V): Promise<void> {
		await this.inner.put(key, value)
		this.writer.write(key, value)
	}

	async delete(key: K): Promise<void> {
		await this.inner.delete(key)
		this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[K, V]> {
		return this.inner.all()
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		return this.inner.range(from, to)
	}

	approximateNumEntries(): Promise<number> {
		return this.inner.approximateNumEntries()
	}

	init(): Promise<void> {
		return this.inner.init()
	}

	flush(): Promise<void> {
		return this.inner.flush()
	}

	close(): Promise<void> {
		return this.inner.close()
	}
}

export class ChangelogBackedKeyValueStore<K, V> extends ChangelogBackedStore<K, V, KeyValueStore<K, V>> {}

export class ChangelogBackedWindowStore<K, V>
	extends ChangelogBackedStore<WindowedKey<K>, V, WindowStore<K, V>>
	implements WindowStore<K, V>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetch(key, timeFrom, timeTo)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetchAll(timeFrom, timeTo)
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo)
	}

	expireOldWindows(currentTime: number): Promise<number> {
		// No tombstones: the changelog is `delete,compact` with a retention tied to the store's, so the broker drops the
		// records of expired windows (by write time, like Kafka Streams' windowed changelogs).
		return this.inner.expireOldWindows(currentTime)
	}
}

export class ChangelogBackedSessionStore<K, V>
	extends ChangelogBackedStore<WindowedKey<K>, V, SessionStore<K, V>>
	implements SessionStore<K, V>
{
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.findSessions(key, earliestStart, latestEnd)
	}

	async remove(key: K): Promise<void> {
		// Every session of the key, negative times included (B11).
		const sessions: WindowedKey<K>[] = []
		for await (const [session] of this.inner.findSessions(key, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)) {
			sessions.push(session)
		}
		await this.inner.remove(key)
		for (const session of sessions) {
			this.writer.writeTombstone(session)
		}
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		// Tombstones, or a restore would replay the original puts and resurrect the expired sessions.
		const expired = await this.inner.expireOldSessions(currentTime)
		for (const session of expired) {
			this.writer.writeTombstone(session)
		}
		return expired
	}
}
