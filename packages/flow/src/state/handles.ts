import { IllegalStateError } from '@/api/errors.js'
import type { KeyValueStore, SessionStore, StateStore, WindowedKey, WindowStore } from '@/state.js'
import { ClockedSessionStore, ClockedWindowStore } from '@/state/time-clipping.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/**
 * A view of a task's store whose writes reject with IllegalStateError once `closed()` holds, so that nothing of a
 * closed task incarnation changes a store another incarnation may own by then. Reads pass through. The store's
 * lifecycle belongs to flow (the StateDirectory initializes it, the provider closes it): `init()` and `close()` reject.
 */
export function storeHandle(
	store: StateStore,
	kind: StoreBuilder['kind'],
	closed: () => boolean,
	owner: string
): StateStore {
	// The clipping layer of a store with a clock stays on top (6.2): reads are clipped, expiry follows the clock.
	if (store instanceof ClockedWindowStore) {
		return store.over(inner => new WindowHandle(inner, closed, owner))
	}
	if (store instanceof ClockedSessionStore) {
		return store.over(inner => new SessionHandle(inner, closed, owner))
	}
	switch (kind) {
		case 'key-value':
			return new KeyValueHandle(store as KeyValueStore<unknown, unknown>, closed, owner)
		case 'window':
			return new WindowHandle(store as WindowStore<unknown, unknown>, closed, owner)
		case 'session':
			return new SessionHandle(store as SessionStore<unknown, unknown>, closed, owner)
	}
}

class KeyValueHandle<K, V, S extends KeyValueStore<K, V>> implements KeyValueStore<K, V> {
	constructor(
		protected readonly inner: S,
		protected readonly closed: () => boolean,
		private readonly owner: string
	) {}

	get name(): string {
		return this.inner.name
	}

	get(key: K): Promise<V | undefined> {
		return this.inner.get(key)
	}

	put(key: K, value: V): Promise<void> {
		return this.closed() ? this.rejectClosed() : this.inner.put(key, value)
	}

	delete(key: K): Promise<void> {
		return this.closed() ? this.rejectClosed() : this.inner.delete(key)
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

	flush(): Promise<void> {
		return this.inner.flush()
	}

	init(): Promise<void> {
		return this.rejectLifecycle('init')
	}

	close(): Promise<void> {
		return this.rejectLifecycle('close')
	}

	protected rejectClosed(): Promise<never> {
		return Promise.reject(
			new IllegalStateError(`State store '${this.name}' of ${this.owner} was written after it closed`)
		)
	}

	private rejectLifecycle(method: string): Promise<never> {
		return Promise.reject(new IllegalStateError(`${method}() of state store '${this.name}' is managed by flow`))
	}
}

class WindowHandle<K, V> extends KeyValueHandle<WindowedKey<K>, V, WindowStore<K, V>> implements WindowStore<K, V> {
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
		return this.closed() ? this.rejectClosed() : this.inner.expireOldWindows(currentTime)
	}
}

class SessionHandle<K, V> extends KeyValueHandle<WindowedKey<K>, V, SessionStore<K, V>> implements SessionStore<K, V> {
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.findSessions(key, earliestStart, latestEnd)
	}

	remove(key: K): Promise<void> {
		return this.closed() ? this.rejectClosed() : this.inner.remove(key)
	}

	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		return this.closed() ? this.rejectClosed() : this.inner.expireOldSessions(currentTime)
	}
}
