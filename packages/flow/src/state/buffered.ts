import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, WindowStore, WindowedKey } from '@/state.js'
import {
	currentTransactionScope,
	currentWorkerScope,
	type TransactionScope,
	type TransactionalBuffer,
} from '@/runtime/context.js'

type Buffered<SK> = { key: SK; keyBytes: Buffer; value: Buffer | null }

/**
 * Write buffer that makes local state transactional under exactly-once processing.
 *
 * While a record is processed inside a Kafka transaction, writes are held in memory (and logged to
 * the changelog inside the transaction) instead of touching the underlying store. Reads see the
 * buffered writes. When the transaction commits the buffer is applied to the store; when it aborts
 * the buffer is dropped. The underlying store therefore only ever contains committed state:
 *
 * - an aborted transaction cannot leave behind updates that get applied a second time on retry
 * - a crash at any point leaves the store at (or behind) the committed changelog, never ahead of
 *   it, so replaying the changelog from the checkpoint always converges on the committed state
 *
 * Outside a transaction (at-least-once mode, restoration) writes go straight through.
 */
abstract class BufferedStoreBase<SK, V, S extends KeyValueStore<SK, V>> implements TransactionalBuffer {
	/**
	 * Buffered writes per transaction scope (i.e. per worker). A store shared by several workers
	 * must never let one worker's commit apply, or its abort drop, another worker's writes.
	 */
	private readonly buffers = new Map<TransactionScope, Map<string, Buffered<SK>>>()

	/** The buffer of the worker processing the current record, if it has one. */
	protected get buffer(): Map<string, Buffered<SK>> | undefined {
		if (this.buffers.size === 0) return undefined
		const scope = currentWorkerScope()
		return scope ? this.buffers.get(scope) : undefined
	}

	private bufferFor(scope: TransactionScope): Map<string, Buffered<SK>> {
		let buffer = this.buffers.get(scope)
		if (!buffer) {
			buffer = new Map()
			this.buffers.set(scope, buffer)
		}
		scope.add(this)
		return buffer
	}

	constructor(
		readonly inner: S,
		protected readonly storeKeyCodec: Codec<SK>,
		protected readonly valueCodec: Codec<V>
	) {}

	get name(): string {
		return this.inner.name
	}

	async get(key: SK): Promise<V | undefined> {
		const buffer = this.buffer
		if (buffer && buffer.size > 0) {
			const hit = buffer.get(this.storeKeyCodec.encode(key).toString('hex'))
			if (hit) {
				return hit.value === null ? undefined : this.valueCodec.decode(hit.value)
			}
		}
		return this.inner.get(key)
	}

	async put(key: SK, value: V): Promise<void> {
		const scope = currentTransactionScope()
		if (!scope) {
			await this.inner.put(key, value)
			return
		}
		const keyBytes = this.storeKeyCodec.encode(key)
		// Buffer the serialized value so later in-place mutation of the object can't leak in.
		this.bufferFor(scope).set(keyBytes.toString('hex'), { key, keyBytes, value: this.valueCodec.encode(value) })
	}

	async delete(key: SK): Promise<void> {
		const scope = currentTransactionScope()
		if (!scope) {
			await this.inner.delete(key)
			return
		}
		const keyBytes = this.storeKeyCodec.encode(key)
		this.bufferFor(scope).set(keyBytes.toString('hex'), { key, keyBytes, value: null })
	}

	all(): AsyncIterable<[SK, V]> {
		return this.merged(this.inner.all(), () => true)
	}

	range(from: SK, to: SK): AsyncIterable<[SK, V]> {
		const fromBytes = this.storeKeyCodec.encode(from)
		const toBytes = this.storeKeyCodec.encode(to)
		return this.merged(
			this.inner.range(from, to),
			entry => Buffer.compare(entry.keyBytes, fromBytes) >= 0 && Buffer.compare(entry.keyBytes, toBytes) <= 0,
			true
		)
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
		this.buffers.clear()
		return this.inner.close()
	}

	async commitBuffer(scope: TransactionScope): Promise<void> {
		const buffer = this.buffers.get(scope)
		if (!buffer) return
		this.buffers.delete(scope)
		for (const entry of buffer.values()) {
			if (entry.value === null) {
				await this.inner.delete(entry.key)
			} else {
				await this.inner.put(entry.key, this.valueCodec.decode(entry.value))
			}
		}
	}

	discardBuffer(scope?: TransactionScope): void {
		if (scope) {
			this.buffers.delete(scope)
		} else {
			this.buffers.clear()
		}
	}

	/**
	 * Overlay buffered writes on a scan of the underlying store. `matches` must select the same
	 * entries the scan would have returned had the buffered writes already been applied.
	 */
	protected async *merged(
		source: AsyncIterable<[SK, V]>,
		matches: (entry: Buffered<SK>) => boolean,
		sorted = false
	): AsyncIterable<[SK, V]> {
		const buffer = this.buffer
		if (!buffer || buffer.size === 0) {
			yield* source
			return
		}

		// Snapshot so writes made by the caller while iterating don't disturb the scan.
		const overlay = new Map(buffer)
		const results: Array<{ keyBytes: Buffer; pair: [SK, V] }> = []
		for await (const pair of source) {
			const keyBytes = this.storeKeyCodec.encode(pair[0])
			if (overlay.has(keyBytes.toString('hex'))) continue
			results.push({ keyBytes, pair })
		}
		for (const entry of overlay.values()) {
			if (entry.value === null || !matches(entry)) continue
			results.push({ keyBytes: entry.keyBytes, pair: [entry.key, this.valueCodec.decode(entry.value)] })
		}
		if (sorted) {
			results.sort((a, b) => Buffer.compare(a.keyBytes, b.keyBytes))
		}
		for (const result of results) {
			yield result.pair
		}
	}
}

/** Keys (as seen through the buffer) whose window ended before the cutoff. */
async function expiredWindowedKeys<K, V>(
	store: { all(): AsyncIterable<[WindowedKey<K>, V]> },
	cutoff: number
): Promise<WindowedKey<K>[]> {
	const expired: WindowedKey<K>[] = []
	for await (const [key] of store.all()) {
		if (key.windowEnd < cutoff) expired.push(key)
	}
	return expired
}

export class BufferedKeyValueStore<K, V>
	extends BufferedStoreBase<K, V, KeyValueStore<K, V>>
	implements KeyValueStore<K, V> {}

export class BufferedWindowStore<K, V>
	extends BufferedStoreBase<WindowedKey<K>, V, WindowStore<K, V>>
	implements WindowStore<K, V>
{
	constructor(
		inner: WindowStore<K, V>,
		storeKeyCodec: Codec<WindowedKey<K>>,
		valueCodec: Codec<V>,
		private readonly userKeyCodec: Codec<K>,
		private readonly retentionMs: number
	) {
		super(inner, storeKeyCodec, valueCodec)
	}

	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.userKeyCodec.encode(key)
		return this.merged(
			this.inner.fetch(key, timeFrom, timeTo),
			entry =>
				entry.key.windowStart >= timeFrom &&
				entry.key.windowStart <= timeTo &&
				this.userKeyCodec.encode(entry.key.key).equals(keyBytes)
		)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.merged(
			this.inner.fetchAll(timeFrom, timeTo),
			entry => entry.key.windowStart >= timeFrom && entry.key.windowStart <= timeTo
		)
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const fromBytes = this.userKeyCodec.encode(keyFrom)
		const toBytes = this.userKeyCodec.encode(keyTo)
		return this.merged(this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo), entry => {
			if (entry.key.windowStart < timeFrom || entry.key.windowStart > timeTo) return false
			const bytes = this.userKeyCodec.encode(entry.key.key)
			return Buffer.compare(bytes, fromBytes) >= 0 && Buffer.compare(bytes, toBytes) <= 0
		})
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		if (!currentTransactionScope()) {
			return this.inner.expireOldWindows(currentTime)
		}
		// Inside a transaction expiry must be buffered like any other delete: applied directly,
		// an abort would have removed committed windows that the replay still needs.
		const expired = await expiredWindowedKeys(this, currentTime - this.retentionMs)
		for (const key of expired) {
			await this.delete(key)
		}
		return expired.length
	}
}

export class BufferedSessionStore<K, V>
	extends BufferedStoreBase<WindowedKey<K>, V, SessionStore<K, V>>
	implements SessionStore<K, V>
{
	constructor(
		inner: SessionStore<K, V>,
		storeKeyCodec: Codec<WindowedKey<K>>,
		valueCodec: Codec<V>,
		private readonly userKeyCodec: Codec<K>,
		private readonly retentionMs: number
	) {
		super(inner, storeKeyCodec, valueCodec)
	}

	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.userKeyCodec.encode(key)
		return this.merged(
			this.inner.findSessions(key, earliestStart, latestEnd),
			entry =>
				entry.key.windowStart <= latestEnd &&
				entry.key.windowEnd >= earliestStart &&
				this.userKeyCodec.encode(entry.key.key).equals(keyBytes)
		)
	}

	async remove(key: K): Promise<void> {
		if (!currentTransactionScope()) {
			await this.inner.remove(key)
			return
		}
		const keyBytes = this.userKeyCodec.encode(key)
		const toDelete: WindowedKey<K>[] = []
		for await (const [windowedKey] of this.all()) {
			if (this.userKeyCodec.encode(windowedKey.key).equals(keyBytes)) {
				toDelete.push(windowedKey)
			}
		}
		for (const windowedKey of toDelete) {
			await this.delete(windowedKey)
		}
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		if (!currentTransactionScope()) {
			return this.inner.expireOldSessions(currentTime)
		}
		// See BufferedWindowStore.expireOldWindows.
		const expired = await expiredWindowedKeys(this, currentTime - this.retentionMs)
		for (const key of expired) {
			await this.delete(key)
		}
		return expired
	}
}
