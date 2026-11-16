import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, WindowStore, WindowedKey } from '@/state.js'
import {
	currentTransactionScope,
	currentWorkerScope,
	type TransactionScope,
	type TransactionalBuffer,
} from '@/runtime/context.js'

/**
 * A buffered write. Key and value are held as the bytes they had when written: the caller's objects
 * may be mutated afterwards (a downstream peek(), say), and the changelog record of the write already
 * carries those bytes. `key` is decoded from them on first use (see keyOf).
 */
type Buffered<SK> = { keyBytes: Buffer; value: Buffer | null; key?: SK }

const BUFFERED = Promise.resolve()

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
		protected readonly inner: S,
		protected readonly storeKeyCodec: Codec<SK>,
		protected readonly valueCodec: Codec<V>
	) {}

	get name(): string {
		return this.inner.name
	}

	// get/put/delete are not async: they run for every record, and only pass the call through or
	// touch the in-memory buffer. They are only called through the (async) partitioned facade.
	get(key: SK): Promise<V | undefined> {
		const buffer = this.buffer
		if (buffer && buffer.size > 0) {
			const hit = buffer.get(this.storeKeyCodec.encode(key).toString('hex'))
			if (hit) {
				return Promise.resolve(hit.value === null ? undefined : this.valueCodec.decode(hit.value))
			}
		}
		return this.inner.get(key)
	}

	put(key: SK, value: V): Promise<void> {
		const scope = currentTransactionScope()
		if (!scope) return this.inner.put(key, value)
		const keyBytes = this.storeKeyCodec.encode(key)
		this.bufferFor(scope).set(keyBytes.toString('hex'), { keyBytes, value: this.valueCodec.encode(value) })
		return BUFFERED
	}

	delete(key: SK): Promise<void> {
		const scope = currentTransactionScope()
		if (!scope) return this.inner.delete(key)
		const keyBytes = this.storeKeyCodec.encode(key)
		this.bufferFor(scope).set(keyBytes.toString('hex'), { keyBytes, value: null })
		return BUFFERED
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
		// Issue every write before waiting for any: a provider that batches the writes of an event
		// turn (LMDB commits them in one transaction) then applies the buffer in one go instead of
		// one commit per entry. The entries have distinct keys, so the order they land in is irrelevant.
		// Wait for all of them even when one fails, so that none lands after this has returned.
		const results = await Promise.allSettled(
			Array.from(buffer.values(), async entry => {
				if (entry.value === null) {
					await this.inner.delete(this.keyOf(entry))
				} else {
					await this.inner.put(this.keyOf(entry), this.valueCodec.decode(entry.value))
				}
			})
		)
		for (const result of results) {
			if (result.status === 'rejected') throw result.reason
		}
	}

	discardBuffer(scope?: TransactionScope): void {
		if (scope) {
			this.buffers.delete(scope)
		} else {
			this.buffers.clear()
		}
	}

	/** The key a buffered entry was written with. */
	protected keyOf(entry: Buffered<SK>): SK {
		return (entry.key ??= this.storeKeyCodec.decode(entry.keyBytes))
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
			results.push({ keyBytes: entry.keyBytes, pair: [this.keyOf(entry), this.valueCodec.decode(entry.value)] })
		}
		if (sorted) {
			results.sort((a, b) => Buffer.compare(a.keyBytes, b.keyBytes))
		}
		for (const result of results) {
			yield result.pair
		}
	}
}

export class BufferedKeyValueStore<K, V>
	extends BufferedStoreBase<K, V, KeyValueStore<K, V>>
	implements KeyValueStore<K, V> {}

/** Buffer of a window or session store: keys are windowed, entries expire after `retentionMs`. */
abstract class BufferedWindowedStore<K, V, S extends KeyValueStore<WindowedKey<K>, V>> extends BufferedStoreBase<
	WindowedKey<K>,
	V,
	S
> {
	constructor(
		inner: S,
		storeKeyCodec: Codec<WindowedKey<K>>,
		valueCodec: Codec<V>,
		protected readonly userKeyCodec: Codec<K>,
		protected readonly retentionMs: number
	) {
		super(inner, storeKeyCodec, valueCodec)
	}

	/**
	 * Delete, through the buffer, the `entries` (as seen through the buffer) whose key matches.
	 *
	 * Inside a transaction expiry and removal must be buffered like any other delete: applied
	 * directly, an abort would have removed committed entries that the replay still needs.
	 */
	protected async deleteEntries(
		entries: AsyncIterable<[WindowedKey<K>, V]>,
		matches: (key: WindowedKey<K>) => boolean = () => true
	): Promise<WindowedKey<K>[]> {
		const keys: WindowedKey<K>[] = []
		for await (const [key] of entries) {
			if (matches(key)) keys.push(key)
		}
		for (const key of keys) {
			await this.delete(key)
		}
		return keys
	}
}

export class BufferedWindowStore<K, V>
	extends BufferedWindowedStore<K, V, WindowStore<K, V>>
	implements WindowStore<K, V>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.userKeyCodec.encode(key)
		return this.merged(this.inner.fetch(key, timeFrom, timeTo), entry => {
			const window = this.keyOf(entry)
			return (
				window.windowStart >= timeFrom &&
				window.windowStart <= timeTo &&
				this.userKeyCodec.encode(window.key).equals(keyBytes)
			)
		})
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.merged(this.inner.fetchAll(timeFrom, timeTo), entry => {
			const { windowStart } = this.keyOf(entry)
			return windowStart >= timeFrom && windowStart <= timeTo
		})
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const fromBytes = this.userKeyCodec.encode(keyFrom)
		const toBytes = this.userKeyCodec.encode(keyTo)
		return this.merged(this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo), entry => {
			const window = this.keyOf(entry)
			if (window.windowStart < timeFrom || window.windowStart > timeTo) return false
			const bytes = this.userKeyCodec.encode(window.key)
			return Buffer.compare(bytes, fromBytes) >= 0 && Buffer.compare(bytes, toBytes) <= 0
		})
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		if (!currentTransactionScope()) {
			return this.inner.expireOldWindows(currentTime)
		}
		const cutoff = currentTime - this.retentionMs
		// A window that ended before the cutoff also started before it, and windows are ordered by
		// their start: only that part of the store is read, not the windows still in use.
		const startedBeforeCutoff = this.fetchAll(Number.MIN_SAFE_INTEGER, cutoff - 1)
		return (await this.deleteEntries(startedBeforeCutoff, window => window.windowEnd < cutoff)).length
	}
}

export class BufferedSessionStore<K, V>
	extends BufferedWindowedStore<K, V, SessionStore<K, V>>
	implements SessionStore<K, V>
{
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.userKeyCodec.encode(key)
		return this.merged(this.inner.findSessions(key, earliestStart, latestEnd), entry => {
			const session = this.keyOf(entry)
			return (
				session.windowStart <= latestEnd &&
				session.windowEnd >= earliestStart &&
				this.userKeyCodec.encode(session.key).equals(keyBytes)
			)
		})
	}

	async remove(key: K): Promise<void> {
		if (!currentTransactionScope()) {
			await this.inner.remove(key)
			return
		}
		await this.deleteEntries(this.findSessions(key, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER))
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		if (!currentTransactionScope()) {
			return this.inner.expireOldSessions(currentTime)
		}
		const cutoff = currentTime - this.retentionMs
		// Sessions are ordered by key, not by time: like the store itself, this reads all of them.
		return this.deleteEntries(this.all(), session => session.windowEnd < cutoff)
	}
}
