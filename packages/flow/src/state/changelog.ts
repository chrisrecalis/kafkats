import type { KeyValueStore, WindowStore, SessionStore, WindowedKey } from '@/state.js'
import type { ChangelogWriter } from '@/changelog.js'
import type { WorkerContext } from '@/processors/base.js'

/**
 * Hooks that enable per-transaction write buffering ("overlay") under exactly_once.
 *
 * Under EOS, local state must not be mutated before the transaction commits: an aborted
 * transaction would otherwise leave uncommitted mutations in a persistent local store, and
 * redelivery would re-apply aggregations on the polluted value. When `getTransactionalWorker`
 * returns a worker (i.e. an EOS transaction is active for the calling context), writes are
 * buffered in an in-memory overlay keyed by that worker; reads consult the overlay first. The
 * changelog writes still ride the transaction immediately. On commit the flow flushes the overlay
 * into the local store via {@link TransactionalStateStore.flushTransactionBuffer}; on abort it is
 * discarded, leaving the local store at its last committed state.
 */
export interface EosStoreSupport {
	/** Returns the worker whose EOS transaction is currently active, or null outside one. */
	getTransactionalWorker: () => WorkerContext | null
}

/**
 * Implemented by store wrappers that buffer writes per EOS transaction. The flow calls these from
 * its commit/abort paths with the owning worker (no AsyncLocalStorage context required there).
 */
export interface TransactionalStateStore {
	/** Apply the worker's buffered writes to the local store (call only after the transaction committed). */
	flushTransactionBuffer(worker: WorkerContext): Promise<void>
	/** Drop the worker's buffered writes (transaction aborted). */
	discardTransactionBuffer(worker: WorkerContext): void
}

export function isTransactionalStateStore(store: unknown): store is TransactionalStateStore {
	return (
		typeof (store as TransactionalStateStore).flushTransactionBuffer === 'function' &&
		typeof (store as TransactionalStateStore).discardTransactionBuffer === 'function'
	)
}

/** value === undefined marks a tombstone (delete). */
type OverlayEntry<K, V> = { key: K; value: V | undefined }
type Overlay<K, V> = Map<string, OverlayEntry<K, V>>

/**
 * Merge a per-transaction overlay into an inner-store iteration: overlay tombstones hide inner
 * entries, overlay puts replace them, and overlay-only puts matching the query are appended after
 * the inner entries (append order, not serialized-key order — EOS in-transaction scans are
 * unordered; callers in this codebase collect results and do not rely on ordering).
 */
async function* mergeOverlay<K, V>(
	inner: AsyncIterable<[K, V]>,
	overlay: Overlay<K, V>,
	keyId: (key: K) => string,
	matches: (key: K) => boolean
): AsyncIterable<[K, V]> {
	const consumed = new Set<string>()
	for await (const [key, value] of inner) {
		const entry = overlay.get(keyId(key))
		if (entry) {
			consumed.add(keyId(key))
			if (entry.value !== undefined) {
				yield [key, entry.value]
			}
			continue
		}
		yield [key, value]
	}
	for (const [id, entry] of overlay) {
		if (consumed.has(id) || entry.value === undefined) continue
		if (matches(entry.key)) {
			yield [entry.key, entry.value]
		}
	}
}

/**
 * Shared overlay bookkeeping for the changelog-backed wrappers.
 */
class TransactionOverlays<K, V> {
	private readonly overlays = new Map<WorkerContext, Overlay<K, V>>()

	constructor(
		private readonly eos: EosStoreSupport | undefined,
		private readonly keyId: (key: K) => string
	) {}

	/** The active transaction's overlay for reads, or null when not inside an EOS transaction. */
	forRead(): Overlay<K, V> | null {
		const worker = this.eos?.getTransactionalWorker()
		if (!worker) return null
		return this.overlays.get(worker) ?? null
	}

	/** The active transaction's overlay for writes (created on demand), or null outside EOS. */
	forWrite(): Overlay<K, V> | null {
		const worker = this.eos?.getTransactionalWorker()
		if (!worker) return null
		let overlay = this.overlays.get(worker)
		if (!overlay) {
			overlay = new Map()
			this.overlays.set(worker, overlay)
		}
		return overlay
	}

	get(overlay: Overlay<K, V> | null, key: K): OverlayEntry<K, V> | undefined {
		return overlay?.get(this.keyId(key))
	}

	set(overlay: Overlay<K, V>, key: K, value: V | undefined): void {
		overlay.set(this.keyId(key), { key, value })
	}

	take(worker: WorkerContext): Overlay<K, V> | undefined {
		const overlay = this.overlays.get(worker)
		this.overlays.delete(worker)
		return overlay
	}

	discard(worker: WorkerContext): void {
		this.overlays.delete(worker)
	}
}

async function applyOverlay<K, V>(overlay: Overlay<K, V> | undefined, store: KeyValueStore<K, V>): Promise<void> {
	if (!overlay) return
	for (const entry of overlay.values()) {
		if (entry.value === undefined) {
			await store.delete(entry.key)
		} else {
			await store.put(entry.key, entry.value)
		}
	}
}

/**
 * A KeyValueStore wrapper that writes mutations to a changelog topic.
 *
 * All put/delete operations are written to Kafka for durability and recovery.
 * Read operations delegate directly to the inner store.
 *
 * With {@link EosStoreSupport}, writes inside an exactly_once transaction are buffered in a
 * per-transaction overlay (reads check the overlay first) and only applied to the inner store
 * once the transaction commits — see EosStoreSupport for the rationale.
 */
export class ChangelogBackedKeyValueStore<K, V> implements KeyValueStore<K, V>, TransactionalStateStore {
	/** The underlying store (exposed for restoration purposes) */
	readonly innerStore: KeyValueStore<K, V>
	private readonly overlays: TransactionOverlays<K, V>

	constructor(
		inner: KeyValueStore<K, V>,
		private readonly writer: ChangelogWriter<K, V>,
		eos?: EosStoreSupport
	) {
		this.innerStore = inner
		this.overlays = new TransactionOverlays(eos, key => writer.encodeKey(key).toString('base64'))
	}

	get name(): string {
		return this.innerStore.name
	}

	async get(key: K): Promise<V | undefined> {
		const overlay = this.overlays.forRead()
		const entry = this.overlays.get(overlay, key)
		if (entry) {
			return entry.value
		}
		return this.innerStore.get(key)
	}

	async put(key: K, value: V): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			// EOS: buffer locally, changelog write rides the transaction.
			this.overlays.set(overlay, key, value)
			await this.writer.write(key, value)
			return
		}
		// Local-first: a crash between the two must leave local stale (replayed on restart),
		// not the changelog ahead (downstream forwards aggregates that disagree with the durable log).
		await this.innerStore.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: K): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			this.overlays.set(overlay, key, undefined)
			await this.writer.writeTombstone(key)
			return
		}
		await this.innerStore.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[K, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.innerStore.all()
		}
		return mergeOverlay(
			this.innerStore.all(),
			overlay,
			key => this.writer.encodeKey(key).toString('base64'),
			() => true
		)
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.innerStore.range(from, to)
		}
		const fromBytes = this.writer.encodeKey(from)
		const toBytes = this.writer.encodeKey(to)
		return mergeOverlay(
			this.innerStore.range(from, to),
			overlay,
			key => this.writer.encodeKey(key).toString('base64'),
			key => {
				const keyBytes = this.writer.encodeKey(key)
				return Buffer.compare(keyBytes, fromBytes) >= 0 && Buffer.compare(keyBytes, toBytes) <= 0
			}
		)
	}

	async flushTransactionBuffer(worker: WorkerContext): Promise<void> {
		await applyOverlay(this.overlays.take(worker), this.innerStore)
	}

	discardTransactionBuffer(worker: WorkerContext): void {
		this.overlays.discard(worker)
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
 *
 * See {@link ChangelogBackedKeyValueStore} for the exactly_once overlay semantics; overlay
 * entries are merged into fetch/fetchAll/fetchRange scans so in-transaction reads (e.g.
 * stream-stream join lookups) see their own writes.
 */
export class ChangelogBackedWindowStore<K, V> implements WindowStore<K, V>, TransactionalStateStore {
	private readonly overlays: TransactionOverlays<WindowedKey<K>, V>

	constructor(
		private readonly inner: WindowStore<K, V>,
		private readonly writer: ChangelogWriter<WindowedKey<K>, V>,
		eos?: EosStoreSupport
	) {
		this.overlays = new TransactionOverlays(eos, key => writer.encodeKey(key).toString('base64'))
	}

	/** The underlying store (exposed so restoration can replay changelog records into it). */
	get innerStore(): WindowStore<K, V> {
		return this.inner
	}

	get name(): string {
		return this.inner.name
	}

	// The writer's key codec is windowedKeyCodec(userCodec): [userKeyBytes][16-byte window], so the
	// user-key bytes are the encoded windowed key minus its 16-byte window suffix.
	private userKeyBytes(key: K): Buffer {
		const encoded = this.writer.encodeKey({ key, windowStart: 0, windowEnd: 0 })
		return encoded.subarray(0, encoded.length - 16)
	}

	private keyId(key: WindowedKey<K>): string {
		return this.writer.encodeKey(key).toString('base64')
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		const entry = this.overlays.get(this.overlays.forRead(), key)
		if (entry) {
			return entry.value
		}
		return this.inner.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			this.overlays.set(overlay, key, value)
			await this.writer.write(key, value)
			return
		}
		// Local-first; see ChangelogBackedKeyValueStore.put for rationale.
		await this.inner.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			this.overlays.set(overlay, key, undefined)
			await this.writer.writeTombstone(key)
			return
		}
		await this.inner.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.all()
		}
		return mergeOverlay(
			this.inner.all(),
			overlay,
			k => this.keyId(k),
			() => true
		)
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.range(from, to)
		}
		const fromBytes = this.writer.encodeKey(from)
		const toBytes = this.writer.encodeKey(to)
		return mergeOverlay(
			this.inner.range(from, to),
			overlay,
			k => this.keyId(k),
			key => {
				const keyBytes = this.writer.encodeKey(key)
				return Buffer.compare(keyBytes, fromBytes) >= 0 && Buffer.compare(keyBytes, toBytes) <= 0
			}
		)
	}

	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.fetch(key, timeFrom, timeTo)
		}
		const targetKeyBytes = this.userKeyBytes(key)
		return mergeOverlay(
			this.inner.fetch(key, timeFrom, timeTo),
			overlay,
			k => this.keyId(k),
			wk =>
				this.userKeyBytes(wk.key).equals(targetKeyBytes) &&
				wk.windowStart >= timeFrom &&
				wk.windowStart <= timeTo
		)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.fetchAll(timeFrom, timeTo)
		}
		return mergeOverlay(
			this.inner.fetchAll(timeFrom, timeTo),
			overlay,
			k => this.keyId(k),
			wk => wk.windowStart >= timeFrom && wk.windowStart <= timeTo
		)
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo)
		}
		const fromBytes = this.userKeyBytes(keyFrom)
		const toBytes = this.userKeyBytes(keyTo)
		return mergeOverlay(
			this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo),
			overlay,
			k => this.keyId(k),
			wk => {
				const keyBytes = this.userKeyBytes(wk.key)
				return (
					Buffer.compare(keyBytes, fromBytes) >= 0 &&
					Buffer.compare(keyBytes, toBytes) <= 0 &&
					wk.windowStart >= timeFrom &&
					wk.windowStart <= timeTo
				)
			}
		)
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		// No per-window tombstones: the window changelog topic is configured with delete+compact and
		// a finite retention.ms tied to the store's retention (see FlowAppImpl.setupChangelog), so the
		// broker prunes records for expired windows — the same approximate, time-based cleanup Kafka
		// Streams uses for windowed changelogs. It is broker wall-clock based, not stream-time exact,
		// so restore can briefly replay a window the local store had already expired; such a window is
		// re-expired once stream time advances far enough after restore.
		// Note: expiry writes through to the inner store even inside an EOS transaction — it only
		// removes windows past retention, which is orthogonal to (and idempotent across) transactions.
		return this.inner.expireOldWindows(currentTime)
	}

	async flushTransactionBuffer(worker: WorkerContext): Promise<void> {
		await applyOverlay(this.overlays.take(worker), this.inner as unknown as KeyValueStore<WindowedKey<K>, V>)
	}

	discardTransactionBuffer(worker: WorkerContext): void {
		this.overlays.discard(worker)
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
 *
 * See {@link ChangelogBackedKeyValueStore} for the exactly_once overlay semantics; overlay
 * entries are merged into findSessions so in-transaction session merges see their own writes.
 */
export class ChangelogBackedSessionStore<K, V> implements SessionStore<K, V>, TransactionalStateStore {
	private readonly overlays: TransactionOverlays<WindowedKey<K>, V>

	constructor(
		private readonly inner: SessionStore<K, V>,
		private readonly writer: ChangelogWriter<WindowedKey<K>, V>,
		eos?: EosStoreSupport
	) {
		this.overlays = new TransactionOverlays(eos, key => writer.encodeKey(key).toString('base64'))
	}

	/** The underlying store (exposed so restoration can replay changelog records into it). */
	get innerStore(): SessionStore<K, V> {
		return this.inner
	}

	get name(): string {
		return this.inner.name
	}

	// See ChangelogBackedWindowStore.userKeyBytes.
	private userKeyBytes(key: K): Buffer {
		const encoded = this.writer.encodeKey({ key, windowStart: 0, windowEnd: 0 })
		return encoded.subarray(0, encoded.length - 16)
	}

	private keyId(key: WindowedKey<K>): string {
		return this.writer.encodeKey(key).toString('base64')
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		const entry = this.overlays.get(this.overlays.forRead(), key)
		if (entry) {
			return entry.value
		}
		return this.inner.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			this.overlays.set(overlay, key, value)
			await this.writer.write(key, value)
			return
		}
		// Local-first; see ChangelogBackedKeyValueStore.put for rationale.
		await this.inner.put(key, value)
		await this.writer.write(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		const overlay = this.overlays.forWrite()
		if (overlay) {
			this.overlays.set(overlay, key, undefined)
			await this.writer.writeTombstone(key)
			return
		}
		await this.inner.delete(key)
		await this.writer.writeTombstone(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.all()
		}
		return mergeOverlay(
			this.inner.all(),
			overlay,
			k => this.keyId(k),
			() => true
		)
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.range(from, to)
		}
		const fromBytes = this.writer.encodeKey(from)
		const toBytes = this.writer.encodeKey(to)
		return mergeOverlay(
			this.inner.range(from, to),
			overlay,
			k => this.keyId(k),
			key => {
				const keyBytes = this.writer.encodeKey(key)
				return Buffer.compare(keyBytes, fromBytes) >= 0 && Buffer.compare(keyBytes, toBytes) <= 0
			}
		)
	}

	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const overlay = this.overlays.forRead()
		if (!overlay || overlay.size === 0) {
			return this.inner.findSessions(key, earliestStart, latestEnd)
		}
		const targetKeyBytes = this.userKeyBytes(key)
		return mergeOverlay(
			this.inner.findSessions(key, earliestStart, latestEnd),
			overlay,
			k => this.keyId(k),
			// Session overlaps if it starts before latestEnd and ends after earliestStart.
			wk =>
				this.userKeyBytes(wk.key).equals(targetKeyBytes) &&
				wk.windowStart <= latestEnd &&
				wk.windowEnd >= earliestStart
		)
	}

	async remove(key: K): Promise<void> {
		// Find all sessions for this key and write tombstones
		const toDelete: WindowedKey<K>[] = []
		for await (const [windowedKey] of this.findSessions(key, 0, Number.MAX_SAFE_INTEGER)) {
			toDelete.push(windowedKey)
		}

		const overlay = this.overlays.forWrite()
		if (overlay) {
			for (const k of toDelete) {
				this.overlays.set(overlay, k, undefined)
				await this.writer.writeTombstone(k)
			}
			return
		}

		await this.inner.remove(key)

		for (const k of toDelete) {
			await this.writer.writeTombstone(k)
		}
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		// Without per-key tombstones, restoration would replay the original puts and resurrect
		// expired sessions (changelog's last value per key wins). Mirror remove(): inner deletes
		// first (local-first), then emit a tombstone for each so the durable log stays consistent.
		// Note: expiry writes through to the inner store even inside an EOS transaction (only
		// sessions past retention are removed — orthogonal to the transaction's data). If the
		// transaction aborts, its expiry tombstones abort with it and restore may resurrect the
		// expired sessions; they are re-expired once stream time advances again.
		const expired = await this.inner.expireOldSessions(currentTime)
		for (const k of expired) {
			await this.writer.writeTombstone(k)
		}
		return expired
	}

	async flushTransactionBuffer(worker: WorkerContext): Promise<void> {
		await applyOverlay(this.overlays.take(worker), this.inner as unknown as KeyValueStore<WindowedKey<K>, V>)
	}

	discardTransactionBuffer(worker: WorkerContext): void {
		this.overlays.discard(worker)
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
