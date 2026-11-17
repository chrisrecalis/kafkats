/**
 * The buffer of a suppress processor (KS InMemoryTimeOrderedKeyValueChangeBuffer, ksdsl§12; 6.16): one entry per key,
 * evicted in `(buffer time, key bytes)` order. A later update of a buffered key replaces its value, timestamp and
 * headers but keeps its buffer time, its place and its prior value (non-resetting).
 *
 * The buffer lives in memory and is written through to a key-value store of key bytes and entry bytes in the same
 * process() call (KS logs dirty keys when the buffer is flushed at a commit; flow has no flush hook before a commit), so
 * the store, its changelog and, under exactly-once, its transactional buffer match the memory at every commit. Each new
 * processor instance (task incarnation) loads it from the store.
 */
import type { ChangelogDefaults } from '@/changelog/config.js'
import { buffer, type Codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import { bufferedChangeCodec, type BufferedChange } from '@/operators/suppress-entry.js'
import type { MaybePromise } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import { yieldIfDue } from '@/runtime/signals.js'
import type { KeyValueStore } from '@/state.js'
import { keyValueStoreBuilder } from '@/state/store-builders.js'
import type { StoreBuilder } from '@/topology/nodes.js'

export type BufferStore = KeyValueStore<Buffer, Buffer>

export interface BufferCodecs<K, V> {
	readonly key: Codec<K>
	readonly value: Codec<V>
}

/** The store of a suppress buffer: key bytes of the table's key codec -> entry bytes (`suppress-entry.ts`). */
export function suppressBufferStoreBuilder(defaults: ChangelogDefaults, name: string): StoreBuilder<BufferStore> {
	return keyValueStoreBuilder(defaults, { name, keyCodec: buffer(), valueCodec: buffer() })
}

/** A buffered key. */
interface Slot<K, V> {
	readonly id: string
	readonly keyBytes: Buffer
	key: K
	change: BufferedChange<V>
	/** Its key and entry bytes: what it counts against `maxBytes`. */
	bytes: number
}

/** Entries loaded between two checks for a yield to the event loop. */
const YIELD_EVERY = 1000

const idOf = (keyBytes: Buffer): string => keyBytes.toString('latin1')

const evictedFirst = <K, V>(a: Slot<K, V>, b: Slot<K, V>): boolean =>
	a.change.bufferTime < b.change.bufferTime ||
	(a.change.bufferTime === b.change.bufferTime && Buffer.compare(a.keyBytes, b.keyBytes) < 0)

export class SuppressBuffer<K, V> {
	private readonly slots = new Map<string, Slot<K, V>>()
	private readonly order = new MinHeap<Slot<K, V>>(evictedFirst)
	private readonly entries: Codec<BufferedChange<V>>
	private totalBytes = 0

	constructor(
		private readonly store: BufferStore,
		private readonly codecs: BufferCodecs<K, V>
	) {
		this.entries = bufferedChangeCodec(codecs.value)
	}

	get numRecords(): number {
		return this.slots.size
	}

	/** The key and entry bytes of every buffered key. */
	get bytes(): number {
		return this.totalBytes
	}

	/** The earliest buffer time; +Infinity when the buffer is empty. */
	get minTime(): number {
		return this.order.peek()?.change.bufferTime ?? Number.POSITIVE_INFINITY
	}

	/** Loads what the store holds: the buffer as of the last commit (exactly-once) or the last write (at-least-once). */
	async load(): Promise<void> {
		let read = 0
		for await (const [keyBytes, bytes] of this.store.all()) {
			this.add(keyBytes, this.codecs.key.decode(keyBytes), this.entries.decode(bytes), bytes.length)
			if (++read % YIELD_EVERY === 0) await yieldIfDue()
		}
	}

	/**
	 * Buffers an update of a table: a key not buffered yet becomes due at `bufferTime` and keeps the update's old value
	 * as its prior value; a buffered one keeps both and takes the update's value, timestamp and headers.
	 */
	put(bufferTime: number, record: StreamRecord<K, Change<V>>): Promise<void> {
		const keyBytes = this.codecs.key.encode(record.key)
		const slot = this.slots.get(idOf(keyBytes))
		const change: BufferedChange<V> = {
			bufferTime: slot?.change.bufferTime ?? bufferTime,
			timestamp: record.timestamp,
			headers: record.headers,
			priorValue: slot === undefined ? record.value.oldValue : slot.change.priorValue,
			newValue: record.value.newValue,
		}
		const bytes = this.entries.encode(change)
		if (slot === undefined) {
			this.add(keyBytes, record.key, change, bytes.length)
		} else {
			const size = keyBytes.length + bytes.length
			this.totalBytes += size - slot.bytes
			slot.key = record.key
			slot.change = change
			slot.bytes = size
		}
		return this.store.put(keyBytes, bytes)
	}

	/**
	 * While `condition()` holds, hands the earliest key to `emit`, then removes it (KS removes after the callback, so a
	 * failed emission leaves the key buffered).
	 */
	async evictWhile(
		condition: () => boolean,
		emit: (key: K, change: BufferedChange<V>) => MaybePromise<void>
	): Promise<void> {
		for (let slot = this.order.peek(); slot !== undefined && condition(); slot = this.order.peek()) {
			await emit(slot.key, slot.change)
			this.order.pop()
			this.slots.delete(slot.id)
			this.totalBytes -= slot.bytes
			await this.store.delete(slot.keyBytes)
		}
	}

	private add(keyBytes: Buffer, key: K, change: BufferedChange<V>, entryBytes: number): void {
		const slot: Slot<K, V> = { id: idOf(keyBytes), keyBytes, key, change, bytes: keyBytes.length + entryBytes }
		this.slots.set(slot.id, slot)
		this.order.push(slot)
		this.totalBytes += slot.bytes
	}
}

/** A binary min-heap: keys leave the buffer only from its head, in order. */
class MinHeap<T> {
	private readonly items: T[] = []

	constructor(private readonly before: (a: T, b: T) => boolean) {}

	peek(): T | undefined {
		return this.items[0]
	}

	push(item: T): void {
		const { items } = this
		let index = items.push(item) - 1
		while (index > 0) {
			const parent = (index - 1) >> 1
			if (!this.before(item, items[parent]!)) break
			items[index] = items[parent]!
			index = parent
		}
		items[index] = item
	}

	pop(): T | undefined {
		const { items } = this
		const head = items[0]
		const last = items.pop()
		if (last === undefined || items.length === 0) {
			return head
		}
		let index = 0
		for (let child = 1; child < items.length; child = 2 * index + 1) {
			if (child + 1 < items.length && this.before(items[child + 1]!, items[child]!)) child++
			if (!this.before(items[child]!, last)) break
			items[index] = items[child]!
			index = child
		}
		items[index] = last
		return head
	}
}
