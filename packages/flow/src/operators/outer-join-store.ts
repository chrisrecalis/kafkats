/**
 * The shared outer-join store of a left or outer stream-stream join (6.10; KS's outer store of `TimestampedKeyAndJoinSide`
 * keys and `ListValueStore` values, ksdsl§7.3): the records of the outer sides that matched nothing yet, kept until their
 * join window and grace closed. A key-value store of raw keys `[int64BE ts][u8 side: 1 left, 0 right][key bytes]`, so
 * it iterates by timestamp, then right before left, then key; the value is the list of the values with that timestamp,
 * side and key (6.25). Both join processors of a task read and write it, each from its own side's point of view, and
 * share its emission state.
 */
import type { ChangelogConfig, ChangelogDefaults } from '@/changelog/config.js'
import { buffer, type Codec } from '@/codec.js'
import { listCodec } from '@/operators/list-value.js'
import { yieldIfDue } from '@/runtime/signals.js'
import type { KeyValueStore } from '@/state.js'
import { keyValueStoreBuilder } from '@/state/store-builders.js'
import type { StoreBuilder } from '@/topology/nodes.js'

export type JoinSide = 'left' | 'right'

type RawStore = KeyValueStore<Buffer, Buffer[]>

export function outerJoinStoreBuilder(
	defaults: ChangelogDefaults,
	options: { readonly name: string; readonly logging?: boolean | ChangelogConfig }
): StoreBuilder<RawStore> {
	return keyValueStoreBuilder(defaults, { ...options, keyCodec: buffer(), valueCodec: listCodec(buffer()) })
}

/**
 * What both join processors of a task share about the store (KS `TimeTracker`; its stream time is the processor clock):
 * a lower bound of the pending timestamps, and the wall-clock time before which no emission scan runs.
 */
export interface EmitState {
	minTime: number
	nextEmit: number
}

/** A pending entry as one side sees it: its own records, or the other side's. */
export type Pending<K, V, VO> = {
	readonly timestamp: number
	readonly key: K
	/** The entry's key in the store, for delete(). */
	readonly storeKey: Buffer
} & ({ readonly own: true; readonly values: readonly V[] } | { readonly own: false; readonly values: readonly VO[] })

const TIMESTAMP_BYTES = 8
const KEY_OFFSET = TIMESTAMP_BYTES + 1
/** Bounds of every key: timestamps are non-negative int64s. */
const FIRST = Buffer.alloc(TIMESTAMP_BYTES)
const LAST = Buffer.from([0xff])
/** Entries read between two checks for a yield to the event loop. */
const YIELD_EVERY = 1000

/** Keyed by the task's store: a task's two processors get the same store, a re-created task a new one. */
const emitStates = new WeakMap<RawStore, EmitState>()

export class OuterJoinStore<K, V, VO> {
	readonly emitState: EmitState
	private readonly ownSide: number
	private readonly otherSide: number

	constructor(
		private readonly store: RawStore,
		side: JoinSide,
		private readonly codecs: { readonly key: Codec<K>; readonly value: Codec<V>; readonly otherValue: Codec<VO> }
	) {
		this.ownSide = side === 'left' ? 1 : 0
		this.otherSide = 1 - this.ownSide
		let state = emitStates.get(store)
		if (state === undefined) {
			state = { minTime: Number.POSITIVE_INFINITY, nextEmit: 0 }
			emitStates.set(store, state)
		}
		this.emitState = state
	}

	/**
	 * Seeds `minTime` with the oldest pending timestamp, so a restarted task emits what closed meanwhile (Kafka Streams
	 * starts from Long.MAX_VALUE and waits for a new unmatched record when after + grace is 0, ksdsl§7.4).
	 */
	async init(): Promise<void> {
		for await (const [storeKey] of this.store.range(FIRST, LAST)) {
			this.emitState.minTime = Math.min(this.emitState.minTime, timestampOf(storeKey))
			return
		}
	}

	/** Adds an unmatched record of this side after the others with its key and timestamp. */
	async add(key: K, timestamp: number, value: V): Promise<void> {
		const storeKey = this.storeKey(timestamp, this.ownSide, key)
		const values = (await this.store.get(storeKey)) ?? []
		await this.store.put(storeKey, [...values, this.codecs.value.encode(value)])
	}

	/** The other side's records of `key` and `timestamp` matched: they are not pending any more (KS `putIfAbsent(k, null)`). */
	async removeOther(key: K, timestamp: number): Promise<void> {
		const storeKey = this.storeKey(timestamp, this.otherSide, key)
		if ((await this.store.get(storeKey)) !== undefined) {
			await this.store.delete(storeKey)
		}
	}

	/** Every pending entry, oldest first. Entries may be deleted while iterating. */
	async *pending(): AsyncGenerator<Pending<K, V, VO>> {
		let read = 0
		for await (const [storeKey, values] of this.store.range(FIRST, LAST)) {
			const entry = {
				timestamp: timestampOf(storeKey),
				key: this.codecs.key.decode(storeKey.subarray(KEY_OFFSET)),
				storeKey,
			}
			yield storeKey[TIMESTAMP_BYTES] === this.ownSide
				? { ...entry, own: true, values: values.map(v => this.codecs.value.decode(v)) }
				: { ...entry, own: false, values: values.map(v => this.codecs.otherValue.decode(v)) }
			if (++read % YIELD_EVERY === 0) await yieldIfDue()
		}
	}

	/** Deletes an entry of pending(), once its values were joined with null. */
	delete(entry: Pending<K, V, VO>): Promise<void> {
		return this.store.delete(entry.storeKey)
	}

	private storeKey(timestamp: number, side: number, key: K): Buffer {
		const keyBytes = this.codecs.key.encode(key)
		const storeKey = Buffer.allocUnsafe(KEY_OFFSET + keyBytes.length)
		storeKey.writeBigInt64BE(BigInt(timestamp), 0)
		storeKey[TIMESTAMP_BYTES] = side
		keyBytes.copy(storeKey, KEY_OFFSET)
		return storeKey
	}
}

const timestampOf = (storeKey: Buffer): number => Number(storeKey.readBigInt64BE(0))
