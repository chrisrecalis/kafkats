import type { Codec } from '@/codec.js'
import type { KeyValueStore } from '@/state.js'

/**
 * Writing many entries of a store at once: loading a table or a changelog, wiping a store.
 *
 * Writes that do not depend on each other are issued together and awaited together. A store that
 * commits asynchronously (LMDB) then commits them as one write transaction, where awaiting them
 * one by one costs a disk transaction per entry.
 */

/** A record of a compacted topic: the new value of a key, or its removal. */
export type Change = { key: Buffer; value: Buffer | null }

/** How many removals of a wipe are issued (and held in memory as promises) at a time. */
const WIPE_ROUND = 10_000

/**
 * The last change of each key, in the order of their last occurrence: only that one decides what
 * the store holds after all of them. Records without a key change nothing.
 */
export function latestPerKey(records: Iterable<{ key: Buffer | null; value: Buffer | null }>): Change[] {
	const latest = new Map<string, Change>()
	for (const { key, value } of records) {
		if (key === null) continue
		const id = key.toString('latin1')
		// Re-inserting moves the key to the end.
		latest.delete(id)
		latest.set(id, { key, value })
	}
	return [...latest.values()]
}

/** Apply changes of distinct keys (see {@link latestPerKey}) to a store. */
export async function writeChanges<K, V>(
	store: KeyValueStore<K, V>,
	keyCodec: Codec<K>,
	valueCodec: Codec<V>,
	changes: Iterable<Change>
): Promise<void> {
	// async: a change that cannot be decoded rejects like a failed write, after the others were issued.
	const write = async ({ key, value }: Change): Promise<void> => {
		const decodedKey = keyCodec.decode(key)
		if (value === null) {
			await store.delete(decodedKey)
		} else {
			await store.put(decodedKey, valueCodec.decode(value))
		}
	}
	await Promise.all(Array.from(changes, write))
}

/** Remove every entry from a store. */
export async function clearStore<K, V>(store: KeyValueStore<K, V>): Promise<void> {
	// Collected before anything is removed: a store need not support removal while it is iterated.
	const keys: K[] = []
	for await (const [key] of store.all()) {
		keys.push(key)
	}
	for (let start = 0; start < keys.length; start += WIPE_ROUND) {
		await Promise.all(keys.slice(start, start + WIPE_ROUND).map(key => store.delete(key)))
	}
}
