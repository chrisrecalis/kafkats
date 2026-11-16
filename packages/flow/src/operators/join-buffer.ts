/**
 * The buffer of one side of a stream-stream join (KS's join window store, which retains duplicates): the side's
 * records in a window store, each in the window `[ts, ts + windowMs]` that starts at its timestamp. Records with the
 * same key and timestamp are all kept: a later one's window end moves to the first free slot (the Stage 1 layout,
 * unchanged from the legacy runtime; Stage 3 replaces it with list values in a session-layout store, 6.10).
 */
import type { WindowStore } from '@/state.js'

/** The window store of a join buffer for a join window of `windowMs`: records are kept for twice the window. */
export function joinBufferWindow(windowMs: number): { readonly retentionMs: number; readonly windowSizeMs: number } {
	return { retentionMs: 2 * windowMs, windowSizeMs: windowMs }
}

export class JoinBuffer<K, V> {
	constructor(
		private readonly store: WindowStore<K, V>,
		private readonly windowMs: number
	) {}

	/** Adds a record; records with the same key and timestamp stay. */
	async put(key: K, timestamp: number, value: V): Promise<void> {
		let windowEnd = timestamp + this.windowMs
		while ((await this.store.get({ key, windowStart: timestamp, windowEnd })) !== undefined) {
			windowEnd++
		}
		await this.store.put({ key, windowStart: timestamp, windowEnd }, value)
	}

	/** The values of `key` with a timestamp in `[from, to]`, in timestamp order. */
	async *fetch(key: K, from: number, to: number): AsyncGenerator<V> {
		for await (const [, value] of this.store.fetch(key, from, to)) {
			yield value
		}
	}

	/** Deletes the records past the retention at stream time `streamTime`. */
	expire(streamTime: number): Promise<number> {
		return this.store.expireOldWindows(streamTime)
	}
}
