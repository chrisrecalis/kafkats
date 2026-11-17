/**
 * The buffer of one side of a stream-stream join (6.10; KS's join window store, which retains duplicates, ksdsl§7.1):
 * a clocked session-layout store with one entry `[ts, ts]` per key and timestamp, whose value is the list of the side's
 * values with that key and timestamp in arrival order (B8: no more window-end bumping). The key comes first in the
 * layout, so a probe reads the entries of one key only (B15). Reads are clipped by the buffer's own store clock, the
 * newest timestamp written to it, at its retention `before + after + grace` (6.2), so expiry never changes a result.
 */
import type { ChangelogDefaults } from '@/changelog/config.js'
import { listCodec } from '@/operators/list-value.js'
import type { SessionStore } from '@/state.js'
import { sessionStoreBuilder, type SessionStoreBuilderOptions } from '@/state/store-builders.js'
import type { ClockedSessionStore } from '@/state/time-clipping.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/** The store of a join buffer of `valueCodec` values. */
export function joinBufferBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: Omit<SessionStoreBuilderOptions<K, V>, 'clock'>
): StoreBuilder<SessionStore<K, V[]>> {
	return sessionStoreBuilder(defaults, { ...options, valueCodec: listCodec(options.valueCodec), clock: {} })
}

export class JoinBuffer<K, V> {
	constructor(private readonly store: ClockedSessionStore<K, V[]>) {}

	/** Adds a record after the values with the same key and timestamp. */
	async put(key: K, timestamp: number, value: V): Promise<void> {
		const entry = { key, windowStart: timestamp, windowEnd: timestamp }
		const values = (await this.store.get(entry)) ?? []
		await this.store.put(entry, [...values, value])
	}

	/** The timestamps and values of `key` in `[from, to]` within the retention, in timestamp order. */
	async *fetch(key: K, from: number, to: number): AsyncGenerator<[timestamp: number, values: V[]]> {
		for await (const [entry, values] of this.store.findSessions(key, from, to)) {
			yield [entry.windowStart, values]
		}
	}

	/** Deletes the entries past the retention by the buffer's own store clock, locally (5.3). */
	expire(): Promise<void> {
		return this.store.expire()
	}
}
