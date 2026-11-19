/**
 * At-least-once, the deletes of a key-value store that wait for the task's next commit (6.16): the layer of a store
 * whose deletes must not outlive a failure before that commit, like the evictions of a suppress buffer, whose results
 * are sent but not acknowledged yet. A held delete is visible to the reads through this layer; once the task
 * committed, it goes to the layers below (the changelog's tombstone and the provider store). A dirty close drops the
 * layer with its held deletes, so the next incarnation finds the entries again, and the records it replays from the
 * last commit delete them again. A put cancels the held delete of its key. Under exactly-once, the task's transaction
 * does the same for every write.
 */
import type { Codec } from '@/codec.js'
import type { KeyValueStore } from '@/state.js'

export class HeldDeletesKeyValueStore<K, V> implements KeyValueStore<K, V> {
	/** By encoded key, in the order of the deletes. */
	private readonly held = new Map<string, K>()

	constructor(
		private readonly inner: KeyValueStore<K, V>,
		private readonly keyCodec: Codec<K>
	) {}

	get name(): string {
		return this.inner.name
	}

	get(key: K): Promise<V | undefined> {
		return this.held.has(this.id(key)) ? Promise.resolve(undefined) : this.inner.get(key)
	}

	put(key: K, value: V): Promise<void> {
		this.held.delete(this.id(key))
		return this.inner.put(key, value)
	}

	delete(key: K): Promise<void> {
		this.held.set(this.id(key), key)
		return Promise.resolve()
	}

	all(): AsyncIterable<[K, V]> {
		return this.visible(this.inner.all())
	}

	range(from: K, to: K): AsyncIterable<[K, V]> {
		return this.visible(this.inner.range(from, to))
	}

	/** Counts the entries of held deletes too. */
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

	/** After the task committed: the held deletes go to the layers below, in the order they were made. */
	async applyHeld(): Promise<void> {
		for (const [id, key] of this.held) {
			await this.inner.delete(key)
			this.held.delete(id)
		}
	}

	private id(key: K): string {
		return this.keyCodec.encode(key).toString('latin1')
	}

	private async *visible(entries: AsyncIterable<[K, V]>): AsyncIterable<[K, V]> {
		for await (const entry of entries) {
			if (this.held.size === 0 || !this.held.has(this.id(entry[0]))) yield entry
		}
	}
}
