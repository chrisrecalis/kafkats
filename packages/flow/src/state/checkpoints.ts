import type { ChangelogCheckpointStore } from '@/state.js'

/** The checkpoint of an invalidated store: below every earliest offset, so never trusted. */
const INVALID = -1n

/**
 * The checkpoint store key of a clear marker: a space is not allowed in a topic name, so it never meets a changelog's.
 * Its value is {@link CLEARING} while the clear is in progress, {@link CLEARED} once it is done (the store API has no
 * delete).
 */
const clearMarker = (store: string): string => `clearing ${store}`
const CLEARING = 1n
const CLEARED = 0n

const tpKey = (topic: string, partition: number): string => `${topic}:${partition}`

const toError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)))

/**
 * Changelog checkpoints: per changelog partition, the next offset to read, up to which the local store reflects the
 * changelog.
 *
 * Durable when the provider has a checkpoint store; otherwise kept in process, which is enough to catch a store that
 * stayed in memory up after a rebalance instead of rebuilding it. One per start: it outlives tasks and recoveries.
 *
 * A checkpoint that fails to persist is never fatal (restoration then replays more of the changelog): the failure goes
 * to `onError`, once per distinct message until the checkpoints of a commit persist again. Invalidation is the
 * exception: a rebuild must not start over a checkpoint that would still be trusted, so its failure is thrown.
 */
export class Checkpoints {
	readonly durable: boolean
	private readonly store: ChangelogCheckpointStore
	private readonly inProcess = new Map<string, bigint>()
	/** Per changelog partition, the last value set since the last invalidation: a checkpoint never moves back. */
	private readonly floors = new Map<string, bigint>()
	/** The message of the failure reported last; cleared by a flush with no failure since the previous flush. */
	private reported: string | null = null
	private failedSinceFlush = false

	constructor(
		store: ChangelogCheckpointStore | undefined,
		private readonly onError: (error: Error) => void
	) {
		this.durable = store !== undefined
		const inProcess = this.inProcess
		this.store = store ?? {
			get: (topic, partition) => Promise.resolve(inProcess.get(tpKey(topic, partition))),
			set: (topic, partition, offset) => {
				inProcess.set(tpKey(topic, partition), offset)
				return Promise.resolve()
			},
		}
	}

	get(topic: string, partition: number): Promise<bigint | undefined> {
		return this.store.get(topic, partition)
	}

	/** Writes `max(floor, offset)`, the floor being the last value set since the last invalidation. */
	async set(topic: string, partition: number, offset: bigint): Promise<void> {
		const key = tpKey(topic, partition)
		const floor = this.floors.get(key)
		const value = floor !== undefined && floor > offset ? floor : offset
		this.floors.set(key, value)
		try {
			await this.store.set(topic, partition, value)
		} catch (error) {
			this.failed(error)
		}
	}

	/**
	 * Makes the checkpoint untrusted before its store is wiped or rebuilt: writes the sentinel `-1n`, flushes and resets
	 * the floor. Throws when that fails.
	 */
	async invalidate(topic: string, partition: number): Promise<void> {
		this.floors.delete(tpKey(topic, partition))
		await this.store.set(topic, partition, INVALID)
		await this.store.flush?.()
	}

	/**
	 * Marks the clear of `store` (`<applicationId>.<store>`) on `partition` in progress, durably (a write and a flush)
	 * before anything is deleted: the only record that a store nothing restores (no changelog, or `skipRestoration`) is
	 * partly cleared once a close or a crash stopped its clear. Throws when that fails.
	 */
	async beginClear(store: string, partition: number): Promise<void> {
		await this.store.set(clearMarker(store), partition, CLEARING)
		await this.store.flush?.()
	}

	/**
	 * Marks the clear done, durably before the store is used again: a marker left in progress would have the next start
	 * clear the state written since. Throws when that fails.
	 */
	async endClear(store: string, partition: number): Promise<void> {
		await this.store.set(clearMarker(store), partition, CLEARED)
		await this.store.flush?.()
	}

	/** Whether a clear of `store` on `partition` began and did not finish (beginClear() without endClear()). */
	async clearing(store: string, partition: number): Promise<boolean> {
		return (await this.store.get(clearMarker(store), partition)) === CLEARING
	}

	/** Makes every checkpoint set so far durable; once per commit. */
	async flush(): Promise<void> {
		try {
			await this.store.flush?.()
			if (!this.failedSinceFlush) this.reported = null
		} catch (error) {
			this.failed(error)
		}
		this.failedSinceFlush = false
	}

	/** Forgets the in-process checkpoints, with the in-memory stores they describe. */
	clearInProcess(): void {
		this.inProcess.clear()
		// A floor only ever raises a checkpoint, so forgetting one is always safe.
		this.floors.clear()
	}

	private failed(error: unknown): void {
		this.failedSinceFlush = true
		const failure = toError(error)
		if (failure.message === this.reported) return
		this.reported = failure.message
		this.onError(failure)
	}
}
