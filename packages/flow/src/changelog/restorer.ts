import type { Admin, Consumer, KafkaClient, Message } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { ChangelogCheckpointStore, KeyValueStore } from '@/state.js'
import { clearStore } from '@/state/partitioned.js'
import type { ChangelogRestorationOptions } from '@/changelog/config.js'

/** Resolves the store that holds the state of one changelog partition. */
export type RestoreTarget<K, V> = KeyValueStore<K, V> | ((partition: number) => Promise<KeyValueStore<K, V>>)

const DEFAULT_RESTORE_IDLE_TIMEOUT_MS = 30_000
const DEFAULT_CHECK_INTERVAL_MS = 1000
/** Upper bound for waiting on a dangling transaction (brokers cap transaction timeouts at 15 minutes by default). */
const MAX_OPEN_TRANSACTION_WAIT_MS = 16 * 60_000
const RESTORE_COMPLETE = Symbol('changelogRestoreComplete')

/** Where restoring one changelog partition starts and ends. */
type PartitionStart<K, V> = {
	partition: number
	store: KeyValueStore<K, V>
	/** Offset replay starts from: the trusted checkpoint, or the start of the log. */
	from: bigint
	/** End of the log when the restore started; replay stops here. */
	end: bigint
	/** The checkpoint was not trusted: the store is wiped and rebuilt from the start of the log. */
	rebuild: boolean
}

/**
 * Restores state from a changelog topic.
 *
 * Guarantees:
 * - restore() only returns once every requested partition has been replayed up to the end offset
 *   captured at the start; a stall is an error, never a silent partial restore
 * - a checkpoint is only trusted when it can describe the local store (within the log's offset
 *   range and the store is not empty); otherwise the store is wiped and rebuilt from the start, so
 *   stale local entries cannot survive
 * - checkpoints are only advanced for partitions that were fully replayed
 */
export class ChangelogRestorer<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly target: RestoreTarget<K, V>
	) {}

	/**
	 * Restore state by replaying the changelog topic.
	 * @returns Number of records restored
	 */
	async restore(
		client: KafkaClient,
		options?: ChangelogRestorationOptions,
		partitions?: number[],
		checkpointStore?: ChangelogCheckpointStore
	): Promise<number> {
		const admin = client.admin()
		const [desc] = await admin.describeTopics([this.topicName])
		if (!desc) {
			return 0
		}
		const requested = [...new Set(partitions ?? desc.partitions.map(p => p.partitionIndex))].sort((a, b) => a - b)
		if (requested.length === 0) {
			return 0
		}

		const starts = await this.decideStarts(admin, requested, checkpointStore)
		await this.wipeRebuiltStores(starts)

		const toReplay = starts.filter(s => s.from < s.end)
		const restored =
			toReplay.length > 0
				? await new Replay(client, admin, this.topicName, options, toReplay, this.applyRecord).run()
				: 0

		if (checkpointStore) {
			// Partitions that were replayed, plus partitions that were wiped because their checkpoint
			// was unusable (their state is now exactly "empty as of end offset").
			await this.recordCheckpoints(
				client,
				checkpointStore,
				starts.filter(s => s.rebuild || s.from < s.end)
			)
		}
		return restored
	}

	private storeFor(partition: number): Promise<KeyValueStore<K, V>> {
		return typeof this.target === 'function' ? this.target(partition) : Promise.resolve(this.target)
	}

	/** Phase 1: per partition, decide whether to resume from its checkpoint or rebuild from the start. */
	private async decideStarts(
		admin: Admin,
		partitions: number[],
		checkpointStore: ChangelogCheckpointStore | undefined
	): Promise<PartitionStart<K, V>[]> {
		// The restore target is the END of the log (high watermark), not the last stable offset.
		// While a crashed instance's transaction is still open, the last stable offset is pinned at
		// that transaction's start even though later transactions (of a successor instance) have
		// already committed beyond it. Stopping there would restore state that is behind the
		// committed input offsets. Reading up to the log end with read_committed waits for the
		// broker to abort the dangling transaction (bounded by its transaction timeout) and then
		// delivers everything that was committed.
		const [startOffsets, endOffsets] = await Promise.all([
			admin.fetchTopicOffsets(this.topicName, partitions, 'earliest', { isolationLevel: 'read_committed' }),
			admin.fetchTopicOffsets(this.topicName, partitions, 'latest', { isolationLevel: 'read_uncommitted' }),
		])

		const candidates = await Promise.all(
			partitions.map(async partition => {
				const earliest = startOffsets.get(partition)
				const end = endOffsets.get(partition)
				if (earliest === undefined || end === undefined) {
					throw new Error(`Missing offset information for ${this.topicName}-${partition}`)
				}
				const checkpoint = checkpointStore ? await checkpointStore.get(this.topicName, partition) : undefined
				const store = await this.storeFor(partition)
				const trusted = await isCheckpointTrusted(checkpoint, earliest, end, store)
				return { partition, store, earliest, end, checkpoint: trusted ? checkpoint : undefined }
			})
		)

		// A single shared target can't be wiped per partition: if any partition needs a full
		// rebuild, rebuild them all.
		const rebuildAll = typeof this.target !== 'function' && candidates.some(c => c.checkpoint === undefined)
		return candidates.map(({ partition, store, earliest, end, checkpoint }) =>
			rebuildAll || checkpoint === undefined
				? { partition, store, end, rebuild: true, from: earliest }
				: { partition, store, end, rebuild: false, from: checkpoint }
		)
	}

	/** Phase 2: wipe the stores that are rebuilt from the start of the log (each store once). */
	private async wipeRebuiltStores(starts: PartitionStart<K, V>[]): Promise<void> {
		const wiped = new Set<KeyValueStore<K, V>>()
		for (const { store, rebuild } of starts) {
			if (!rebuild || wiped.has(store)) continue
			wiped.add(store)
			await clearStore(store)
		}
	}

	/** Phase 3 (per record): write one changelog record into the raw store. */
	private readonly applyRecord = async (partition: number, key: Buffer, value: Buffer | null): Promise<void> => {
		const store = await this.storeFor(partition)
		const decodedKey = this.keyCodec.decode(key)
		if (value === null) {
			await store.delete(decodedKey)
		} else {
			await store.put(decodedKey, this.valueCodec.decode(value))
		}
	}

	/** Phase 4: record the end offset as the checkpoint of every partition now caught up. */
	private async recordCheckpoints(
		client: KafkaClient,
		checkpointStore: ChangelogCheckpointStore,
		caughtUp: PartitionStart<K, V>[]
	): Promise<void> {
		const logger = client.cluster.getLogger()
		await Promise.all(
			caughtUp.map(async ({ partition, end }) => {
				try {
					await checkpointStore.set(this.topicName, partition, end)
				} catch (err) {
					// Restored state is in place regardless; failing to record this means a future
					// restart will re-restore (idempotent but wasteful). Log so it is visible.
					logger.error('post-restore checkpoint persistence failed', {
						topic: this.topicName,
						partition,
						error: err instanceof Error ? err.message : String(err),
					})
				}
			})
		)
		await checkpointStore.flush?.()
	}
}

/** A checkpoint is trusted only if it lies within the log and the store can contain what it claims. */
async function isCheckpointTrusted<K, V>(
	checkpoint: bigint | undefined,
	earliest: bigint,
	end: bigint,
	store: KeyValueStore<K, V>
): Promise<boolean> {
	if (checkpoint === undefined || checkpoint < earliest || checkpoint > end) return false
	// A checkpoint past the start of the log on an empty store claims progress the store cannot
	// contain, e.g. the state directory was replaced. Rebuild from the start.
	return checkpoint === earliest || (await store.approximateNumEntries()) !== 0
}

type ReplayProgress = {
	end: bigint
	/** Next offset the restore consumer will fetch. */
	position: bigint
	/** Records handed to the consumer by fetches so far. */
	fetched: number
	/** Records the handler has finished with so far. */
	handled: number
}

/**
 * Phase 3: one run of a throwaway consumer over the partitions to replay, from their start offsets
 * up to the end offsets captured before it started. Ends when every partition is caught up, and
 * fails on a stall unless it is waiting out an open transaction.
 */
class Replay {
	private readonly progress = new Map<number, ReplayProgress>()
	/** Partitions not caught up yet. */
	private readonly pending: Set<number>
	private readonly stop = new AbortController()
	private readonly consumer: Consumer
	private readonly idleTimeoutMs: number
	private readonly initialIdleTimeoutMs: number
	private readonly checkIntervalMs: number
	private startedAt = 0
	private lastProgressAt = Date.now()
	private madeProgress = false
	private consumerRunning = false
	/** Records being written to a store right now. */
	private inFlight = 0
	private restored = 0
	private handlerError: unknown = null
	private stallCheckRunning = false
	private warnedOpenTransaction = false

	constructor(
		private readonly client: KafkaClient,
		private readonly admin: Admin,
		private readonly topicName: string,
		options: ChangelogRestorationOptions | undefined,
		partitions: Array<{ partition: number; from: bigint; end: bigint }>,
		private readonly apply: (partition: number, key: Buffer, value: Buffer | null) => Promise<void>
	) {
		for (const { partition, from, end } of partitions) {
			this.progress.set(partition, { end, position: from, fetched: 0, handled: 0 })
		}
		this.pending = new Set(this.progress.keys())
		this.idleTimeoutMs = options?.idleTimeoutMs ?? DEFAULT_RESTORE_IDLE_TIMEOUT_MS
		this.initialIdleTimeoutMs = options?.initialIdleTimeoutMs ?? this.idleTimeoutMs
		this.checkIntervalMs = options?.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS
		this.consumer = client.consumer({
			groupId: `${topicName}-restorer-${Date.now()}-${Math.random().toString(36).slice(2)}`,
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
			...(options?.consumerMaxWaitMs !== undefined ? { maxWaitMs: options.consumerMaxWaitMs } : {}),
		})
	}

	/** @returns Number of records restored (puts; tombstones are not counted). */
	async run(): Promise<number> {
		this.consumer.once('running', () => {
			this.consumerRunning = true
			this.lastProgressAt = Date.now()
		})
		this.consumer.on('fetchPosition', (position, recordCount) => {
			if (position.topic !== this.topicName) return
			const state = this.progress.get(position.partition)
			if (!state) return
			this.recordProgress()
			state.fetched += recordCount
			if (position.offset > state.position) state.position = position.offset
			this.completeIfCaughtUp(position.partition)
		})

		this.startedAt = Date.now()
		const stallCheck = setInterval(() => this.checkForStall(), this.checkIntervalMs)
		try {
			await this.consumer.runEach([this.topicName], message => this.handle(message), {
				commitOffsets: false,
				assignment: [...this.progress].map(([partition, { position }]) => ({
					topic: this.topicName,
					partition,
					offset: position,
				})),
				signal: this.stop.signal,
			})
		} finally {
			clearInterval(stallCheck)
		}

		this.throwIfIncomplete()
		return this.restored
	}

	private async handle(message: Message): Promise<void> {
		const state = this.progress.get(message.partition)
		if (!state) return
		this.inFlight += 1
		try {
			// Records past the captured end offset belong to a concurrent writer; the partition's
			// owner will have them in its own state.
			if (message.offset < state.end && message.key !== null) {
				await this.apply(message.partition, message.key, message.value)
				if (message.value !== null) this.restored++
			}
		} catch (err) {
			this.handlerError = err
			throw err
		} finally {
			this.inFlight -= 1
			state.handled += 1
			this.recordProgress()
		}
		this.completeIfCaughtUp(message.partition)
	}

	private recordProgress(): void {
		this.lastProgressAt = Date.now()
		this.madeProgress = true
	}

	/**
	 * A partition is complete once the consumer's fetch position has reached the end offset AND
	 * every record it fetched has been applied. Position alone is not enough (fetched records may
	 * still be queued); reaching the last record is not enough either (on transactional changelogs
	 * the end offset sits past trailing commit markers).
	 */
	private completeIfCaughtUp(partition: number): void {
		const state = this.progress.get(partition)
		if (!state || !this.pending.has(partition)) return
		if (state.position < state.end || state.handled < state.fetched) return
		this.pending.delete(partition)
		try {
			this.consumer.pause([{ topic: this.topicName, partition }])
		} catch {
			// best effort; the consumer is stopped once all partitions complete
		}
		if (this.pending.size === 0) {
			this.stop.abort(RESTORE_COMPLETE)
		}
	}

	/** Runs every checkIntervalMs: fail the restore if it made no progress for too long. */
	private checkForStall(): void {
		if (this.stallCheckRunning || !this.consumerRunning || this.isOver()) return
		// A slow store write is progress in the making, not a stall.
		if (this.inFlight > 0) return
		const timeoutMs = this.madeProgress ? this.idleTimeoutMs : this.initialIdleTimeoutMs
		if (Date.now() - this.lastProgressAt <= timeoutMs) return

		this.stallCheckRunning = true
		void this.isHeldByOpenTransaction()
			.catch(() => false)
			.then(held => {
				this.stallCheckRunning = false
				if (this.isOver()) return
				if (held && Date.now() - this.startedAt < MAX_OPEN_TRANSACTION_WAIT_MS) {
					this.waitForOpenTransaction()
					return
				}
				this.stop.abort(
					new Error(
						`Timed out restoring changelog "${this.topicName}": no progress for ${timeoutMs}ms ` +
							`(pending partitions: ${[...this.pending].join(', ')})`
					)
				)
			})
	}

	/**
	 * True when every pending partition has consumed all stable data and is only held back by an
	 * open transaction (last stable offset below the restore target).
	 */
	private async isHeldByOpenTransaction(): Promise<boolean> {
		const stable = await this.admin.fetchTopicOffsets(this.topicName, [...this.pending], 'latest', {
			isolationLevel: 'read_committed',
		})
		for (const partition of this.pending) {
			const state = this.progress.get(partition)
			const lastStable = stable.get(partition)
			if (!state || lastStable === undefined) return false
			if (lastStable >= state.end || state.position < lastStable) return false
		}
		return true
	}

	/**
	 * Not a stall: the broker will abort the dangling transaction once its transaction timeout
	 * expires, after which the remaining records arrive.
	 */
	private waitForOpenTransaction(): void {
		this.lastProgressAt = Date.now()
		if (this.warnedOpenTransaction) return
		this.warnedOpenTransaction = true
		this.client.cluster.getLogger().warn('changelog restore is waiting for an open transaction to complete', {
			topic: this.topicName,
			partitions: [...this.pending],
		})
	}

	private isOver(): boolean {
		return this.stop.signal.aborted || this.pending.size === 0
	}

	private throwIfIncomplete(): void {
		if (this.handlerError) {
			throw this.handlerError instanceof Error ? this.handlerError : new Error(String(this.handlerError))
		}
		const reason: unknown = this.stop.signal.reason
		if (reason instanceof Error) {
			throw reason
		}
		if (this.pending.size > 0) {
			throw new Error(
				`Restoring changelog "${this.topicName}" stopped before completion ` +
					`(pending partitions: ${[...this.pending].join(', ')})`,
				{ cause: reason }
			)
		}
	}
}
