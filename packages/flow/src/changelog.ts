import type { KafkaClient, Producer } from '@kafkats/client'
import type { SendResult } from '@kafkats/client'
import type { Codec } from './codec.js'
import type { ChangelogCheckpointStore, KeyValueStore, WindowedKey } from './state.js'

/**
 * Configuration for changelog topics backing state stores.
 *
 * Note: Partition count is always inferred from source topic(s) and cannot be overridden.
 * This ensures state locality - Task N processing partition N writes to changelog partition N.
 */
export interface ChangelogConfig {
	/** Whether changelog is enabled. Default: true */
	enabled?: boolean
	/** Custom topic name. Default: {appId}-{storeName}-changelog */
	topicName?: string
	/** Replication factor. Default: broker default */
	replicationFactor?: number
	/** Custom topic configs. Merged with defaults */
	topicConfigs?: Record<string, string>
	/** Skip state restoration on startup. Default: false */
	skipRestoration?: boolean
}

export interface ChangelogRestorationOptions {
	/**
	 * Fail restoration if no progress is made for this many milliseconds while restoration is incomplete.
	 * A stalled restore is always an error; it is never treated as complete.
	 * Default: 30000ms.
	 */
	idleTimeoutMs?: number
	/**
	 * Abort restoration if no progress is made for this many milliseconds BEFORE the first message is received,
	 * while restoration is incomplete.
	 *
	 * This can be set higher than idleTimeoutMs to tolerate slow consumer startup
	 * while still aborting quickly if restoration stalls.
	 *
	 * Default: idleTimeoutMs.
	 */
	initialIdleTimeoutMs?: number
	/**
	 * How often to check for the progress timeout.
	 * Default: 1000ms.
	 */
	checkIntervalMs?: number
	/**
	 * Override the consumer's Fetch maxWaitMs during restoration to make stop() responsive.
	 * Default: unset (uses consumer defaults).
	 */
	consumerMaxWaitMs?: number
}

/**
 * Error thrown when an existing changelog topic has the wrong number of partitions.
 * This indicates data corruption risk and requires manual intervention.
 */
export class ChangelogPartitionMismatchError extends Error {
	override readonly name = 'ChangelogPartitionMismatchError'

	constructor(
		public readonly changelogTopic: string,
		public readonly expectedPartitions: number,
		public readonly actualPartitions: number,
		public readonly sourceTopics: string[]
	) {
		super(
			`Changelog topic "${changelogTopic}" has ${actualPartitions} partitions ` +
				`but source topics [${sourceTopics.join(', ')}] require ${expectedPartitions}. ` +
				`Delete the changelog topic and restart, or manually recreate with ${expectedPartitions} partitions.`
		)
	}
}

/**
 * Error thrown when a source topic does not exist and partition count cannot be inferred.
 */
export class SourceTopicNotFoundError extends Error {
	override readonly name = 'SourceTopicNotFoundError'

	constructor(
		public readonly topic: string,
		public readonly storeName: string
	) {
		super(
			`Source topic "${topic}" does not exist. Cannot determine partition count for ` +
				`changelog topic of store "${storeName}". Create the source topic first.`
		)
	}
}

/**
 * Error thrown when topics that must be co-partitioned have different partition counts.
 *
 * State is partitioned by source partition, so records with the same key from every topic feeding a
 * stateful operation (join sides, merged streams) must land in the same partition index.
 */
export class CoPartitioningError extends Error {
	override readonly name = 'CoPartitioningError'

	constructor(public readonly partitionCounts: Map<string, number>) {
		super(
			`Topics [${[...partitionCounts].map(([topic, count]) => `${topic} (${count})`).join(', ')}] feed the same ` +
				'stateful operation and must have the same number of partitions. ' +
				'Repartition one side with through(), or use a globalTable() for lookups.'
		)
	}
}

/**
 * Default topic configs for changelog topics.
 */
export const DEFAULT_CHANGELOG_CONFIGS: Record<string, string> = {
	'cleanup.policy': 'compact',
	'retention.ms': '-1',
	'segment.bytes': '52428800',
}

/**
 * Builds the changelog topic name for a state store.
 */
export function buildChangelogTopicName(appId: string, storeName: string): string {
	return `${appId}-${storeName}-changelog`
}

/**
 * Returns the default topic configs for changelog topics.
 */
export function getDefaultTopicConfigs(overrides?: Record<string, string>): Record<string, string> {
	return { ...DEFAULT_CHANGELOG_CONFIGS, ...overrides }
}

/**
 * Resolves changelog config from various input types.
 */
export function resolveChangelogConfig(changelog: boolean | ChangelogConfig | undefined): ChangelogConfig {
	if (changelog === false) {
		return { enabled: false }
	}
	if (changelog === undefined || changelog === true) {
		return { enabled: true }
	}
	return { enabled: changelog.enabled !== false, ...changelog }
}

/**
 * Spec for a changelog topic to be created.
 */
export interface ChangelogTopicSpec {
	storeName: string
	topicName: string
	replicationFactor?: number
	configs: Record<string, string>
	keyCodec: Codec<unknown>
	valueCodec: Codec<unknown>
	skipRestoration: boolean
	/** Source topics that feed into this state store. Used to infer partition count. */
	sourceTopics: Set<string>
	/**
	 * If true, restoration is restricted to partitions currently assigned from source topics.
	 * Use false for stores that are re-keyed and no longer have source-partition affinity.
	 */
	restrictRestorationToSourcePartitions?: boolean
	/** If true, only validate existing topics, don't create new ones. */
	validateOnly: boolean
}

/**
 * Transaction-like interface for sending messages.
 * Matches the send signature of both Producer and ActiveTransaction.
 */
interface TransactionLike {
	send(
		topic: string,
		messages: { key?: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer>; partition?: number }
	): Promise<SendResult>
}

/**
 * Writes state mutations to a changelog topic.
 *
 * Records are written to the changelog partition with the same index as the source partition of
 * the record being processed (`getPartition`). Restoration reads changelog partition N to rebuild
 * the state of source partition N, so the two must line up regardless of how the changelog key
 * hashes (windowed keys, custom input partitioners, differing key codecs, ...).
 */
export class ChangelogWriter<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly getProducer: () => Producer,
		private readonly getTransaction: () => TransactionLike | null,
		private readonly onWriteAck?: (result: SendResult) => void | Promise<void>,
		private readonly getPartition?: () => number | undefined
	) {}

	/**
	 * Write a key-value pair to the changelog.
	 */
	async write(key: K, value: V): Promise<void> {
		await this.send(this.keyCodec.encode(key), this.valueCodec.encode(value))
	}

	/**
	 * Write a tombstone (null value) to the changelog.
	 */
	async writeTombstone(key: K): Promise<void> {
		await this.send(this.keyCodec.encode(key), null)
	}

	private async send(key: Buffer, value: Buffer | null): Promise<void> {
		const partition = this.getPartition?.()
		const message = partition === undefined ? { key, value } : { key, value, partition }
		const target = this.getTransaction() ?? this.getProducer()
		const result = await target.send(this.topicName, message)
		await this.onWriteAck?.(result)
	}
}

/** Resolves the store that holds the state of one changelog partition. */
export type RestoreTarget<K, V> = KeyValueStore<K, V> | ((partition: number) => Promise<KeyValueStore<K, V>>)

type PartitionProgress = {
	endOffset: bigint
	/** Next offset the restore consumer will fetch. */
	position: bigint
	/** Records handed to the consumer by fetches so far. */
	fetched: number
	/** Records the handler has finished with so far. */
	handled: number
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

	private storeFor(partition: number): Promise<KeyValueStore<K, V>> {
		return typeof this.target === 'function' ? this.target(partition) : Promise.resolve(this.target)
	}

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

		const assigned = [...new Set(partitions ?? desc.partitions.map(p => p.partitionIndex))].sort((a, b) => a - b)
		if (assigned.length === 0) {
			return 0
		}

		// The restore target is the END of the log (high watermark), not the last stable offset.
		// While a crashed instance's transaction is still open, the last stable offset is pinned at
		// that transaction's start even though later transactions (of a successor instance) have
		// already committed beyond it. Stopping there would restore state that is behind the
		// committed input offsets. Reading up to the log end with read_committed waits for the
		// broker to abort the dangling transaction (bounded by its transaction timeout) and then
		// delivers everything that was committed.
		const [startOffsets, endOffsets] = await Promise.all([
			admin.fetchTopicOffsets(this.topicName, assigned, 'earliest', { isolationLevel: 'read_committed' }),
			admin.fetchTopicOffsets(this.topicName, assigned, 'latest', { isolationLevel: 'read_uncommitted' }),
		])

		// Decide, per partition, whether the checkpoint can be trusted.
		const plan = await Promise.all(
			assigned.map(async partition => {
				const earliest = startOffsets.get(partition)
				const end = endOffsets.get(partition)
				if (earliest === undefined || end === undefined) {
					throw new Error(`Missing offset information for ${this.topicName}-${partition}`)
				}
				const checkpoint = checkpointStore ? await checkpointStore.get(this.topicName, partition) : undefined
				const store = await this.storeFor(partition)
				let resumable = checkpoint !== undefined && checkpoint >= earliest && checkpoint <= end
				if (resumable && checkpoint! > earliest && (await store.approximateNumEntries()) === 0) {
					// The checkpoint claims progress the (empty) store cannot contain, e.g. the
					// state directory was replaced. Rebuild from the start.
					resumable = false
				}
				return { partition, earliest, end, checkpoint, resumable, store }
			})
		)

		// A single shared target can't be wiped per partition: if any partition needs a full
		// rebuild, rebuild them all.
		const sharedTarget = typeof this.target !== 'function'
		const rebuildAll = sharedTarget && plan.some(p => !p.resumable)
		const wiped = new Set<KeyValueStore<K, V>>()
		const progress = new Map<number, PartitionProgress>()
		const assignment: Array<{ topic: string; partition: number; offset: bigint }> = []
		const rebuilt: number[] = []

		for (const p of plan) {
			const resumable = p.resumable && !rebuildAll
			if (!resumable) {
				if (!wiped.has(p.store)) {
					wiped.add(p.store)
					await clearAll(p.store)
				}
				rebuilt.push(p.partition)
			}
			const startOffset = resumable ? p.checkpoint! : p.earliest
			if (startOffset >= p.end) continue
			progress.set(p.partition, { endOffset: p.end, position: startOffset, fetched: 0, handled: 0 })
			assignment.push({ topic: this.topicName, partition: p.partition, offset: startOffset })
		}

		let restoredCount = 0

		if (assignment.length > 0) {
			restoredCount = await this.replay(client, options, assignment, progress)
		}

		if (checkpointStore) {
			const logger = client.cluster.getLogger()
			// Partitions that were replayed, plus partitions that were wiped because their
			// checkpoint was unusable (their state is now exactly "empty as of end offset").
			const toCheckpoint = new Set<number>([...assignment.map(a => a.partition), ...rebuilt])
			await Promise.all(
				plan
					.filter(p => toCheckpoint.has(p.partition))
					.map(async p => {
						try {
							await checkpointStore.set(this.topicName, p.partition, p.end)
						} catch (err) {
							// Restored state is in place regardless; failing to record this means a future
							// restart will re-restore (idempotent but wasteful). Log so it is visible.
							const error = err instanceof Error ? err : new Error(String(err))
							logger.error('post-restore checkpoint persistence failed', {
								topic: this.topicName,
								partition: p.partition,
								error: error.message,
							})
						}
					})
			)
			await checkpointStore.flush?.()
		}

		return restoredCount
	}

	private async replay(
		client: KafkaClient,
		options: ChangelogRestorationOptions | undefined,
		assignment: Array<{ topic: string; partition: number; offset: bigint }>,
		progress: Map<number, PartitionProgress>
	): Promise<number> {
		const uniqueGroupId = `${this.topicName}-restorer-${Date.now()}-${Math.random().toString(36).slice(2)}`
		const consumer = client.consumer({
			groupId: uniqueGroupId,
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
			...(options?.consumerMaxWaitMs !== undefined ? { maxWaitMs: options.consumerMaxWaitMs } : {}),
		})

		const pending = new Set<number>(progress.keys())
		const abortController = new AbortController()
		const restoreCompleteReason = Symbol('changelogRestoreComplete')
		const idleTimeoutMs = options?.idleTimeoutMs ?? DEFAULT_RESTORE_IDLE_TIMEOUT_MS
		const initialIdleTimeoutMs = options?.initialIdleTimeoutMs ?? idleTimeoutMs
		const checkIntervalMs = options?.checkIntervalMs ?? 1000

		let restoredCount = 0
		let lastProgressTime = Date.now()
		let isRunning = false
		let madeProgress = false
		let inFlight = 0

		// A partition is complete once the consumer's fetch position has reached the end offset
		// AND every record it fetched has been applied. Position alone is not enough (fetched
		// records may still be queued); reaching the last record is not enough either (on
		// transactional changelogs the end offset sits past trailing commit markers).
		const maybeComplete = (partition: number) => {
			const state = progress.get(partition)
			if (!state || !pending.has(partition)) return
			if (state.position < state.endOffset || state.handled < state.fetched) return
			pending.delete(partition)
			try {
				consumer.pause([{ topic: this.topicName, partition }])
			} catch {
				// best effort; the consumer is stopped once all partitions complete
			}
			if (pending.size === 0) {
				abortController.abort(restoreCompleteReason)
			}
		}

		consumer.once('running', () => {
			isRunning = true
			lastProgressTime = Date.now()
		})
		consumer.on('fetchPosition', (position, recordCount) => {
			if (position.topic !== this.topicName) return
			const state = progress.get(position.partition)
			if (!state) return
			lastProgressTime = Date.now()
			madeProgress = true
			state.fetched += recordCount
			if (position.offset > state.position) state.position = position.offset
			maybeComplete(position.partition)
		})

		const admin = client.admin()
		const startedAt = Date.now()
		let checking = false
		let warnedOpenTransaction = false
		// True when every pending partition has consumed all stable data and is only held back by
		// an open transaction (last stable offset below the restore target).
		const blockedByOpenTransaction = async (): Promise<boolean> => {
			const stable = await admin.fetchTopicOffsets(this.topicName, [...pending], 'latest', {
				isolationLevel: 'read_committed',
			})
			for (const partition of pending) {
				const state = progress.get(partition)
				const lastStable = stable.get(partition)
				if (!state || lastStable === undefined) return false
				if (lastStable >= state.endOffset || state.position < lastStable) return false
			}
			return true
		}

		const checkIdle = setInterval(() => {
			if (checking || !isRunning || abortController.signal.aborted || pending.size === 0) return
			// A slow store write is progress in the making, not a stall.
			if (inFlight > 0) return
			const timeoutMs = madeProgress ? idleTimeoutMs : initialIdleTimeoutMs
			if (Date.now() - lastProgressTime <= timeoutMs) return

			checking = true
			void blockedByOpenTransaction()
				.catch(() => false)
				.then(blocked => {
					checking = false
					if (abortController.signal.aborted || pending.size === 0) return
					if (blocked && Date.now() - startedAt < MAX_OPEN_TRANSACTION_WAIT_MS) {
						// Not a stall: the broker will abort the dangling transaction once its
						// transaction timeout expires, after which the remaining records arrive.
						lastProgressTime = Date.now()
						if (!warnedOpenTransaction) {
							warnedOpenTransaction = true
							client.cluster
								.getLogger()
								.warn('changelog restore is waiting for an open transaction to complete', {
									topic: this.topicName,
									partitions: [...pending],
								})
						}
						return
					}
					abortController.abort(
						new Error(
							`Timed out restoring changelog "${this.topicName}": no progress for ${timeoutMs}ms ` +
								`(pending partitions: ${[...pending].join(', ')})`
						)
					)
				})
		}, checkIntervalMs)

		let handlerError: unknown = null
		try {
			await consumer.runEach(
				[this.topicName],
				async message => {
					const state = progress.get(message.partition)
					if (!state) return
					inFlight += 1
					try {
						// Records past the captured end offset belong to a concurrent writer; the
						// partition's owner will have them in its own state.
						if (message.offset < state.endOffset && message.key !== null) {
							const store = await this.storeFor(message.partition)
							const key = this.keyCodec.decode(message.key)
							if (message.value === null) {
								await store.delete(key)
							} else {
								await store.put(key, this.valueCodec.decode(message.value))
								restoredCount++
							}
						}
					} catch (err) {
						handlerError = err
						throw err
					} finally {
						inFlight -= 1
						state.handled += 1
						lastProgressTime = Date.now()
						madeProgress = true
					}
					maybeComplete(message.partition)
				},
				{
					commitOffsets: false,
					assignment,
					signal: abortController.signal,
				}
			)
		} finally {
			clearInterval(checkIdle)
		}

		if (handlerError) {
			throw handlerError instanceof Error ? handlerError : new Error(String(handlerError))
		}
		const abortReason: unknown = abortController.signal.reason
		if (abortReason instanceof Error) {
			throw abortReason
		}
		if (pending.size > 0) {
			throw new Error(
				`Restoring changelog "${this.topicName}" stopped before completion ` +
					`(pending partitions: ${[...pending].join(', ')})`,
				{ cause: abortReason }
			)
		}

		return restoredCount
	}
}

const DEFAULT_RESTORE_IDLE_TIMEOUT_MS = 30_000
/** Upper bound for waiting on a dangling transaction (brokers cap transaction timeouts at 15 minutes by default). */
const MAX_OPEN_TRANSACTION_WAIT_MS = 16 * 60_000

async function clearAll<K, V>(store: KeyValueStore<K, V>): Promise<void> {
	const keys: K[] = []
	for await (const [key] of store.all()) {
		keys.push(key)
	}
	for (const key of keys) {
		await store.delete(key)
	}
}

/**
 * Creates a windowed key codec by encoding key + window boundaries.
 */
export function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode(windowed: WindowedKey<K>): Buffer {
			const keyBuffer = keyCodec.encode(windowed.key)
			const windowBuffer = Buffer.alloc(16)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowStart), 0)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowEnd), 8)
			return Buffer.concat([keyBuffer, windowBuffer])
		},
		decode(buffer: Buffer): WindowedKey<K> {
			const keyBuffer = buffer.subarray(0, buffer.length - 16)
			const windowBuffer = buffer.subarray(buffer.length - 16)
			return {
				key: keyCodec.decode(keyBuffer),
				windowStart: Number(windowBuffer.readBigInt64BE(0)),
				windowEnd: Number(windowBuffer.readBigInt64BE(8)),
			}
		},
	}
}
