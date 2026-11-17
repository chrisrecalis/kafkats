/**
 * Changelog restoration (4.7.2): brings stores up to the end their changelog partitions had when the restore started.
 *
 * Guarantees:
 * - a restore resolves only once every target is replayed up to the end offset captured at its start (the high
 *   watermark, read with read_committed); a stall is an error, never a silent partial restore
 * - a checkpoint is trusted only when it can describe its store (within the log and the store is not empty); otherwise
 *   it is invalidated before the store is wiped and rebuilt from the start of the log, so stale local entries cannot
 *   survive and an interrupted rebuild is never trusted later
 * - checkpoints never claim more than the store holds: progress checkpoints cover applied batches only, and the end is
 *   checkpointed once a target is caught up
 */
import type { KafkaClient, Message } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { ChangelogRestorationOptions } from '@/changelog/config.js'
import { ChangelogDecodeError } from '@/changelog/errors.js'
import { Replay, type ReplayOptions, type ReplayTarget } from '@/changelog/replay.js'
import type { AdminPort, RestoreConsumerPort } from '@/runtime/io.js'
import { KafkaAdmin } from '@/runtime/kafka-admin.js'
import { KafkaRestoreConsumer } from '@/runtime/kafka-consumers.js'
import { abortError } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { ChangelogCheckpointStore, KeyValueStore } from '@/state.js'
import { clearStore, latestPerKey } from '@/state/bulk.js'
import { Checkpoints } from '@/state/checkpoints.js'

export type { ReplayOptions as RestoreOptions } from '@/changelog/replay.js'

export interface RestoreTarget extends ReplayTarget {
	/** The raw store (below the write buffer and the logging layer, runtime§6.36). Targets may share one store. */
	readonly store: KeyValueStore<unknown, unknown>
}

export interface RestoreResult {
	/** Records applied that are not tombstones (overwritten ones included). */
	readonly restored: number
	/** Targets (`topic:partition`) whose checkpoint was not trusted: invalidated, store wiped, replayed from the start. */
	readonly rebuilt: readonly string[]
	/** Targets that had records to replay. */
	readonly replayed: readonly string[]
	/** Max key time applied per target. */
	readonly maxKeyTime: ReadonlyMap<string, number>
}

export interface RestoreIO {
	/** Called only when a target has records to replay. */
	readonly consumer: () => RestoreConsumerPort
	readonly admin: AdminPort
}

/** Where the restore of one target starts and ends. */
interface Plan {
	readonly target: RestoreTarget
	readonly key: string
	readonly start: bigint
	/** The high watermark when the restore started: the replay stops there. */
	readonly end: bigint
	readonly rebuild: boolean
}

/**
 * Restores every target from its changelog partition. A missing changelog topic has nothing to restore. Rejects with
 * the client's OffsetOutOfRangeError when the log lost records the replay still needed, with ChangelogDecodeError
 * when a record does not decode, and with an AbortError once `options.signal` aborts (after the write in flight).
 */
export async function restoreChangelogs(
	io: RestoreIO,
	targets: readonly RestoreTarget[],
	checkpoints: Checkpoints,
	options: ReplayOptions
): Promise<RestoreResult> {
	const { signal } = options
	const plans = await plan(io.admin, targets, checkpoints, signal)
	const rebuilt = plans.filter(p => p.rebuild)
	// Invalidated first: a rebuild cut short must never be taken for the state its old checkpoint described (CR3).
	await Promise.all(rebuilt.map(({ target }) => checkpoints.invalidate(target.topic, target.partition)))
	for (const store of new Set(rebuilt.map(({ target }) => target.store))) {
		await clearStore(store, signal)
	}
	if (signal.aborted) {
		throw abortError(signal)
	}
	const replayed = plans.filter(p => p.start < p.end)
	const { restored, maxKeyTime } =
		replayed.length > 0
			? await new Replay(io, replayed, checkpoints, options).run()
			: { restored: 0, maxKeyTime: new Map<string, number>() }
	// Rebuilt targets with nothing to replay hold exactly "empty as of the end" now.
	const caughtUp = plans.filter(p => p.rebuild || p.start < p.end)
	if (caughtUp.length > 0) {
		await Promise.all(caughtUp.map(({ target, end }) => checkpoints.set(target.topic, target.partition, end)))
		await checkpoints.flush()
	}
	return {
		restored,
		rebuilt: rebuilt.map(p => p.key),
		replayed: replayed.map(p => p.key),
		maxKeyTime,
	}
}

/** Per target: the log's range when the restore starts, and whether its checkpoint is trusted. */
async function plan(
	admin: AdminPort,
	targets: readonly RestoreTarget[],
	checkpoints: Checkpoints,
	signal: AbortSignal
): Promise<Plan[]> {
	const counts = await admin.describeTopics([...new Set(targets.map(target => target.topic))], { signal })
	const existing = targets.filter(target => counts.has(target.topic))
	if (existing.length === 0) {
		return []
	}
	const tps = existing.map(({ topic, partition }) => ({ topic, partition }))
	// The end is the high watermark, not the last stable offset: while a crashed instance's transaction is open, the
	// last stable offset stays at its start although later transactions (of its successor) committed beyond it, and
	// stopping there would restore state behind the committed input offsets. Reading up to the high watermark with
	// read_committed waits for the broker to abort the dangling transaction and delivers everything committed.
	const [earliest, end] = await Promise.all([
		admin.listOffsets(tps, 'earliest', 'read_committed', { signal }),
		admin.listOffsets(tps, 'latest', 'read_uncommitted', { signal }),
	])
	const candidates = await Promise.all(
		existing.map(async target => {
			const key = tpKey(target)
			const first = earliest.get(key)
			const last = end.get(key)
			if (first === undefined || last === undefined) {
				throw new Error(`Missing offsets of changelog '${target.topic}' partition ${target.partition}`)
			}
			const checkpoint = await checkpoints.get(target.topic, target.partition)
			const trusted = await isTrusted(checkpoint, first, last, target.store)
			return { target, key, earliest: first, end: last, checkpoint: trusted ? checkpoint : undefined }
		})
	)
	// A wipe clears every partition of a store: targets sharing it are rebuilt together.
	const wiped = new Set(candidates.filter(c => c.checkpoint === undefined).map(c => c.target.store))
	return candidates.map(({ target, key, earliest: first, end: last, checkpoint }) =>
		checkpoint === undefined || wiped.has(target.store)
			? { target, key, end: last, rebuild: true, start: first }
			: { target, key, end: last, rebuild: false, start: checkpoint }
	)
}

/**
 * The trust rule (runtime§6.35): within the log, and the store can hold what it claims. A checkpoint past the log
 * start on an empty store claims progress the store cannot contain (its state was replaced). The invalidation
 * sentinel -1n and a checkpoint of a longer, recreated changelog fall outside the log.
 */
async function isTrusted(
	checkpoint: bigint | undefined,
	earliest: bigint,
	end: bigint,
	store: KeyValueStore<unknown, unknown>
): Promise<boolean> {
	if (checkpoint === undefined || checkpoint < earliest || checkpoint > end) {
		return false
	}
	return checkpoint === earliest || (await store.approximateNumEntries()) !== 0
}

/**
 * The target of one changelog partition and its store. A batch is applied as the latest change of each key
 * (runtime§6.37), decoded with the changelog's codecs, written together (state/bulk.ts); `keyTime` gives the key time
 * of a written key (window start, session end) for the store's clock.
 */
export function restoreTarget<K, V>(
	changelog: { readonly topic: string; readonly partition: number },
	store: KeyValueStore<K, V>,
	codecs: { readonly key: Codec<K>; readonly value: Codec<V> },
	keyTime?: (key: K) => number
): RestoreTarget {
	const { topic, partition } = changelog
	const decode = (batch: readonly Message<Buffer, Buffer>[]): { key: K; value: V | null }[] => {
		try {
			return latestPerKey(batch).map(({ key, value }) => ({
				key: codecs.key.decode(key),
				value: value === null ? null : codecs.value.decode(value),
			}))
		} catch (error) {
			throw new ChangelogDecodeError(topic, partition, { cause: error })
		}
	}
	return {
		topic,
		partition,
		store,
		apply: async batch => {
			let maxKeyTime = -1
			await Promise.all(
				decode(batch).map(({ key, value }) => {
					if (keyTime) maxKeyTime = Math.max(maxKeyTime, keyTime(key))
					return value === null ? store.delete(key) : store.put(key, value)
				})
			)
			return maxKeyTime
		},
	}
}

const DEFAULT_IDLE_TIMEOUT_MS = 30_000
const DEFAULT_CHECK_INTERVAL_MS = 1000

/** Restores one store from the partitions of one changelog topic (a standalone utility; the runtime restores tasks). */
export class ChangelogRestorer<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly target: KeyValueStore<K, V>
	) {}

	/**
	 * Replays the changelog topic into the store: the given partitions, else all. With a checkpoint store, a trusted
	 * checkpoint is resumed from and the end is checkpointed. The partitions share the store: if one of them is not
	 * trusted, all are rebuilt.
	 * @returns Number of records restored (tombstones are not counted)
	 */
	async restore(
		client: KafkaClient,
		options: ChangelogRestorationOptions = {},
		partitions?: number[],
		checkpointStore?: ChangelogCheckpointStore
	): Promise<number> {
		const { topicName: topic } = this
		const logger = client.cluster.getLogger()
		const admin = new KafkaAdmin(client.admin())
		const signal = new AbortController().signal
		const count = (await admin.describeTopics([topic], { signal })).get(topic) ?? 0
		const requested = [...new Set(partitions ?? Array.from({ length: count }, (_, partition) => partition))]
		const codecs = { key: this.keyCodec, value: this.valueCodec }
		const checkpoints = new Checkpoints(checkpointStore, error => {
			logger.error('changelog checkpoint persistence failed', { topic, error: error.message })
		})
		const idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
		const { restored } = await restoreChangelogs(
			{
				consumer: () =>
					new KafkaRestoreConsumer(client, {
						groupId: `${topic}-restorer-${Date.now()}-${Math.random().toString(36).slice(2)}`,
						isolationLevel: 'read_committed',
						autoOffsetReset: 'none',
						...(options.consumerMaxWaitMs !== undefined ? { maxWaitMs: options.consumerMaxWaitMs } : {}),
					}),
				admin,
			},
			requested.map(partition => restoreTarget({ topic, partition }, this.target, codecs)),
			checkpoints,
			{
				idleTimeoutMs,
				initialIdleTimeoutMs: options.initialIdleTimeoutMs ?? idleTimeoutMs,
				checkIntervalMs: options.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS,
				// Only the end is checkpointed (and the progress, when the restore fails).
				commitIntervalMs: Number.POSITIVE_INFINITY,
				signal,
				logger,
			}
		)
		return restored
	}
}
