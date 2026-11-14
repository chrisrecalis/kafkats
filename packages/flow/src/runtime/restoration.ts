import { ErrorCode, KafkaError, type KafkaClient, type Logger } from '@kafkats/client'
import { ChangelogRestorer } from '@/changelog.js'
import type { KeyValueStore } from '@/state.js'
import { clearStore } from '@/state/partitioned.js'
import type { FlowConfig } from '@/types.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
import { toError } from '@/runtime/errors.js'
import type { AppStatus } from '@/runtime/status.js'
import type { StoreRegistry } from '@/runtime/store-registry.js'

/** How long after a revocation unowned in-memory state partitions are kept before being dropped. */
const UNOWNED_STATE_PRUNE_DELAY_MS = 30_000

export type TopicPartition = { topic: string; partition: number }

export type RestorationDeps = {
	client: KafkaClient
	config: FlowConfig
	stores: StoreRegistry
	checkpoints: Checkpoints
	status: AppStatus
	logger: Logger
	/** Whether a worker of this instance currently owns state partition `partition` fed by `sourceTopics`. */
	isStatePartitionOwned: (sourceTopics: Set<string>, partition: number) => boolean
}

/**
 * Restores state stores from their changelogs and releases state of partitions no longer owned.
 *
 * All work on state partitions (restore, release, prune) runs through one serialized queue, so a
 * partition is never restored and dropped concurrently.
 */
export class Restoration {
	private queue: Promise<void> = Promise.resolve()
	private pruneTimer: ReturnType<typeof setTimeout> | null = null

	constructor(private readonly deps: RestorationDeps) {}

	/** App-scoped stores hold state for all partitions; rebuild them fully before processing. */
	async restoreAppScopedStores(): Promise<void> {
		await Promise.all(
			[...this.deps.stores.definitions]
				.filter(def => def.scope === 'app' && def.changelog && !def.changelog.skipRestoration)
				.map(async def => {
					const spec = def.changelog!
					const stack = await def.partitions.forPartition(0)
					const restorer = new ChangelogRestorer(
						spec.topicName,
						spec.keyCodec,
						spec.valueCodec,
						stack.raw as unknown as KeyValueStore<unknown, unknown>
					)
					await this.runRestorer(restorer, undefined)
				})
		)
	}

	/**
	 * Restore the state partitions that back newly assigned source partitions.
	 * Restorations are serialized so a partition is never restored twice concurrently.
	 */
	enqueue(partitions: TopicPartition[]): Promise<void> {
		const task = this.queue.then(() => this.restorePartitions(partitions))
		this.queue = task.catch(err => {
			// A restore failure during startup fails start() (see drain).
			this.deps.status.failStartup(toError(err))
		})
		return task
	}

	/** Wait until every queued restoration (including ones queued meanwhile) is done. */
	async drain(): Promise<void> {
		let seen: Promise<void> | null = null
		while (seen !== this.queue) {
			seen = this.queue
			await seen
			this.deps.status.assertNotClosing()
		}
	}

	/** Settles once all queued work is done; never rejects. */
	settled(): Promise<void> {
		return this.queue.catch(() => {})
	}

	/**
	 * Partitions were revoked or lost: drop anything buffered for them and schedule a cleanup.
	 *
	 * The local copy is kept for now, because the partition often comes straight back (every
	 * rebalance revokes before it re-assigns) and can then be caught up from its checkpoint instead
	 * of being rebuilt from the start of the changelog.
	 */
	release(released: TopicPartition[]): void {
		if (released.length === 0) return
		const task = this.queue.then(async () => {
			for (const def of this.deps.stores.definitions) {
				if (def.scope !== 'partition') continue
				const partitions = new Set(
					released.filter(tp => def.sourceTopics.has(tp.topic)).map(tp => tp.partition)
				)
				for (const partition of partitions) {
					if (this.deps.isStatePartitionOwned(def.sourceTopics, partition)) continue
					if (!def.partitions.materializedPartitions().includes(partition)) continue
					const stack = await def.partitions.forPartition(partition)
					stack.buffer.discardBuffer()
				}
			}
		})
		this.queue = task.catch(err => {
			this.deps.logger.warn('failed to release state of revoked partitions', { error: toError(err).message })
		})
		this.schedulePrune()
	}

	/** Cancel the pending prune (on shutdown). */
	cancelPrune(): void {
		if (this.pruneTimer) {
			clearTimeout(this.pruneTimer)
			this.pruneTimer = null
		}
	}

	private async restorePartitions(assigned: TopicPartition[]): Promise<void> {
		if (!this.deps.status.clusterAvailable || assigned.length === 0) return

		await Promise.all(
			[...this.deps.stores.definitions]
				.filter(def => def.scope === 'partition' && def.changelog && !def.changelog.skipRestoration)
				.map(async def => {
					const spec = def.changelog!
					const partitions = [
						...new Set(assigned.filter(tp => def.sourceTopics.has(tp.topic)).map(tp => tp.partition)),
					]
					if (partitions.length === 0) return

					const restorer = new ChangelogRestorer(
						spec.topicName,
						spec.keyCodec,
						spec.valueCodec,
						async partition => {
							const stack = await def.partitions.forPartition(partition)
							// Anything still buffered belongs to a transaction that no longer exists.
							stack.buffer.discardBuffer()
							return stack.raw as unknown as KeyValueStore<unknown, unknown>
						}
					)
					await this.runRestorer(restorer, partitions)
				})
		)
	}

	private async runRestorer(
		restorer: ChangelogRestorer<unknown, unknown>,
		partitions: number[] | undefined
	): Promise<void> {
		try {
			await restorer.restore(
				this.deps.client,
				this.deps.config.changelog?.restoration,
				partitions,
				this.deps.checkpoints.store
			)
		} catch (err) {
			// A changelog topic that doesn't exist yet has nothing to restore (first startup with
			// autoCreate disabled); anything else must fail loudly rather than run on partial state.
			const error = err as Error & { code?: string }
			if (error.code === 'UNKNOWN_TOPIC_OR_PARTITION') return
			if (error instanceof KafkaError && error.errorCode === ErrorCode.UnknownTopicOrPartition) return
			throw err
		}
	}

	/**
	 * Once assignments have settled, drop the in-memory state of partitions this instance no longer
	 * owns. Another instance is advancing that state now, so the local copy would only go stale and
	 * hold memory. Durable state (with durable checkpoints) is kept on disk for a later return.
	 */
	private schedulePrune(): void {
		if (this.deps.checkpoints.durable) return
		if (this.pruneTimer) clearTimeout(this.pruneTimer)
		this.pruneTimer = setTimeout(() => {
			this.pruneTimer = null
			const { status, stores, checkpoints } = this.deps
			if (status.closing) return
			const task = this.queue.then(async () => {
				for (const def of stores.definitions) {
					if (def.scope !== 'partition') continue
					for (const partition of def.partitions.materializedPartitions()) {
						if (status.closing || this.deps.isStatePartitionOwned(def.sourceTopics, partition)) continue
						const stack = await def.partitions.forPartition(partition)
						stack.buffer.discardBuffer()
						await clearStore(stack.raw as unknown as KeyValueStore<unknown, unknown>)
						def.partitions.forget(partition)
						if (def.changelog) {
							checkpoints.forgetInProcess(def.changelog.topicName, partition)
						}
					}
				}
			})
			this.queue = task.catch(err => {
				this.deps.logger.warn('failed to prune state of unowned partitions', { error: toError(err).message })
			})
		}, UNOWNED_STATE_PRUNE_DELAY_MS)
		this.pruneTimer.unref?.()
	}
}
