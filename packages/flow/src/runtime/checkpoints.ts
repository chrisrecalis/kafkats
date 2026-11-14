import type { Logger } from '@kafkats/client'
import type { ChangelogCheckpointStore, StateStoreProvider } from '@/state.js'
import { toError } from '@/runtime/errors.js'
import type { AppStatus } from '@/runtime/status.js'

export type ChangelogPosition = { topic: string; partition: number; offset: bigint }

/**
 * Changelog positions written by a worker's open transaction. They become checkpoints only once
 * the transaction has committed (see Checkpoints.flush); an abort discards them.
 */
export class PendingCheckpoints {
	private readonly positions = new Map<string, ChangelogPosition>()

	/** A changelog record of the open transaction was acknowledged at `offset`. */
	track(topic: string, partition: number, offset: bigint): void {
		const key = `${topic}:${partition}`
		const nextOffset = offset + 1n
		const existing = this.positions.get(key)
		if (!existing || nextOffset > existing.offset) {
			this.positions.set(key, { topic, partition, offset: nextOffset })
		}
	}

	/** The position after the last record of the transaction, per changelog partition. */
	list(): ChangelogPosition[] {
		return [...this.positions.values()]
	}

	clear(): void {
		this.positions.clear()
	}
}

/**
 * Changelog restore progress (the offset up to which a local state partition reflects its changelog).
 *
 * Providers with durable state supply a durable store; otherwise progress is tracked in-process,
 * which is enough to catch a retained partition up after a rebalance instead of rebuilding it from
 * the start of the changelog.
 */
export class Checkpoints {
	readonly store: ChangelogCheckpointStore
	readonly durable: boolean
	private readonly inProcess = new Map<string, bigint>()
	/** Last persistence error logged, to log a persistent failure once instead of on every commit. */
	private lastErrorMessage: string | null = null

	constructor(
		provider: StateStoreProvider,
		private readonly status: AppStatus,
		private readonly logger: Logger
	) {
		const durable = provider.getChangelogCheckpointStore?.()
		this.durable = durable !== undefined
		const inProcess = this.inProcess
		this.store = durable ?? {
			get: (topic, partition) => Promise.resolve(inProcess.get(`${topic}:${partition}`)),
			set: (topic, partition, offset) => {
				inProcess.set(`${topic}:${partition}`, offset)
				return Promise.resolve()
			},
		}
	}

	/** Checkpoint a changelog record written outside of a transaction (`offset` is the record's). */
	async recordWrite(topic: string, partition: number, offset: bigint): Promise<void> {
		try {
			await this.store.set(topic, partition, offset + 1n)
		} catch (err) {
			this.recordError(err, { topic, partition })
		}
	}

	/**
	 * Checkpoint the changelog positions of a committed transaction.
	 *
	 * Data is already durable post-EOS-commit; surface failures so a stuck checkpoint store doesn't
	 * silently cause restore-time over-replay.
	 */
	async flush(checkpoints: ChangelogPosition[]): Promise<void> {
		if (checkpoints.length === 0) {
			return
		}

		let anyFailure = false
		for (const checkpoint of checkpoints) {
			try {
				// `offset` is the position after the last changelog record of the transaction; the
				// transaction's commit marker occupies that position, so the committed state ends
				// one past it. (A state partition has a single writer, so nothing else can sit
				// in between; restoration tolerates either position.)
				await this.store.set(checkpoint.topic, checkpoint.partition, checkpoint.offset + 1n)
			} catch (err) {
				anyFailure = true
				this.recordError(err, { topic: checkpoint.topic, partition: checkpoint.partition })
			}
		}
		// One fsync covers all the puts above; awaiting per-set would be O(partitions) syscalls per commit.
		try {
			await this.store.flush?.()
		} catch (err) {
			anyFailure = true
			this.recordError(err, { phase: 'flush' })
		}
		if (!anyFailure && this.lastErrorMessage !== null) {
			this.logger.info('checkpoint persistence recovered')
			this.lastErrorMessage = null
		}
	}

	/** Drop the in-process checkpoint of a state partition whose local state was discarded. */
	forgetInProcess(topic: string, partition: number): void {
		this.inProcess.delete(`${topic}:${partition}`)
	}

	/** Drop all in-process checkpoints (the in-process stores they describe are being dropped). */
	clearInProcess(): void {
		this.inProcess.clear()
	}

	private recordError(err: unknown, context: Record<string, unknown>): void {
		const error = toError(err)
		this.status.lastError = error
		// Dedupe log lines: persistent disk-full would otherwise emit per-partition every commit interval.
		if (this.lastErrorMessage !== error.message) {
			this.lastErrorMessage = error.message
			this.logger.error('checkpoint persistence failed', { ...context, error: error.message })
		}
	}
}
