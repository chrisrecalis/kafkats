/**
 * Purge of repartition topics (4.8.6, C12, KS `TaskManager.maybePurgeCommittedRecords`): the app never reads records
 * of a repartition topic below its committed offset again, so after commits, at most once per `intervalMs`, they are
 * deleted (DeleteRecords). Repartition topics keep their records forever otherwise (`retention.ms=-1`, 5.4). Best
 * effort: one request at a time, never awaited by a commit; a failure is logged and its offsets go with the next one.
 */
import type { Logger } from '@kafkats/client'
import type { CommitListener } from '@/runtime/config.js'
import { errorMessage } from '@/runtime/errors.js'
import type { AdminPort, Clock, OffsetAndMetadata } from '@/runtime/io.js'
import { cap } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'

/** How long one DeleteRecords request may take. */
const PURGE_TIMEOUT_MS = 10_000

interface PurgeOffset {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
}

export interface RepartitionPurgerDeps {
	readonly admin: Pick<AdminPort, 'deleteRecords'>
	/** The repartition topics the app reads. */
	readonly topics: ReadonlySet<string>
	/** `repartitionPurgeIntervalMs`; 0 disables purging. */
	readonly intervalMs: number
	readonly clock: Clock
	/** The run's hard stop: cuts a request in flight at the close deadline. */
	readonly signal: AbortSignal
	readonly logger: Logger
}

/** One per Run: told about every successful commit. */
export class RepartitionPurger implements CommitListener {
	/** Per repartition partition: the offset committed last and not purged yet. */
	private readonly pending = new Map<string, PurgeOffset>()
	/** Failure messages logged at warn already (a denied DeleteRecords fails the same way at every interval). */
	private readonly reported = new Set<string>()
	private lastPurgeAt: number
	private purging = false

	constructor(private readonly deps: RepartitionPurgerDeps) {
		this.lastPurgeAt = deps.clock.now()
	}

	noteCommitted(offsets: readonly OffsetAndMetadata[]): void {
		const { topics, intervalMs, clock } = this.deps
		if (intervalMs === 0) {
			return
		}
		for (const { topic, partition, offset } of offsets) {
			if (topics.has(topic)) this.pending.set(tpKey({ topic, partition }), { topic, partition, offset })
		}
		const now = clock.now()
		if (this.pending.size > 0 && !this.purging && now - this.lastPurgeAt >= intervalMs) {
			this.lastPurgeAt = now
			void this.purge()
		}
	}

	private async purge(): Promise<void> {
		const { admin, signal, logger } = this.deps
		const offsets = [...this.pending.values()]
		this.pending.clear()
		this.purging = true
		try {
			await admin.deleteRecords(offsets, { signal: cap(signal, PURGE_TIMEOUT_MS) })
			logger.debug('purged repartition topics', { partitions: offsets.length })
		} catch (error) {
			// Offsets committed meanwhile are newer: they replace the failed ones.
			for (const offset of offsets) {
				const key = tpKey(offset)
				if (!this.pending.has(key)) this.pending.set(key, offset)
			}
			const message = errorMessage(error)
			const first = !this.reported.has(message)
			this.reported.add(message)
			logger[first ? 'warn' : 'debug']('purging repartition topics failed: retried after a later commit', {
				error: message,
			})
		} finally {
			this.purging = false
		}
	}
}
