/**
 * The prune of unowned state (4.10.4, runtime§6.29, CR6), like Kafka Streams' state directory cleaner. While every task
 * of a run is RUNNING, the store instances no task leased (another instance of the application owns their partitions
 * now) are dropped: in-memory state 30 s after the run became RUNNING, durable state once no task held it for the
 * cleanup delay too (`StateDirectory.unleased`, KS `state.cleanup.delay.ms`). Outside the scheduler's exclusive
 * section, like the cleaner beside its stream threads: a prune clears a whole store, which must not hold up slices,
 * commits and rebalance hooks. That is safe because StateDirectory.prune re-checks the lease and invalidates the
 * checkpoint before it clears, and a lease made meanwhile stops the clear, which the new owner's restore pipeline
 * finishes. A rebalance, a revival, a recovery and close disarm it, also while it runs: no further instance is pruned
 * then. A failed prune is logged and retried 30 s later, like KS's cleaner goes on with the other directories and runs
 * again.
 */
import type { Logger } from '@kafkats/client'
import { errorMessage } from '@/runtime/errors.js'
import type { Clock } from '@/runtime/io.js'
import type { StateDirectory, UnleasedInstance } from '@/state/state-directory.js'

/** How long a run stays RUNNING before the state it does not own is dropped, and before a failed prune is tried again. */
export const PRUNE_DELAY_MS = 30_000

const key = (store: string, partition: number): string => `${store}\u0000${partition}`

export interface PruneTimerDeps {
	readonly stateDirectory: Pick<StateDirectory, 'unleased' | 'prune'>
	readonly clock: Clock
	/** The run's hard stop. */
	readonly signal: AbortSignal
	readonly logger: Logger
}

export class PruneTimer {
	private cancel: (() => void) | undefined
	/** Moves with every disarm: a prune that fired before stops before its next instance. */
	private generation = 0
	/** When the run last became RUNNING (the last arm). */
	private runningSince = 0
	/** Per instance whose prune failed, when it is tried again. */
	private readonly retryAt = new Map<string, number>()

	constructor(private readonly deps: PruneTimerDeps) {}

	/** Every task is RUNNING: prunes each unowned instance once it is due, unless disarmed meanwhile. */
	arm(): void {
		if (this.cancel !== undefined) {
			return
		}
		this.runningSince = this.deps.clock.now()
		this.schedule(this.generation)
	}

	disarm(): void {
		this.generation++
		this.cancel?.()
		this.cancel = undefined
	}

	/** A timer for the first instance due; nothing to prune, no timer. */
	private schedule(generation: number): void {
		let first = Infinity
		for (const instance of this.deps.stateDirectory.unleased()) first = Math.min(first, this.dueAt(instance))
		if (first === Infinity) {
			return
		}
		this.cancel = this.deps.clock.setTimer(Math.max(first, this.runningSince + PRUNE_DELAY_MS), () => {
			this.cancel = undefined
			void this.prune(generation)
		})
	}

	/** Prunes the instances due, one after the other (a failure is logged and retried later), then waits for the next. */
	private async prune(generation: number): Promise<void> {
		const { stateDirectory, clock, signal, logger } = this.deps
		for (const instance of stateDirectory.unleased()) {
			if (generation !== this.generation || signal.aborted) {
				return
			}
			if (this.dueAt(instance) > clock.now()) continue
			const { store, partition } = instance
			try {
				await stateDirectory.prune(store, partition, signal)
				this.retryAt.delete(key(store, partition))
			} catch (error) {
				if (signal.aborted) return
				this.retryAt.set(key(store, partition), clock.now() + PRUNE_DELAY_MS)
				logger.warn('dropping unowned state failed: it is tried again in 30 s', {
					store,
					partition,
					error: errorMessage(error),
				})
			}
		}
		if (generation === this.generation) {
			this.schedule(generation)
		}
	}

	private dueAt({ store, partition, prunableAt }: UnleasedInstance): number {
		return Math.max(prunableAt, this.retryAt.get(key(store, partition)) ?? -Infinity)
	}
}
