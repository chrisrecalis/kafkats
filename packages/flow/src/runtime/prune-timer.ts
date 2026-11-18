/**
 * The prune of unowned state (4.10.4, runtime§6.29, CR6), like Kafka Streams' state directory cleaner. While every task
 * of a run is RUNNING, the store instances no task leased (another instance of the application owns their partitions
 * now) are dropped: in-memory state 30 s after the run became RUNNING, durable state once no task held it for the
 * cleanup delay too (`StateDirectory.unleased`, KS `state.cleanup.delay.ms`). Outside the scheduler's exclusive
 * section, like the cleaner beside its stream threads: a prune clears a whole store, which must not hold up slices,
 * commits and rebalance hooks. That is safe because StateDirectory.prune re-checks the lease and invalidates the
 * checkpoint before it clears, and a lease made meanwhile waits for the prune and gets a new, empty instance. A
 * rebalance, a revival, a recovery and close disarm it, also while it runs: no further instance is pruned then.
 */
import type { Logger } from '@kafkats/client'
import { errorMessage } from '@/runtime/errors.js'
import type { Clock } from '@/runtime/io.js'
import type { StateDirectory } from '@/state/state-directory.js'

/** How long a run stays RUNNING before the state it does not own is dropped. */
export const PRUNE_DELAY_MS = 30_000

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
		for (const { prunableAt } of this.deps.stateDirectory.unleased()) first = Math.min(first, prunableAt)
		if (first === Infinity) {
			return
		}
		this.cancel = this.deps.clock.setTimer(Math.max(first, this.runningSince + PRUNE_DELAY_MS), () => {
			this.cancel = undefined
			void this.prune(generation)
		})
	}

	/** Prunes the instances due, then waits for the next one; a failure waits for the next arm. */
	private async prune(generation: number): Promise<void> {
		const { stateDirectory, clock, signal, logger } = this.deps
		try {
			for (const { store, partition, prunableAt } of stateDirectory.unleased()) {
				if (generation !== this.generation) {
					return
				}
				if (prunableAt <= clock.now()) await stateDirectory.prune(store, partition, signal)
			}
		} catch (error) {
			logger.warn('dropping unowned state failed: its next owner rebuilds it', { error: errorMessage(error) })
			return
		}
		if (generation === this.generation) {
			this.schedule(generation)
		}
	}
}
