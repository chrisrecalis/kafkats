/**
 * The prune of unowned in-memory state (4.10.4, runtime§6.29, CR6). Once every task of a run is RUNNING, the store
 * instances no task leased (another instance of the application owns their partitions now) are dropped after 30 s.
 * Outside the scheduler's exclusive section, like Kafka Streams' state directory cleaner beside its stream threads: a
 * prune clears a whole store, which must not hold up slices, commits and rebalance hooks. That is safe because
 * StateDirectory.prune re-checks the lease and invalidates the checkpoint before it clears, and a lease made meanwhile
 * waits for the prune and gets a new, empty instance. A rebalance, a revival, a recovery and close disarm it, also
 * while it runs: no further instance is pruned then. Durable state is kept, like Kafka Streams' task directories.
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

	constructor(private readonly deps: PruneTimerDeps) {}

	/** Every task is RUNNING: prunes in 30 s unless disarmed meanwhile; nothing to prune, no timer. */
	arm(): void {
		const { stateDirectory, clock } = this.deps
		if (this.cancel !== undefined || stateDirectory.unleased().length === 0) {
			return
		}
		const { generation } = this
		this.cancel = clock.setTimer(clock.now() + PRUNE_DELAY_MS, () => {
			this.cancel = undefined
			void this.prune(generation)
		})
	}

	disarm(): void {
		this.generation++
		this.cancel?.()
		this.cancel = undefined
	}

	private async prune(generation: number): Promise<void> {
		const { stateDirectory, signal, logger } = this.deps
		try {
			for (const { store, partition } of stateDirectory.unleased()) {
				if (generation !== this.generation) {
					return
				}
				await stateDirectory.prune(store, partition, signal)
			}
		} catch (error) {
			logger.warn('dropping unowned state failed: its next owner rebuilds it', { error: errorMessage(error) })
		}
	}
}
