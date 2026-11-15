import type { ConsumerConfig, ProducerConfig, RunBatchOptions } from '@kafkats/client'
import type { FlowConfig } from '@/types.js'
import type { OffsetReset } from '@/runtime/topology.js'

/** Default transaction timeout for exactly_once producers (same as Kafka Streams). */
const DEFAULT_TRANSACTION_TIMEOUT_MS = 10_000

/**
 * Default time to wait for a worker to join the group. Joining can legitimately take as long as
 * the session timeout of a member that crashed (the group waits for it before rebalancing).
 */
const DEFAULT_START_TIMEOUT_MS = 120_000

/** Which of the app's `numStreamThreads` workers a client is configured for. */
export type WorkerSlot = { id: number; threadCount: number }

/** A user-supplied id made unique per worker; kept as is when the app runs a single worker. */
function perWorker(base: string, slot: WorkerSlot): string {
	return slot.threadCount > 1 ? `${base}-w${slot.id}` : base
}

export function workerStartTimeoutMs(config: FlowConfig): number {
	return config.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS
}

/**
 * Producer of one worker. Under exactly_once it is transactional, with a transactional id unique
 * to the worker: derived from the user's id, or from the application and process id.
 */
export function workerProducerConfig(
	config: FlowConfig,
	eosEnabled: boolean,
	processId: string,
	slot: WorkerSlot
): ProducerConfig | undefined {
	if (!eosEnabled) {
		return config.producer
	}

	const base = config.producer ?? {}
	const transactionalId = base.transactionalId
		? perWorker(base.transactionalId, slot)
		: `${config.applicationId}-${processId}-w${slot.id}`
	return {
		...base,
		transactionalId,
		idempotent: true,
		retries: base.retries ?? 10,
		// How long a crashed instance's open transaction can block restoration and
		// read_committed readers of its output before the broker aborts it.
		transactionTimeoutMs: base.transactionTimeoutMs ?? DEFAULT_TRANSACTION_TIMEOUT_MS,
	}
}

/**
 * Consumer of one worker. `commitOpenTransaction` commits the worker's open exactly_once
 * transaction, if any; it runs before every rebalance.
 */
export function workerConsumerConfig(
	config: FlowConfig,
	eosEnabled: boolean,
	offsetReset: OffsetReset | undefined,
	slot: WorkerSlot,
	commitOpenTransaction: () => Promise<void>
): ConsumerConfig {
	const userHook = config.consumer?.onBeforeRebalance
	const groupInstanceId = config.consumer?.groupInstanceId
	return {
		groupId: config.applicationId,
		...config.consumer,
		autoOffsetReset: offsetReset,
		groupInstanceId: groupInstanceId ? perWorker(groupInstanceId, slot) : undefined,
		isolationLevel: eosEnabled ? 'read_committed' : config.consumer?.isolationLevel,
		// State is partitioned by source partition, so partition N of every co-partitioned
		// topic must be owned by the same worker. The range assignor guarantees that;
		// the sticky assignors balance topic-partitions individually and do not.
		partitionAssignmentStrategy: 'range',
		// EOS: commit the in-flight transaction inside the awaited rebalance window
		// so offsets land before the group rejoins. Compose with any user-supplied
		// hook (user runs first; their hook can prep state before the EOS commit).
		// A commit failure rejects this promise → the consumer halts before processing
		// on stale state and the worker restarts from the last committed offsets.
		onBeforeRebalance:
			eosEnabled || userHook
				? async () => {
						if (userHook) await userHook()
						if (!eosEnabled) return
						await commitOpenTransaction()
					}
				: undefined,
	}
}

/** Under exactly_once, offsets are committed by the transaction, one record of a partition at a time. */
export function workerRunOptions(config: FlowConfig, eosEnabled: boolean): RunBatchOptions | undefined {
	if (!eosEnabled) {
		return config.consumerRun
	}

	return {
		...config.consumerRun,
		autoCommit: false,
		commitOffsets: false,
		partitionConcurrency: 1,
	}
}
