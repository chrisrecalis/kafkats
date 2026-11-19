/**
 * Partition provider abstraction for unified assignment handling
 *
 * Abstracts the source of partition assignments (manual vs consumer group)
 * so the Consumer can use a single code path for the fetch loop.
 */

import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import type { Logger } from '@/logger.js'
import type { ConsumerGroup, JoinResult } from './consumer-group.js'
import type { OffsetManager } from './offset-manager.js'
import type {
	AssignedPartition,
	CommittedOffset,
	ManualAssignment,
	OffsetResetEvent,
	OffsetResetPolicy,
	TopicPartition,
} from './types.js'
import { diagnostics, trace, type RebalanceDiagnostics } from '@/diagnostics.js'
import { retry } from '@/utils/retry.js'
import { sleep } from '@/utils/sleep.js'
import { tpKey } from '@/utils/topic-partition.js'
import {
	CoordinatorNotAvailableError,
	NotCoordinatorError,
	TimeoutError,
	isConnectionFailure,
} from '@/client/errors.js'
import { ReconnectionStrategy } from '@/utils/reconnection-strategy.js'

/** Minimum spacing between 'error' events reported while waiting for an unreachable cluster. */
const UNREACHABLE_REPORT_INTERVAL_MS = 5000

export interface PartitionProviderCallbacks {
	/** The group asked for a rebalance; a rebalance step follows once the handlers returned. */
	onRebalancePending: () => void
	// Awaited at the start of the rebalance protocol so transactional consumers can commit
	// in-flight work before partitions are revoked.
	onRebalance: () => Promise<void>
	onPartitionsAssigned: (partitions: AssignedPartition[]) => Promise<void>
	onPartitionsRevoked: (partitions: TopicPartition[]) => Promise<void>
	/** Called synchronously when the session is lost; the next rejoin waits for the returned promise. */
	onPartitionsLost: (partitions: TopicPartition[]) => Promise<void>
	/** A rebalance step finished, and no other rebalance was requested meanwhile. */
	onRebalanceComplete: () => void
	/** A partition without a committed offset starts where its reset policy put it. */
	onOffsetReset: (reset: OffsetResetEvent) => void
	onError: (error: Error) => void
}

export interface PartitionProvider {
	start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]>
	/** Leave/clean up; `budgetMs` bounds best-effort network work (LeaveGroup). */
	stop(budgetMs?: number): Promise<void>
	/** Abandon requests parked at an unresponsive broker (shutdown deadline exceeded). */
	interrupt?(): void
	/** Whether a rebalance step is waiting to be run (see checkAndHandleRebalance). */
	hasPendingRebalance(): boolean
	/** Whether a rebalance was requested and the step serving it has not finished yet. */
	isRebalancing(): boolean
	/**
	 * Check for and handle any pending rebalance synchronously.
	 * Called by the poll loop before processing batches to ensure
	 * rebalance happens between poll iterations, not during handler execution.
	 */
	checkAndHandleRebalance(): Promise<void>
}

/**
 * Where each partition starts: its given offset, else its committed offset, else where its reset policy puts it
 * (reported to `onReset`). A partition whose policy is 'none' and that has no committed offset fails it with
 * NoOffsetForPartitionError.
 */
async function resolveAssignment(
	offsetManager: OffsetManager,
	partitions: readonly TopicPartition[],
	committedOffsets: ReadonlyMap<string, CommittedOffset>,
	resetPolicy: OffsetResetPolicy,
	onReset: (reset: OffsetResetEvent) => void,
	givenOffsets?: ReadonlyMap<string, bigint>
): Promise<AssignedPartition[]> {
	const assigned: AssignedPartition[] = []
	for (const { topic, partition } of partitions) {
		const key = tpKey(topic, partition)
		const committed = committedOffsets.get(key) ?? null
		let offset = givenOffsets?.get(key) ?? committed?.offset
		if (offset === undefined) {
			const policy = typeof resetPolicy === 'function' ? resetPolicy(topic, partition) : resetPolicy
			offset = await offsetManager.resolveStartingOffset(topic, partition, policy, committedOffsets)
			onReset({ topic, partition, from: null, to: offset, reason: 'no-commit' })
		}
		assigned.push({ topic, partition, offset, committed })
	}
	return assigned
}

export interface ManualPartitionProviderConfig {
	assignment: ManualAssignment[]
	cluster: Cluster
	groupId: string
	offsetManager: OffsetManager
	autoOffsetReset: OffsetResetPolicy
	signal?: AbortSignal
}

export class ManualPartitionProvider implements PartitionProvider {
	private readonly assignment: ManualAssignment[]
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly offsetManager: OffsetManager
	private readonly autoOffsetReset: OffsetResetPolicy
	private readonly signal?: AbortSignal

	constructor(config: ManualPartitionProviderConfig) {
		this.assignment = config.assignment
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.offsetManager = config.offsetManager
		this.autoOffsetReset = config.autoOffsetReset
		this.signal = config.signal
	}

	async start(_topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		const coordinator = await this.getCoordinatorWithRetry()
		this.offsetManager.updateGroupState('', -1, coordinator)

		const partitions: TopicPartition[] = this.assignment.map(tp => ({
			topic: tp.topic,
			partition: tp.partition,
		}))
		const givenOffsets = new Map<string, bigint>()
		for (const tp of this.assignment) {
			if (tp.offset !== undefined) givenOffsets.set(tpKey(tp.topic, tp.partition), tp.offset)
		}
		const committedOffsets = await this.offsetManager.fetchCommittedOffsets(partitions)
		await callbacks.onPartitionsAssigned(
			await resolveAssignment(
				this.offsetManager,
				partitions,
				committedOffsets,
				this.autoOffsetReset,
				callbacks.onOffsetReset,
				givenOffsets
			)
		)

		return partitions
	}

	async stop(): Promise<void> {}

	hasPendingRebalance(): boolean {
		return false
	}

	isRebalancing(): boolean {
		return false
	}

	async checkAndHandleRebalance(): Promise<void> {
		// Manual assignment doesn't do rebalancing
	}

	private async getCoordinatorWithRetry(): Promise<Broker> {
		return await retry(() => this.cluster.getCoordinator('GROUP', this.groupId), {
			maxAttempts: 10,
			initialDelayMs: 100,
			maxDelayMs: 1_000,
			multiplier: 2,
			jitter: 0,
			signal: this.signal,
			resolveOnAbort: true,
			shouldRetry: error => {
				if (this.signal?.aborted) {
					return false
				}
				return error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError
			},
		})
	}
}

export interface GroupPartitionProviderConfig {
	consumerGroup: ConsumerGroup
	cluster: Cluster
	groupId: string
	autoOffsetReset: OffsetResetPolicy
	offsetManager: OffsetManager
	logger: Logger
	signal?: AbortSignal
	isRunning: () => boolean
}

export class GroupPartitionProvider implements PartitionProvider {
	private readonly consumerGroup: ConsumerGroup
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly autoOffsetReset: OffsetResetPolicy
	private readonly offsetManager: OffsetManager
	private readonly logger: Logger
	private readonly signal?: AbortSignal
	private readonly isRunning: () => boolean

	private callbacks: PartitionProviderCallbacks | null = null
	private topics: string[] = []
	/** A rebalance was requested and the run loop has not started its step yet. */
	private rebalanceRequested = false
	/** From the first request of a rebalance until the step serving it finished. */
	private rebalancing = false
	/** The loss hooks invoked since the last rejoin: the next rejoin waits for them. */
	private readonly losses: Promise<void>[] = []
	private lastUnreachableReportAt = 0

	constructor(config: GroupPartitionProviderConfig) {
		this.consumerGroup = config.consumerGroup
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.autoOffsetReset = config.autoOffsetReset
		this.offsetManager = config.offsetManager
		this.logger = config.logger
		this.signal = config.signal
		this.isRunning = config.isRunning
	}

	async start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		this.callbacks = callbacks
		this.topics = topics

		this.consumerGroup.on('rebalance', () => this.onRebalanceEvent())
		this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
			const handled = callbacks.onPartitionsLost(partitions)
			// Awaited, and its failure raised, by the next rejoin.
			handled.catch(() => {})
			this.losses.push(handled)
		})
		this.consumerGroup.on('error', (error: Error) => callbacks.onError(error))

		const joinResult = await this.consumerGroup.join(topics)
		await this.updateGroupState()

		await callbacks.onPartitionsAssigned(await this.resolveOffsets(joinResult.assignment))

		return joinResult.assignment
	}

	async stop(budgetMs?: number): Promise<void> {
		await this.consumerGroup.stop(budgetMs)
	}

	interrupt(): void {
		this.consumerGroup.interrupt()
	}

	/**
	 * Run a step of the rebalance protocol, retrying with bounded exponential backoff while the
	 * cluster is unreachable (e.g. the broker is restarting). Dying here would strand the consumer
	 * outside the group; instead it keeps trying until the cluster is back or the consumer stops.
	 * Protocol-level errors are not retried, nor others unless `alsoRetried` says so. Failures are surfaced as
	 * throttled 'error' events.
	 */
	private async whileReachable<T>(
		step: () => Promise<T>,
		alsoRetried: (error: unknown) => boolean = () => false
	): Promise<T> {
		const backoff = new ReconnectionStrategy({ initialDelayMs: 100, maxDelayMs: 5000, jitter: 0.2 })
		for (;;) {
			try {
				return await step()
			} catch (error) {
				if (!(isConnectionFailure(error) || alsoRetried(error)) || this.signal?.aborted || !this.isRunning()) {
					throw error
				}
				const now = Date.now()
				if (now - this.lastUnreachableReportAt >= UNREACHABLE_REPORT_INTERVAL_MS) {
					this.lastUnreachableReportAt = now
					this.logger.warn('cluster unreachable during rebalance, retrying', {
						error: (error as Error).message,
					})
					this.callbacks?.onError(error as Error)
				}
				await sleep(backoff.nextDelay(), { signal: this.signal, resolveOnAbort: true })
				backoff.recordFailure()
			}
		}
	}

	private onRebalanceEvent(): void {
		this.logger.debug('rebalance event received')
		this.rebalanceRequested = true
		if (!this.rebalancing) {
			this.rebalancing = true
			this.callbacks?.onRebalancePending()
		}
	}

	hasPendingRebalance(): boolean {
		return this.rebalanceRequested
	}

	isRebalancing(): boolean {
		return this.rebalancing
	}

	async checkAndHandleRebalance(): Promise<void> {
		const callbacks = this.callbacks
		if (!this.rebalanceRequested || !this.isRunning() || !callbacks) {
			return
		}

		this.rebalanceRequested = false
		await trace(
			diagnostics.rebalance,
			(): RebalanceDiagnostics => ({ clientId: this.cluster.clientId, groupId: this.groupId }),
			async rebalance => {
				await this.rebalance(callbacks)
				if (rebalance) rebalance.assignment = this.consumerGroup.currentAssignment
			}
		)
		// A rebalance requested after this one's join is a new one: its own step completes it.
		if (this.rebalanceRequested || !this.isRunning()) return
		this.rebalancing = false
		callbacks.onRebalanceComplete()
	}

	/** Rejoin the group, once the loss hooks invoked so far have resolved. */
	private async rejoin(revokedPartitions?: TopicPartition[]): Promise<JoinResult> {
		await Promise.all(this.losses.splice(0))
		const result = await this.whileReachable(() => this.consumerGroup.rejoin(this.topics, revokedPartitions))
		// What asked for a rebalance until the group answered (e.g. heartbeats while partitions were revoked) asked
		// for the rebalance this join took part in.
		this.rebalanceRequested = false
		return result
	}

	/** Go through one rebalance: let the application finish, give up partitions, rejoin, take the new assignment. */
	private async rebalance(callbacks: NonNullable<GroupPartitionProvider['callbacks']>): Promise<void> {
		try {
			// Awaited so transactional consumers can commit in-flight work before partitions move.
			await callbacks.onRebalance()

			const previousAssignment = this.consumerGroup.currentAssignment
			const wasEager = this.consumerGroup.currentRebalanceProtocol === 'eager'

			// Eager protocol: revoke ALL partitions (which commits their final offsets)
			// BEFORE rejoining. The commit runs under the OLD generation, which the broker
			// accepts until the member rejoins — committing after JoinGroup would let the
			// new owner fetch a stale committed offset and reprocess records.
			if (wasEager && previousAssignment.length > 0) {
				await callbacks.onPartitionsRevoked(previousAssignment)
			}

			const rejoinResult = await this.rejoin()
			if (!this.isRunning()) return

			await this.updateGroupState()

			if (rejoinResult.protocol === 'eager') {
				// If the previous generation was eager everything was already revoked
				// pre-join; only a cooperative→eager protocol switch still needs the revoke.
				await this.handleEagerRebalance(wasEager ? [] : previousAssignment, rejoinResult.assignment)
			} else {
				// If the previous generation was eager, all partitions were revoked pre-join,
				// so the full final assignment (not just the added delta) must be assigned.
				await this.handleCooperativeRebalance(rejoinResult, wasEager)
			}
		} catch (error) {
			// Stopping while the cluster was unreachable: nothing left to rebalance.
			if (!this.isRunning() && isConnectionFailure(error)) {
				return
			}
			const err = error instanceof Error ? error : new Error(String(error))
			this.logger.error('rebalance handler failed', { error: err.message })
			// Stop the group: if the rebalance failed (e.g. EOS commit in onRebalance),
			// continuing the fetch loop on the stale assignment would be unsafe. Rethrow so
			// the poll loop that drives checkAndHandleRebalance exits instead of fetching
			// partitions this consumer no longer owns after leaving the group.
			void this.consumerGroup.stop().catch(() => {})
			throw err
		}
	}

	private async handleEagerRebalance(
		previousAssignment: TopicPartition[],
		assignment: TopicPartition[]
	): Promise<void> {
		this.logger.debug('eager rebalance: assigning new generation', {
			pendingRevokeCount: previousAssignment.length,
		})

		// Normally empty: the eager protocol revokes before the rejoin. Non-empty only
		// on a cooperative→eager protocol switch, where the revoke could not happen early.
		if (previousAssignment.length > 0) {
			await this.callbacks!.onPartitionsRevoked(previousAssignment)
		}

		await this.callbacks!.onPartitionsAssigned(await this.resolveOffsets(assignment))
	}

	/**
	 * @param assignFullAssignment - assign the full final assignment instead of only the
	 * added delta. Used when the previous generation ran the eager protocol (all
	 * partitions were revoked before the rejoin), so kept partitions must be re-added.
	 */
	private async handleCooperativeRebalance(rejoinResult: JoinResult, assignFullAssignment = false): Promise<void> {
		if (rejoinResult.protocol === 'cooperative') {
			this.logger.debug('cooperative rebalance: phase 1', {
				revokedCount: rejoinResult.revoked.length,
				keptCount: rejoinResult.kept.length,
			})

			if (rejoinResult.revoked.length > 0) {
				await this.callbacks!.onPartitionsRevoked(rejoinResult.revoked)
			}

			// Phase 2: rejoin loop until stable
			let rejoinAttempt = 0
			const MAX_REJOIN_ATTEMPTS = 5
			const REJOIN_BACKOFF_MS = 500

			while (rejoinResult.needsRejoin) {
				rejoinAttempt++
				if (rejoinAttempt > MAX_REJOIN_ATTEMPTS) {
					throw new Error(`Max cooperative rejoin attempts exceeded (${MAX_REJOIN_ATTEMPTS})`)
				}

				const backoffMs = Math.min(REJOIN_BACKOFF_MS * rejoinAttempt, 3000)
				await sleep(backoffMs, { signal: this.signal, resolveOnAbort: true })
				if (!this.isRunning()) return

				rejoinResult = await this.rejoin(rejoinResult.revoked)
				await this.updateGroupState()

				if (rejoinResult.needsRejoin && rejoinResult.revoked.length > 0) {
					await this.callbacks!.onPartitionsRevoked(rejoinResult.revoked)
				}
			}

			this.logger.debug('cooperative rebalance: complete', {
				assignmentCount: rejoinResult.assignment.length,
				addedCount: rejoinResult.added.length,
			})

			const toAssign = assignFullAssignment ? rejoinResult.assignment : rejoinResult.added
			if (toAssign.length > 0) {
				await this.callbacks!.onPartitionsAssigned(await this.resolveOffsets(toAssign))
			}
		}
	}

	private async updateGroupState(): Promise<void> {
		const coordinator = await this.whileReachable(() => this.cluster.getCoordinator('GROUP', this.groupId))
		this.offsetManager.updateGroupState(
			this.consumerGroup.currentMemberId,
			this.consumerGroup.currentGenerationId,
			coordinator
		)
	}

	private async resolveOffsets(partitions: TopicPartition[]): Promise<AssignedPartition[]> {
		// Each fetch is bounded by defaultApiTimeoutMs. One that ran out of time (a coordinator that does not answer,
		// or answers it is loading) is tried again like one of an unreachable cluster, as Java's consumer does at its
		// next poll: it does not fail the rebalance, which would leave the group.
		const committedOffsets = await this.whileReachable(
			() => this.offsetManager.fetchCommittedOffsets(partitions),
			error => error instanceof TimeoutError
		)
		return resolveAssignment(
			this.offsetManager,
			partitions,
			committedOffsets,
			this.autoOffsetReset,
			this.callbacks!.onOffsetReset
		)
	}
}
