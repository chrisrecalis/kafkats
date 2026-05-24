/**
 * PartitionTracker - Central source of truth for partition ownership and processing state
 *
 * This class bridges ConsumerGroup and FetchManager by tracking:
 * 1. Which partitions are assigned (owned) by this consumer
 * 2. Which partitions have in-flight handler processing
 *
 * The key insight is that revoking a partition requires waiting for any
 * in-flight handler to complete before it's safe to release the partition
 * (commit offsets, let another consumer take over).
 *
 * Usage:
 * - ConsumerGroup calls assign() and revoke()
 * - FetchManager calls isAssigned() to know what to fetch
 * - Consumer calls startProcessing() and endProcessing() around handlers
 * - OffsetManager can use isAssigned() to reject stale markConsumed calls
 */

import { tpKey } from '@/utils/topic-partition.js'
import type { TopicPartition } from './types.js'
import { noopLogger, type Logger } from '@/logger.js'

/**
 * A Promise that can be resolved/rejected from outside
 */
class Deferred<T = void> {
	readonly promise: Promise<T>
	resolve!: (value: T | PromiseLike<T>) => void
	reject!: (reason?: unknown) => void
	private _resolved = false

	constructor() {
		this.promise = new Promise<T>((resolve, reject) => {
			this.resolve = (value: T | PromiseLike<T>) => {
				if (!this._resolved) {
					this._resolved = true
					resolve(value)
				}
			}
			this.reject = (reason?: unknown) => {
				if (!this._resolved) {
					this._resolved = true
					let error: Error
					if (reason instanceof Error) {
						error = reason
					} else if (typeof reason === 'string') {
						error = new Error(reason)
					} else {
						error = new Error('rejected')
					}
					reject(error)
				}
			}
		})
	}

	get isResolved(): boolean {
		return this._resolved
	}
}

export interface PartitionTrackerConfig {
	logger?: Logger
	/**
	 * Maximum time to wait for in-flight handlers during revoke.
	 * If exceeded, revoke completes anyway (broker will kick us eventually).
	 * Default: no timeout (wait indefinitely)
	 */
	revokeTimeoutMs?: number
}

export class PartitionTracker {
	private readonly logger: Logger

	/**
	 * Partitions currently assigned to this consumer.
	 * Key: "topic:partition"
	 */
	private readonly assigned = new Set<string>()

	/**
	 * Partitions with in-flight handler processing, ref-counted: `count` is the number of
	 * concurrent batches being processed for the partition, and `deferred` resolves only
	 * once the count drops back to zero. Ref-counting is required because, with
	 * partitionConcurrency > 1, two batches for the same partition can be in flight at once;
	 * a single shared Deferred would resolve on the FIRST endProcessing and let revoke()
	 * complete while the second batch is still running.
	 * Key: "topic:partition"
	 */
	private readonly inFlight = new Map<string, { deferred: Deferred<void>; count: number }>()

	/**
	 * Partitions that are being revoked (waiting for in-flight to complete).
	 * Used to prevent race conditions where a partition could be
	 * re-assigned while still waiting for revoke.
	 * Key: "topic:partition"
	 */
	private readonly revoking = new Set<string>()

	private readonly revokeTimeoutMs?: number

	constructor(config: PartitionTrackerConfig = {}) {
		this.logger = config.logger?.child({ component: 'partition-tracker' }) ?? noopLogger
		this.revokeTimeoutMs = config.revokeTimeoutMs
	}

	/**
	 * Mark partitions as assigned (owned by this consumer).
	 * Called by ConsumerGroup after rebalance assigns new partitions.
	 *
	 * @param partitions - The newly assigned partitions
	 */
	assign(partitions: TopicPartition[]): void {
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)

			// Don't assign if currently being revoked (race condition protection)
			if (this.revoking.has(key)) {
				this.logger.warn('attempted to assign partition that is being revoked', {
					topic: tp.topic,
					partition: tp.partition,
				})
				continue
			}

			this.assigned.add(key)
		}

		this.logger.debug('partitions assigned', {
			count: partitions.length,
			total: this.assigned.size,
		})
	}

	/**
	 * Revoke partitions. Immediately marks them as not-assigned,
	 * then waits for any in-flight processing to complete.
	 *
	 * This is the key synchronization point: ConsumerGroup cannot
	 * commit offsets or rejoin until this Promise resolves.
	 *
	 * @param partitions - The partitions to revoke
	 * @returns Promise that resolves when all in-flight handlers complete
	 */
	async revoke(partitions: TopicPartition[]): Promise<void> {
		if (partitions.length === 0) {
			return
		}

		const waitPromises: Array<{ key: string; promise: Promise<void> }> = []

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)

			// Mark as revoking (prevents re-assignment during revoke)
			this.revoking.add(key)

			// Remove from assigned immediately
			// FetchManager will stop fetching this partition
			this.assigned.delete(key)

			// If there's in-flight work, we need to wait for it (until ALL concurrent
			// batches for this partition have completed, i.e. the ref count reaches zero).
			const entry = this.inFlight.get(key)
			if (entry && !entry.deferred.isResolved) {
				waitPromises.push({ key, promise: entry.deferred.promise })
			}
		}

		if (waitPromises.length > 0) {
			this.logger.debug('waiting for in-flight handlers before revoke completes', {
				partitions: waitPromises.map(w => w.key),
			})

			// Wait for all in-flight handlers, with optional timeout
			if (this.revokeTimeoutMs) {
				const timeout = new Promise<void>((_, reject) => {
					setTimeout(() => {
						reject(new Error(`Revoke timeout: handlers did not complete within ${this.revokeTimeoutMs}ms`))
					}, this.revokeTimeoutMs)
				})

				try {
					await Promise.race([Promise.all(waitPromises.map(w => w.promise)), timeout])
				} catch (error) {
					this.logger.warn('revoke timeout exceeded, proceeding anyway', {
						error: error instanceof Error ? error.message : String(error),
						pendingPartitions: waitPromises.filter(w => this.inFlight.has(w.key)).map(w => w.key),
					})
					// Continue with revoke even on timeout - broker will handle it
				}
			} else {
				await Promise.all(waitPromises.map(w => w.promise))
			}
		}

		// Clear revoking state
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			this.revoking.delete(key)
			// Also clean up any lingering in-flight state
			this.inFlight.delete(key)
		}

		this.logger.debug('partitions revoked', {
			count: partitions.length,
			remaining: this.assigned.size,
		})
	}

	/**
	 * Check if a partition is currently assigned to this consumer.
	 * FetchManager uses this to know which partitions to fetch from.
	 * OffsetManager can use this to reject markConsumed for revoked partitions.
	 *
	 * @returns true if the partition is assigned and not being revoked
	 */
	isAssigned(topic: string, partition: number): boolean {
		const key = tpKey(topic, partition)
		return this.assigned.has(key) && !this.revoking.has(key)
	}

	/**
	 * Get all currently assigned partitions.
	 * Excludes partitions that are being revoked.
	 */
	getAssigned(): TopicPartition[] {
		const result: TopicPartition[] = []
		for (const key of this.assigned) {
			if (!this.revoking.has(key)) {
				const [topic, partitionStr] = key.split(':')
				result.push({ topic: topic!, partition: parseInt(partitionStr!, 10) })
			}
		}
		return result
	}

	/**
	 * Mark that a handler has started processing a batch from this partition.
	 * Called by Consumer before invoking the user's handler.
	 *
	 * @returns true if processing was started, false if partition is not assigned
	 */
	startProcessing(topic: string, partition: number): boolean {
		const key = tpKey(topic, partition)

		// Don't start processing if partition is not assigned or being revoked
		if (!this.assigned.has(key) || this.revoking.has(key)) {
			this.logger.debug('skipping startProcessing for non-assigned partition', {
				topic,
				partition,
				assigned: this.assigned.has(key),
				revoking: this.revoking.has(key),
			})
			return false
		}

		// Ref-count concurrent batches for this partition; the Deferred for the first
		// batch is shared and only resolves once the last batch ends.
		let entry = this.inFlight.get(key)
		if (!entry) {
			entry = { deferred: new Deferred(), count: 0 }
			this.inFlight.set(key, entry)
		}
		entry.count++
		this.logger.debug('partition processing started', { topic, partition, inFlight: entry.count })

		return true
	}

	/**
	 * Mark that a handler has finished processing a batch from this partition.
	 * Called by Consumer after the user's handler completes (success or error).
	 *
	 * This resolves any pending revoke() wait for this partition.
	 */
	endProcessing(topic: string, partition: number): void {
		const key = tpKey(topic, partition)
		const entry = this.inFlight.get(key)

		if (entry) {
			entry.count--
			// Resolve the revoke wait only once the LAST concurrent batch for this
			// partition finishes. `<= 0` (not `=== 0`) tolerates the unpaired force-decrement
			// from onPartitionsLost (consumer.ts), which calls endProcessing without a
			// matching startProcessing.
			if (entry.count <= 0) {
				entry.deferred.resolve()
				this.inFlight.delete(key)
				this.logger.debug('partition processing ended', { topic, partition })
			} else {
				this.logger.debug('partition batch ended, others still in flight', {
					topic,
					partition,
					inFlight: entry.count,
				})
			}
		}
	}

	/**
	 * Check if a partition has in-flight processing.
	 */
	isProcessing(topic: string, partition: number): boolean {
		const key = tpKey(topic, partition)
		const entry = this.inFlight.get(key)
		return entry !== undefined && !entry.deferred.isResolved
	}

	/**
	 * Clear all state. Called when consumer stops.
	 * Resolves any pending waits so they don't hang.
	 */
	clear(): void {
		// Resolve any pending waits so revoke() calls don't hang forever
		for (const entry of this.inFlight.values()) {
			entry.deferred.resolve()
		}

		this.assigned.clear()
		this.inFlight.clear()
		this.revoking.clear()

		this.logger.debug('partition tracker cleared')
	}

	/**
	 * Get current state for debugging/monitoring.
	 */
	getState(): {
		assigned: string[]
		inFlight: string[]
		revoking: string[]
	} {
		return {
			assigned: Array.from(this.assigned),
			inFlight: Array.from(this.inFlight.keys()),
			revoking: Array.from(this.revoking),
		}
	}
}
