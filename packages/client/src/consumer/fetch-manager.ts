/**
 * Fetch manager for consumer
 *
 * Handles:
 * - Fetching records from assigned partitions
 * - Managing partition concurrency
 * - Decoding record batches
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	decodeRecordBatchFrom,
	decodeRecordBatchFromSync,
	isControlBatch,
	isTransactional,
} from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import type { TopicPartition, AutoOffsetReset, IsolationLevel, ConsumerTraceFn } from './types.js'
import type { OffsetManager } from './offset-manager.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey } from '@/utils/topic-partition.js'
import { sleep } from '@/utils/sleep.js'

function filterRecordsFromOffset(records: DecodedRecord[], minOffset: bigint): DecodedRecord[] {
	if (records.length === 0) {
		return records
	}

	if (records[0]!.offset >= minOffset) {
		return records
	}
	let start = 0
	while (start < records.length && records[start]!.offset < minOffset) {
		start++
	}

	return start === 0 ? records : records.slice(start)
}

/**
 * Simple semaphore for concurrency control
 */
class Semaphore {
	private available: number
	private readonly waiting: Array<{ resolve: () => void }> = []

	constructor(capacity: number) {
		this.available = capacity
	}

	/**
	 * Acquire a semaphore slot
	 * @param signal - Optional abort signal to cancel waiting
	 * @returns true if acquired, false if aborted while waiting
	 */
	async acquire(signal?: AbortSignal): Promise<boolean> {
		if (this.available > 0) {
			this.available--
			return true
		}

		return new Promise<boolean>(resolve => {
			let onAbort: (() => void) | null = null

			const cleanup = () => {
				if (signal && onAbort) {
					signal.removeEventListener('abort', onAbort)
				}
				onAbort = null
			}

			const entry = {
				resolve: () => {
					cleanup()
					resolve(true)
				},
			}
			this.waiting.push(entry)

			// If signal is provided, listen for abort
			if (signal) {
				onAbort = () => {
					// Remove from waiting queue
					const idx = this.waiting.indexOf(entry)
					if (idx !== -1) {
						this.waiting.splice(idx, 1)
					}
					cleanup()
					resolve(false) // Return false to indicate aborted, don't reject
				}

				if (signal.aborted) {
					// Already aborted
					const idx = this.waiting.indexOf(entry)
					if (idx !== -1) {
						this.waiting.splice(idx, 1)
					}
					cleanup()
					resolve(false)
					return
				}

				signal.addEventListener('abort', onAbort)
			}
		})
	}

	release(): void {
		const next = this.waiting.shift()
		if (next) {
			next.resolve()
		} else {
			this.available++
		}
	}
}

/**
 * Per-partition fetch state
 */
interface PartitionState {
	topic: string
	partition: number
	offset: bigint
	paused: boolean
	abortController: AbortController // Per-partition abort signal for incremental control
	cachedLeader: Broker | null // Cached leader broker - invalidated on NotLeaderOrFollower
}

/**
 * Fetch manager configuration
 */
export interface FetchManagerConfig {
	maxBytesPerPartition: number
	minBytes: number
	maxWaitMs: number
	partitionConcurrency: number
	isolationLevel: IsolationLevel
	trace?: ConsumerTraceFn
}

/**
 * Default fetch manager configuration
 */
export const DEFAULT_FETCH_CONFIG: FetchManagerConfig = {
	maxBytesPerPartition: 1048576, // 1MB
	minBytes: 1,
	maxWaitMs: 5000,
	partitionConcurrency: 1,
	isolationLevel: 'read_committed',
}

/**
 * Fetch manager
 */
/**
 * Callback type for handling fetched records
 * Uses DecodedRecord directly to avoid intermediate Message object creation
 */
type FetchRecordsCallback = (topic: string, partition: number, records: DecodedRecord[]) => Promise<void>

type ActiveLoop = {
	abortController: AbortController
	promise: Promise<void>
}

/**
 * Fetch manager
 *
 * Uses broker-level fetch batching: groups all ready partitions by their leader broker
 * and issues one fetch request per broker containing all partitions.
 */
export class FetchManager {
	private readonly cluster: Cluster
	private readonly config: FetchManagerConfig
	private readonly partitionStates: Map<string, PartitionState> = new Map()
	private readonly activeLoops: Map<string, ActiveLoop> = new Map() // Track active fetch loops (legacy)
	private readonly semaphore: Semaphore
	private readonly logger: Logger
	private readonly trace?: ConsumerTraceFn
	private readonly autoOffsetReset?: AutoOffsetReset
	private readonly offsetManager?: OffsetManager
	private abortController: AbortController | null = null
	private onRecordsCallback: FetchRecordsCallback | null = null // Store callback for addPartitions
	private running = false
	private useBrokerBatching = true // Use broker-level batching (faster than per-partition loops)

	constructor(
		cluster: Cluster,
		configOrOffsetManager?: Partial<FetchManagerConfig> | OffsetManager,
		autoOffsetResetOrLogger?: AutoOffsetReset | Logger,
		configOrLogger?: Partial<FetchManagerConfig> | Logger,
		maybeLogger?: Logger
	) {
		this.cluster = cluster

		// Handle overloaded constructor signatures
		// Signature 1 (new): cluster, offsetManager, autoOffsetReset, config, logger
		// Signature 2 (old): cluster, config, logger
		if (
			configOrOffsetManager &&
			typeof configOrOffsetManager === 'object' &&
			'getEarliestOffset' in configOrOffsetManager
		) {
			// New signature
			this.offsetManager = configOrOffsetManager
			this.autoOffsetReset = autoOffsetResetOrLogger as AutoOffsetReset
			this.config = { ...DEFAULT_FETCH_CONFIG, ...(configOrLogger as Partial<FetchManagerConfig>) }
			this.logger = maybeLogger?.child({ component: 'fetch-manager' }) ?? noopLogger
			this.trace = this.config.trace
		} else {
			// Old signature (for backwards compatibility with tests)
			this.config = { ...DEFAULT_FETCH_CONFIG, ...(configOrOffsetManager as Partial<FetchManagerConfig>) }
			this.logger = (autoOffsetResetOrLogger as Logger)?.child?.({ component: 'fetch-manager' }) ?? noopLogger
			this.trace = this.config.trace
		}

		this.semaphore = new Semaphore(this.config.partitionConcurrency)
	}

	/**
	 * Set partitions to fetch from with their starting offsets
	 * This replaces all partitions (used for eager rebalance)
	 */
	setPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		this.logger.debug('setting partitions', {
			count: partitions.length,
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})
		// Abort all existing partition loops
		for (const state of this.partitionStates.values()) {
			state.abortController.abort()
		}
		this.partitionStates.clear()
		this.activeLoops.clear()

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
			}
			this.partitionStates.set(key, state)
		}

		// If we're already running, restart partition loops for the new assignment.
		if (!this.useBrokerBatching && this.running && this.onRecordsCallback) {
			for (const [key, state] of this.partitionStates.entries()) {
				const promise = this.fetchPartitionLoop(state, this.onRecordsCallback, state.abortController.signal)
				this.activeLoops.set(key, { abortController: state.abortController, promise })
			}
		}
	}

	/**
	 * Add partitions incrementally (used for cooperative rebalance)
	 * Starts fetch loops for newly added partitions if the manager is running
	 */
	addPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		if (partitions.length === 0) return

		this.logger.debug('adding partitions', {
			count: partitions.length,
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			// Skip if already exists
			if (this.partitionStates.has(key)) {
				this.logger.debug('partition already exists, skipping', { topic: tp.topic, partition: tp.partition })
				continue
			}

			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
			}
			this.partitionStates.set(key, state)

			// If running and we have a callback, start a fetch loop for this partition
			if (!this.useBrokerBatching && this.running && this.onRecordsCallback) {
				const promise = this.fetchPartitionLoop(state, this.onRecordsCallback, state.abortController.signal)
				this.activeLoops.set(key, { abortController: state.abortController, promise })
			}
		}
	}

	/**
	 * Remove partitions incrementally (used for cooperative rebalance)
	 * Aborts fetch loops for removed partitions
	 */
	removePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('removing partitions', {
			count: partitions.length,
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				// Abort the partition's fetch loop
				state.abortController.abort()
				this.partitionStates.delete(key)
				this.activeLoops.delete(key)
			}
		}
	}

	/**
	 * Pause partitions (stop fetching but keep state)
	 */
	pausePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('pausing partitions', {
			count: partitions.length,
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = true
			}
		}
	}

	/**
	 * Resume paused partitions
	 */
	resumePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('resuming partitions', {
			count: partitions.length,
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = false
			}
		}
	}

	/**
	 * Update offset for a partition (after consuming)
	 */
	updateOffset(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (state) {
			state.offset = offset + 1n // Next offset to fetch
		}
	}

	/**
	 * Start the fetch loop
	 *
	 * @param onRecords - Callback for each batch of records from a partition
	 */
	async start(onRecords: FetchRecordsCallback): Promise<void> {
		if (this.running) {
			return
		}

		this.logger.info('starting fetch manager', {
			partitionCount: this.partitionStates.size,
			concurrency: this.config.partitionConcurrency,
			useBrokerBatching: this.useBrokerBatching,
		})
		this.running = true
		this.abortController = new AbortController()
		this.onRecordsCallback = onRecords // Store for addPartitions

		if (this.useBrokerBatching) {
			// New: Single fetch loop with broker-level batching
			await this.brokerBatchedFetchLoop(onRecords)
		} else {
			// Legacy: Per-partition fetch loops
			for (const [key, state] of this.partitionStates.entries()) {
				const promise = this.fetchPartitionLoop(state, onRecords, state.abortController.signal)
				this.activeLoops.set(key, { abortController: state.abortController, promise })
			}

			while (this.running) {
				const loops = Array.from(this.activeLoops.values()).map(loop => loop.promise)
				if (loops.length === 0) {
					await sleep(100, { signal: this.abortController.signal }).catch(() => {})
					if (!this.running) break
					continue
				}
				await Promise.race(loops).catch(() => {})
			}

			await Promise.all(Array.from(this.activeLoops.values()).map(loop => loop.promise)).catch(() => {})
			this.activeLoops.clear()
		}

		this.onRecordsCallback = null
		this.logger.debug('fetch manager stopped')
	}

	/**
	 * Broker-batched fetch loop - groups partitions by broker and issues one fetch per broker
	 */
	private async brokerBatchedFetchLoop(onRecords: FetchRecordsCallback): Promise<void> {
		const signal = this.abortController!.signal

		while (this.running && !signal.aborted) {
			try {
				// Get all ready (non-paused) partitions
				const readyPartitions = Array.from(this.partitionStates.values()).filter(s => !s.paused)

				if (readyPartitions.length === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Group partitions by leader broker (using cached leaders when available)
				const partitionsByBroker = new Map<number, { broker: Broker; partitions: PartitionState[] }>()

				// Collect partitions needing leader lookup
				const needsLookup: PartitionState[] = []
				for (const state of readyPartitions) {
					if (state.cachedLeader) {
						const brokerId = state.cachedLeader.nodeId
						if (!partitionsByBroker.has(brokerId)) {
							partitionsByBroker.set(brokerId, { broker: state.cachedLeader, partitions: [] })
						}
						partitionsByBroker.get(brokerId)!.partitions.push(state)
					} else {
						needsLookup.push(state)
					}
				}

				// Parallel leader lookups for cache misses
				if (needsLookup.length > 0) {
					const lookups = await Promise.all(
						needsLookup.map(async state => {
							try {
								const leader = await this.cluster.getLeaderForPartition(state.topic, state.partition)
								state.cachedLeader = leader
								return { state, leader }
							} catch (error) {
								this.logger.error('failed to get leader', {
									topic: state.topic,
									partition: state.partition,
									error: (error as Error).message,
								})
								return null
							}
						})
					)

					for (const result of lookups) {
						if (!result) continue
						const brokerId = result.leader.nodeId
						if (!partitionsByBroker.has(brokerId)) {
							partitionsByBroker.set(brokerId, { broker: result.leader, partitions: [] })
						}
						partitionsByBroker.get(brokerId)!.partitions.push(result.state)
					}
				}

				if (partitionsByBroker.size === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Issue fetch requests to all brokers in parallel
				const fetchPromises = Array.from(partitionsByBroker.entries()).map(
					async ([brokerId, { broker, partitions }]) => {
						try {
							// Track fetch offsets used for this request per partition so we can filter
							// record batches that may start before the requested offset.
							const fetchOffsetByKey = new Map<string, bigint>()

							// Build topics array for this broker's partitions
							const topicsMap = new Map<string, Array<{ state: PartitionState }>>()
							for (const state of partitions) {
								if (!topicsMap.has(state.topic)) {
									topicsMap.set(state.topic, [])
								}
								topicsMap.get(state.topic)!.push({ state })
								fetchOffsetByKey.set(tpKey(state.topic, state.partition), state.offset)
							}

							const topics = Array.from(topicsMap.entries()).map(([topic, parts]) => ({
								topic,
								topicId: '00000000-0000-0000-0000-000000000000',
								partitions: parts.map(p => ({
									partitionIndex: p.state.partition,
									currentLeaderEpoch: -1,
									fetchOffset: p.state.offset,
									lastFetchedEpoch: -1,
									logStartOffset: -1n,
									partitionMaxBytes: this.config.maxBytesPerPartition,
								})),
							}))

							const fetchStart = performance.now()
							const response = await broker.fetch({
								replicaId: -1,
								maxWaitMs: this.config.maxWaitMs,
								minBytes: this.config.minBytes,
								maxBytes: this.config.maxBytesPerPartition * partitions.length,
								isolationLevel: this.config.isolationLevel === 'read_committed' ? 1 : 0,
								topics,
							})
							const fetchEnd = performance.now()
							if (this.trace) {
								this.trace({
									stage: 'fetch_request',
									durationMs: fetchEnd - fetchStart,
									brokerId,
									recordCount: partitions.length,
								})
							}

							const processPromises: Promise<void>[] = []

							// Process response for each partition
							for (const topicResponse of response.topics) {
								for (const partitionResponse of topicResponse.partitions) {
									const key = tpKey(topicResponse.topic, partitionResponse.partitionIndex)
									const state = this.partitionStates.get(key)
									if (!state) continue

									// Handle errors
									if (partitionResponse.errorCode !== ErrorCode.None) {
										await this.handleFetchError(state, partitionResponse.errorCode)
										continue
									}

									// Decode and deliver records
									if (partitionResponse.recordsData && partitionResponse.recordsData.length > 0) {
										const decodeStart = performance.now()
										const decodeResult = this.decodeRecords(
											state.topic,
											state.partition,
											partitionResponse.recordsData,
											partitionResponse.abortedTransactions,
											partitionResponse.lastStableOffset
										)
										// Handle both sync and async cases efficiently
										let records =
											decodeResult instanceof Promise ? await decodeResult : decodeResult
										const decodeEnd = performance.now()
										if (this.trace) {
											this.trace({
												stage: 'decode_records',
												durationMs: decodeEnd - decodeStart,
												recordCount: records.length,
												bytes: partitionResponse.recordsData?.length ?? 0,
												topic: state.topic,
												partition: state.partition,
											})
										}

										const fetchOffset = fetchOffsetByKey.get(key)
										if (fetchOffset !== undefined) {
											records = filterRecordsFromOffset(records, fetchOffset)
										}

										if (records.length > 0) {
											processPromises.push(
												(async () => {
													const acquired = await this.semaphore.acquire(signal)
													if (!acquired) return
													try {
														const handlerStart = performance.now()
														await onRecords(state.topic, state.partition, records)
														const handlerEnd = performance.now()
														if (this.trace) {
															this.trace({
																stage: 'handler',
																durationMs: handlerEnd - handlerStart,
																recordCount: records.length,
																topic: state.topic,
																partition: state.partition,
															})
														}
														state.offset = records[records.length - 1]!.offset + 1n
													} finally {
														this.semaphore.release()
													}
												})()
											)
										}
									}
								}
							}

							// Wait for all partition processing for this broker (respecting semaphore limits)
							if (processPromises.length > 0) {
								await Promise.all(processPromises)
							}
						} catch (error) {
							this.logger.error('broker fetch failed', { brokerId, error: (error as Error).message })
						}
					}
				)

				await Promise.all(fetchPromises)
			} catch (error) {
				if (signal.aborted) break
				this.logger.error('fetch loop error', { error: (error as Error).message })
				await sleep(1000, { signal }).catch(() => {})
			}
		}
	}

	/**
	 * Handle fetch error for a partition
	 */
	private async handleFetchError(state: PartitionState, errorCode: ErrorCode): Promise<void> {
		switch (errorCode) {
			case ErrorCode.OffsetOutOfRange:
				if (this.offsetManager && this.autoOffsetReset) {
					switch (this.autoOffsetReset) {
						case 'earliest':
							state.offset = await this.offsetManager.getEarliestOffset(state.topic, state.partition)
							break
						case 'latest':
							state.offset = await this.offsetManager.getLatestOffset(state.topic, state.partition)
							break
						case 'none':
							throw new Error(
								`Offset out of range for ${state.topic}-${state.partition} at offset ${state.offset}`
							)
					}
				}
				break

			case ErrorCode.NotLeaderOrFollower:
			case ErrorCode.UnknownTopicOrPartition:
				state.cachedLeader = null // Invalidate cached leader
				await this.cluster.refreshMetadata([state.topic])
				break

			default:
				this.logger.error('fetch error', {
					topic: state.topic,
					partition: state.partition,
					errorCode,
				})
		}
	}

	/**
	 * Stop the fetch loop
	 */
	stop(): void {
		this.logger.info('stopping fetch manager')
		this.running = false
		// Abort global controller (for sleep in start loop)
		this.abortController?.abort()
		// Abort all per-partition controllers
		for (const state of this.partitionStates.values()) {
			state.abortController.abort()
		}
	}

	/**
	 * Fetch loop for a single partition
	 */
	private async fetchPartitionLoop(
		state: PartitionState,
		onRecords: FetchRecordsCallback,
		signal: AbortSignal
	): Promise<void> {
		const key = tpKey(state.topic, state.partition)
		this.logger.debug('starting fetch loop for partition', {
			topic: state.topic,
			partition: state.partition,
			startOffset: String(state.offset),
		})

		try {
			while (this.running && !signal.aborted) {
				try {
					// Acquire semaphore slot (pass signal to avoid leak on abort)
					const acquired = await this.semaphore.acquire(signal)

					// If aborted while waiting, exit cleanly (no semaphore slot held)
					if (!acquired) {
						break
					}

					if (!this.running || signal.aborted) {
						this.semaphore.release()
						break
					}

					try {
						// Skip if paused
						if (state.paused) {
							await sleep(100, { signal })
							continue
						}

						// Fetch records
						const records = await this.fetchRecords(state)

						if (records.length > 0) {
							this.logger.debug('fetched records', {
								topic: state.topic,
								partition: state.partition,
								count: records.length,
								highOffset: String(records[records.length - 1]!.offset),
							})
							await onRecords(state.topic, state.partition, records)

							const lastRecord = records[records.length - 1]!
							state.offset = lastRecord.offset + 1n
						} else {
							// No records - wait a bit before next fetch
							await sleep(100, { signal })
						}
					} finally {
						this.semaphore.release()
					}
				} catch (error) {
					if (signal.aborted) {
						break
					}

					this.logger.error('fetch error', {
						topic: state.topic,
						partition: state.partition,
						error: (error as Error).message,
					})

					// Back off before retrying
					try {
						await sleep(1000, { signal })
					} catch {
						break
					}
				}
			}
		} finally {
			// Clean up: remove from active loops when exiting
			const current = this.activeLoops.get(key)
			if (current && current.abortController === state.abortController) {
				this.activeLoops.delete(key)
			}
			this.logger.debug('fetch loop ended for partition', { topic: state.topic, partition: state.partition })
		}
	}

	/**
	 * Fetch records from a partition
	 */
	private async fetchRecords(state: PartitionState): Promise<DecodedRecord[]> {
		// Use cached leader or lookup
		if (!state.cachedLeader) {
			state.cachedLeader = await this.cluster.getLeaderForPartition(state.topic, state.partition)
		}
		const leader = state.cachedLeader
		const fetchOffset = state.offset

		const response = await leader.fetch({
			replicaId: -1,
			maxWaitMs: this.config.maxWaitMs,
			minBytes: this.config.minBytes,
			maxBytes: this.config.maxBytesPerPartition * this.partitionStates.size,
			isolationLevel: this.config.isolationLevel === 'read_committed' ? 1 : 0,
			topics: [
				{
					topic: state.topic,
					topicId: '00000000-0000-0000-0000-000000000000',
					partitions: [
						{
							partitionIndex: state.partition,
							currentLeaderEpoch: -1,
							fetchOffset: state.offset,
							lastFetchedEpoch: -1,
							logStartOffset: -1n,
							partitionMaxBytes: this.config.maxBytesPerPartition,
						},
					],
				},
			],
		})

		this.logger.debug('fetch response received', {
			topic: state.topic,
			partition: state.partition,
			topicCount: response.topics.length,
			errorCode: response.errorCode,
		})

		// Find our partition response
		const topicResponse = response.topics.find(t => t.topic === state.topic)
		const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === state.partition)

		this.logger.debug('partition response', {
			topic: state.topic,
			partition: state.partition,
			found: !!partitionResponse,
			errorCode: partitionResponse?.errorCode,
			highWatermark: partitionResponse?.highWatermark?.toString(),
			recordsDataLength: partitionResponse?.recordsData?.length,
		})

		if (!partitionResponse) {
			return []
		}

		// Handle errors
		if (partitionResponse.errorCode !== ErrorCode.None) {
			// Handle specific errors
			switch (partitionResponse.errorCode) {
				case ErrorCode.OffsetOutOfRange:
					// If offsetManager and autoOffsetReset are configured, apply recovery strategy
					if (this.offsetManager && this.autoOffsetReset) {
						this.logger.error('offset out of range, resetting based on autoOffsetReset strategy', {
							topic: state.topic,
							partition: state.partition,
							currentOffset: String(state.offset),
							strategy: this.autoOffsetReset,
						})

						// Apply autoOffsetReset strategy
						switch (this.autoOffsetReset) {
							case 'earliest':
								state.offset = await this.offsetManager.getEarliestOffset(state.topic, state.partition)
								this.logger.info('reset to earliest offset', {
									topic: state.topic,
									partition: state.partition,
									newOffset: String(state.offset),
								})
								break
							case 'latest':
								state.offset = await this.offsetManager.getLatestOffset(state.topic, state.partition)
								this.logger.info('reset to latest offset', {
									topic: state.topic,
									partition: state.partition,
									newOffset: String(state.offset),
								})
								break
							case 'none':
								throw new Error(
									`Offset out of range for ${state.topic}-${state.partition} at offset ${state.offset} and autoOffsetReset is 'none'`
								)
						}
						// Return empty to continue the fetch loop with the new offset
						return []
					} else {
						// Fall back to old behavior if not configured
						this.logger.error('offset out of range', {
							topic: state.topic,
							partition: state.partition,
							offset: String(state.offset),
						})
						throw new Error(`Offset out of range: ${state.offset}`)
					}

				case ErrorCode.NotLeaderOrFollower:
				case ErrorCode.UnknownTopicOrPartition:
					// Invalidate cached leader and refresh metadata
					state.cachedLeader = null
					this.logger.debug('refreshing metadata due to fetch error', {
						topic: state.topic,
						partition: state.partition,
						errorCode: partitionResponse.errorCode,
					})
					await this.cluster.refreshMetadata([state.topic])
					return []

				default:
					this.logger.error('fetch protocol error', {
						topic: state.topic,
						partition: state.partition,
						errorCode: partitionResponse.errorCode,
					})
					throw new Error(`Fetch error ${partitionResponse.errorCode} for ${state.topic}-${state.partition}`)
			}
		}

		// Decode records
		if (!partitionResponse.recordsData || partitionResponse.recordsData.length === 0) {
			return []
		}

		const decodeResult = this.decodeRecords(
			state.topic,
			state.partition,
			partitionResponse.recordsData,
			partitionResponse.abortedTransactions,
			partitionResponse.lastStableOffset
		)
		const records = decodeResult instanceof Promise ? await decodeResult : decodeResult
		return filterRecordsFromOffset(records, fetchOffset)
	}

	/**
	 * Decode records from raw buffer
	 * Returns DecodedRecord[] directly for better performance (avoids intermediate Message creation)
	 * Returns sync result or Promise depending on whether compression is encountered
	 */
	private decodeRecords(
		_topic: string,
		_partition: number,
		data: Buffer,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): DecodedRecord[] | Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)

		// Decode all record batches first so we can:
		// - drop control batches
		// - filter aborted transactional records for read_committed
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}> = []

		// Try synchronous decoding first (faster, no promise overhead)
		let needsAsync = false
		while (decoder.remaining() > 0) {
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = decodeRecordBatchFromSync(decoder, {
					assumeSequentialOffsets: true,
				})
				batches.push(batch)
			} catch (e) {
				// If sync failed due to compression, switch to async path
				if ((e as Error).message.includes('cannot decode compressed')) {
					needsAsync = true
					break
				}
				// Other errors mean end of valid data
				break
			}
		}

		// If compression was encountered, use async decode
		if (needsAsync) {
			return this.decodeRecordsAsync(data, abortedTransactions, lastStableOffset)
		}

		// Sync path - process batches directly
		return this.processBatches(batches, abortedTransactions, lastStableOffset)
	}

	/**
	 * Async decode path for compressed batches
	 */
	private async decodeRecordsAsync(
		data: Buffer,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}> = []

		while (decoder.remaining() > 0) {
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = await decodeRecordBatchFrom(decoder, {
					assumeSequentialOffsets: true,
				})
				batches.push(batch)
			} catch {
				break
			}
		}

		return this.processBatches(batches, abortedTransactions, lastStableOffset)
	}

	/**
	 * Process decoded batches into records
	 */
	private processBatches(
		batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}>,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): DecodedRecord[] {
		// Pre-compute aborted transaction ranges (exclusive end offset)
		const abortedRanges: Array<{ producerId: bigint; start: bigint; endExclusive: bigint }> = []
		if (this.config.isolationLevel === 'read_committed' && abortedTransactions.length > 0) {
			// Control batches act as transaction markers; use the next marker offset as the end of the transaction.
			const controlBatches = batches
				.filter(b => isControlBatch(b.attributes))
				.map(b => ({ producerId: b.producerId, offset: b.baseOffset }))
				.sort((a, b) => (a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0))

			for (const tx of abortedTransactions) {
				const end =
					controlBatches.find(cb => cb.producerId === tx.producerId && cb.offset > tx.firstOffset)?.offset ??
					lastStableOffset
				abortedRanges.push({ producerId: tx.producerId, start: tx.firstOffset, endExclusive: end })
			}
		}

		const result: DecodedRecord[] = []

		// Fast path: no aborted transactional ranges to filter.
		// This is the common case (non-transactional workloads).
		if (abortedRanges.length === 0) {
			for (const batch of batches) {
				// Control batches contain transactional markers (commit/abort) and should never be exposed as user messages.
				if (isControlBatch(batch.attributes)) {
					continue
				}

				const records = batch.records
				for (let i = 0; i < records.length; i++) {
					result.push(records[i]!)
				}
			}
			return result
		}

		for (const batch of batches) {
			// Control batches contain transactional markers (commit/abort) and should never be exposed as user messages.
			if (isControlBatch(batch.attributes)) {
				continue
			}

			const transactional = isTransactional(batch.attributes)
			const ranges = transactional ? abortedRanges.filter(r => r.producerId === batch.producerId) : []

			for (const record of batch.records) {
				// For read_committed, drop records that are part of aborted transactions.
				if (ranges.length > 0 && ranges.some(r => record.offset >= r.start && record.offset < r.endExclusive)) {
					continue
				}

				// Push DecodedRecord directly - consumer will create Message with decoded value
				result.push(record)
			}
		}

		return result
	}
}
