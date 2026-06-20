/**
 * Kafka Producer - high-level producer API with implicit batching
 */

import { EventEmitter } from 'node:events'
import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { ErrorCode, isRetriableError } from '@/protocol/messages/error-codes.js'
import {
	shouldRefreshMetadata,
	ProducerFencedError,
	InvalidTxnStateError,
	TransactionAbortedError,
	KafkaProtocolError,
} from '@/client/errors.js'
import { createRecordBatch, encodeRecordBatch, encodeRecordBatchSync } from '@/protocol/records/record-batch.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { ReconnectionStrategy } from '@/network/reconnection.js'
import { noopLogger, type Logger } from '@/logger.js'
import { sleep as sleepMs } from '@/utils/sleep.js'
import { createRetryStrategyOptions, retry } from '@/utils/retry.js'
import { RecordAccumulator } from './accumulator.js'
import { murmur2Partitioner } from './partitioners/murmur2.js'
import { createRoundRobinPartitioner } from './partitioners/round-robin.js'
import type {
	ProducerConfig,
	ResolvedProducerConfig,
	ProducerMessage,
	SendResult,
	QueuedMessage,
	PartitionBatch,
	TopicDefinition,
	ProducerState,
	IdempotentState,
	ProducerEvents,
	Acks,
	Partitioner,
	CompressionName,
	PartitionerFunction,
	TransactionState,
	TransactionOptions,
	TopicPartitionOffset,
	SendOffsetsParams,
	ProducerTransaction,
	GroupMetadata,
	ProducerTraceFn,
} from './types.js'

const EMPTY_BUFFER = Buffer.alloc(0)

type PreparedProduceBatch = {
	batch: PartitionBatch
	encodedBatch: Buffer
	baseSequence: number
	recordCount: number
	batchId: symbol
	messagesToSend: QueuedMessage[]
	/**
	 * True once the encoded batch has been handed to broker.produce().
	 *
	 * A batch that has never been transmitted is safe to roll back its reserved
	 * sequence — the broker never saw the sequence number.  A batch that was sent
	 * (even just once) may have been appended: if the connection drops or the
	 * request times out before we receive a response, the outcome is AMBIGUOUS and
	 * we must NOT reuse the sequence.  Instead we fence + reinit the producer so
	 * the next send uses a fresh epoch whose sequence starts at 0.
	 */
	everSent: boolean
	/**
	 * The producerId encoded into this batch's RecordBatch header.
	 *
	 * Used by handleDuplicateSequence to detect sequence-state corruption: if
	 * the current producer identity has changed (epoch bumped by fencing or
	 * reinit) but the broker returns DuplicateSequenceNumber for this batch,
	 * the response does not match this batch — do NOT resolve as success.
	 */
	encodedProducerId: bigint
	/**
	 * The producerEpoch encoded into this batch's RecordBatch header.
	 */
	encodedProducerEpoch: number
}

type ProduceAttemptOutcome = {
	retryBatches: PreparedProduceBatch[]
	stop: boolean
	retryError?: Error
}

function isCoordinatorErrorCode(errorCode: ErrorCode): boolean {
	return errorCode === ErrorCode.NotCoordinator || errorCode === ErrorCode.CoordinatorNotAvailable
}

/**
 * Default producer configuration values
 */
const DEFAULT_CONFIG: Required<Omit<ProducerConfig, 'transactionalId' | 'trace'>> = {
	lingerMs: 5,
	maxBatchBytes: 16384,
	acks: 'all',
	retries: 3,
	retryBackoffMs: 100,
	maxRetryBackoffMs: 1000,
	compression: 'none',
	partitioner: 'murmur2',
	requestTimeoutMs: 30000,
	idempotent: false,
	maxInFlight: 5,
	transactionTimeoutMs: 60000,
	maxBlockMs: 60000,
}

/**
 * Kafka Producer with implicit batching
 *
 * Messages are queued locally and batched before sending based on
 * lingerMs (time) and maxBatchBytes (size) configuration.
 *
 * @example
 * ```typescript
 * const producer = client.producer({
 *   lingerMs: 5,
 *   maxBatchBytes: 16384,
 *   acks: 'all',
 *   compression: 'snappy',
 * })
 *
 * // Single message
 * const result = await producer.send(orderTopic, { key: order.id, value: order })
 *
 * // Fire-and-forget
 * producer.send(orderTopic, { key: order.id, value: order })
 * await producer.flush()
 *
 * await producer.disconnect()
 * ```
 */
export class Producer extends EventEmitter<ProducerEvents> {
	private readonly cluster: Cluster
	private readonly config: ResolvedProducerConfig
	private readonly logger: Logger
	private readonly accumulator: RecordAccumulator
	private readonly trace?: ProducerTraceFn

	private state: ProducerState = 'idle'
	private stickyPartitions = new Map<string, { partition: number; partitionCount: number }>() // For keyless messages
	private inflight = new Set<Promise<void>>()
	private partitionCache = new Map<string, { count: number; fetchedAt: number }>()

	/** Partition count cache TTL in ms (5 minutes) */
	private static readonly PARTITION_CACHE_TTL_MS = 5 * 60 * 1000

	// Idempotent producer state
	private producerId: bigint = -1n
	private producerEpoch: number = -1
	private sequences = new Map<string, number>() // "topic:partition" -> committed sequence
	private reservedSequences = new Map<string, number>() // "topic:partition" -> reserved count
	private idempotentState: IdempotentState = 'idle'
	private initPromise: Promise<void> | null = null

	// In-flight batch tracking for fencing recovery
	private inflightBatches = new Map<
		symbol,
		{
			topic: string
			partition: number
			messages: QueuedMessage[]
			recordCount: number
		}
	>()

	private inflightCount = 0

	// Broker-level batching: collect ready batches and group by broker
	private pendingBatches: PartitionBatch[] = []
	private drainScheduled = false
	// Every outstanding send() promise. A send's promise settles only once all its
	// messages are acked (including retries), so flush() can wait for in-progress sends
	// by awaiting these directly — no need to inspect internal batch/drain state.
	private activeSends = new Set<Promise<unknown>>()
	// Partitions with an in-flight send. A partition stays muted until its send
	// settles, so at most one batch per partition is ever in flight (preserves
	// per-partition ordering for the idempotent producer).
	private mutedPartitions = new Set<string>()

	// Transaction state (only used when transactionalId is set)
	private transactionState: TransactionState = 'idle'
	private partitionsInTransaction = new Set<string>() // "topic:partition"
	private offsetsToCommit = new Map<string, { offsets: TopicPartitionOffset[]; groupMetadata?: GroupMetadata }>() // groupId -> { offsets, groupMetadata }

	constructor(cluster: Cluster, config: ProducerConfig = {}) {
		super()
		this.cluster = cluster
		this.logger = (cluster as unknown as { logger?: Logger }).logger?.child({ component: 'producer' }) ?? noopLogger
		this.trace = config.trace

		this.config = this.resolveConfig(config)
		this.accumulator = new RecordAccumulator(this.config, this.logger)
		this.accumulator.on('batchReady', batch => {
			// Collect batches and schedule drain to group by broker
			this.pendingBatches.push(batch)
			this.scheduleDrain()
		})

		this.state = 'running'
	}

	/**
	 * Schedule a microtask to drain pending batches
	 *
	 * This allows multiple batchReady events from a single send() call
	 * to be collected and grouped by broker before sending.
	 */
	private scheduleDrain(): void {
		if (this.drainScheduled) return
		this.drainScheduled = true

		queueMicrotask(() => {
			this.drainScheduled = false
			this.drainPendingBatches()
		})
	}

	/**
	 * Drain pending batches, group by broker, and send
	 *
	 * This implements the Java client pattern of sending one ProduceRequest
	 * per broker containing all ready partition batches for that broker.
	 */
	private drainPendingBatches(): void {
		const batches = this.pendingBatches
		this.pendingBatches = []

		if (batches.length === 0) return

		// Take at most one batch per partition, and never a partition that already has
		// an in-flight send (muted). This preserves per-partition ordering for the
		// idempotent producer: sending a second batch while an earlier one is in flight
		// would let a retriable failure on the earlier batch reorder sequences, so the
		// later (higher-sequence) batch reaches the broker first and trips
		// OutOfOrderSequenceNumber. Deferred batches stay queued and are drained when
		// their partition's in-flight send settles (see the finally below) — every
		// deferred batch's partition is muted, so a re-drain is always pending.
		const pickedPartitions = new Set<string>()
		const toSend: PartitionBatch[] = []

		for (const batch of batches) {
			const key = `${batch.topic}:${batch.partition}`
			if (this.mutedPartitions.has(key) || pickedPartitions.has(key)) {
				this.pendingBatches.push(batch)
			} else {
				pickedPartitions.add(key)
				toSend.push(batch)
			}
		}

		if (toSend.length === 0) return

		// Mute the partitions we are about to send so subsequent drains skip them.
		for (const key of pickedPartitions) {
			this.mutedPartitions.add(key)
		}

		const promise = this.sendBatchesWithRetries(toSend)
		this.inflight.add(promise)
		promise.finally(() => {
			this.inflight.delete(promise)
			for (const key of pickedPartitions) {
				this.mutedPartitions.delete(key)
			}
			// Now-unmuted partitions may have deferred batches waiting.
			if (this.pendingBatches.length > 0) {
				this.scheduleDrain()
			}
		})
	}

	private async sendBatchesWithRetries(batches: PartitionBatch[]): Promise<void> {
		try {
			const preparedBatches = await this.prepareProduceBatches(batches)
			if (preparedBatches.length > 0) {
				await this.sendPreparedBatches(preparedBatches)
			}
		} finally {
			for (let i = 0; i < batches.length; i++) {
				this.accumulator.batchCompleted()
			}
		}
	}

	private async prepareProduceBatches(batches: PartitionBatch[]): Promise<PreparedProduceBatch[]> {
		const preparedBatches: PreparedProduceBatch[] = []
		for (const batch of batches) {
			const { topic, partition, messages } = batch
			let recordCount = messages.length
			let messagesToSend = messages

			let baseSequence = -1
			if (this.config.idempotent) {
				const reservation = this.reserveSequence(topic, partition, recordCount)
				baseSequence = reservation.baseSequence

				// Handle sequence wrap - only use reserved count
				if (reservation.reservedCount < recordCount) {
					messagesToSend = messages.slice(0, reservation.reservedCount)
					// Re-queue remaining messages
					const remainingBatch: PartitionBatch = {
						topic,
						partition,
						messages: messages.slice(reservation.reservedCount),
						sizeBytes: messages
							.slice(reservation.reservedCount)
							.reduce((sum, m) => sum + (m.value?.length ?? 0) + (m.key?.length ?? 0), 0),
						createdAt: Date.now(),
					}
					// Prepend, not push: this remainder holds the messages that immediately
					// follow the ones being sent now. It must drain before any later batch
					// already queued for this partition, or per-partition order would break
					// (the later batch would get a lower sequence than the remainder).
					this.pendingBatches.unshift(remainingBatch)
					this.scheduleDrain()
					recordCount = reservation.reservedCount
				}
			}

			const batchId = Symbol('batch')
			if (this.config.idempotent) {
				this.inflightBatches.set(batchId, { topic, partition, messages: messagesToSend, recordCount })
			}

			let baseTimestamp: bigint | undefined
			if (messagesToSend.length > 0) {
				baseTimestamp = messagesToSend[0]!.timestamp
				for (let i = 1; i < messagesToSend.length; i++) {
					const ts = messagesToSend[i]!.timestamp
					if (ts < baseTimestamp) {
						baseTimestamp = ts
					}
				}
			}

			const encodeStart = performance.now()
			const recordBatch = createRecordBatch(
				messagesToSend,
				0n,
				baseTimestamp,
				this.config.idempotent ? this.producerId : -1n,
				this.config.idempotent ? this.producerEpoch : -1,
				baseSequence
			)

			try {
				const encodeOptions = {
					compression: this.config.compression,
					isTransactional: this.config.transactionalId !== null,
				}

				// Use synchronous encoding when compression is disabled (faster)
				const encodedBatch =
					this.config.compression === CompressionType.None
						? encodeRecordBatchSync(recordBatch, encodeOptions)
						: await encodeRecordBatch(recordBatch, encodeOptions)

				const encodeEnd = performance.now()
				if (this.trace) {
					this.trace({
						stage: 'encode_batch',
						durationMs: encodeEnd - encodeStart,
						recordCount,
						bytes: encodedBatch.length,
						topic,
						partition,
					})
				}

				preparedBatches.push({
					batch,
					encodedBatch,
					baseSequence,
					recordCount,
					batchId,
					messagesToSend,
					everSent: false,
					encodedProducerId: this.config.idempotent ? this.producerId : -1n,
					encodedProducerEpoch: this.config.idempotent ? this.producerEpoch : -1,
				})
			} catch (error) {
				// Encoding failed - rollback and reject
				if (this.config.idempotent) {
					this.rollbackSequence(topic, partition, recordCount)
					this.inflightBatches.delete(batchId)
				}
				for (const msg of messagesToSend) {
					msg.reject(error instanceof Error ? error : new Error(String(error)))
				}
			}
		}
		return preparedBatches
	}

	private async sendPreparedBatches(preparedBatches: PreparedProduceBatch[]): Promise<void> {
		const strategy = new ReconnectionStrategy(createRetryStrategyOptions(this.config))
		let pendingBatches = preparedBatches

		while (strategy.shouldReconnect()) {
			try {
				const outcome = await this.sendPreparedBatchesOnce(pendingBatches)
				if (outcome.stop) {
					return
				}

				if (outcome.retryBatches.length === 0) {
					return
				}

				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())
					pendingBatches = outcome.retryBatches
					continue
				}

				if (outcome.retryError) {
					// retryError is set when sendPreparedBatchesToBroker threw a network
					// exception (broker.produce() rejected before a response was received).
					// The batches were transmitted (everSent=true) but the outcome is
					// ambiguous — fence + reinit for idempotent producers.
					this.failPreparedBatches(outcome.retryBatches, () => outcome.retryError!, true)
				} else if (this.config.idempotent && outcome.retryBatches.some(p => p.everSent)) {
					// Retry exhaustion for an idempotent batch that was already transmitted at
					// least once (everSent=true).  The triggering code (e.g.
					// NotEnoughReplicasAfterAppend, RequestTimedOut) may mean the append
					// DID happen — the broker's response is not a definitive rejection of the
					// write.  We cannot safely roll back the sequence and reuse it under the
					// current epoch.  Treat the outcome as ambiguous: fence + reinit so the
					// next epoch starts at sequence 0.  The app still gets a clear rejection.
					this.failPreparedBatches(
						outcome.retryBatches,
						prepared =>
							new Error(
								`Produce failed for ${prepared.batch.topic}-${prepared.batch.partition}: max retries exceeded`
							),
						true // ambiguous — fence + reinit
					)
				} else {
					// Retry exhaustion for a batch that was never transmitted (everSent=false),
					// or a non-idempotent producer.  Each attempt received an explicit broker
					// response — the outcome is definitive (broker rejected, not appended).
					this.failPreparedBatches(
						outcome.retryBatches,
						prepared =>
							new Error(
								`Produce failed for ${prepared.batch.topic}-${prepared.batch.partition}: max retries exceeded`
							)
					)
				}
				return
			} catch (error) {
				strategy.recordFailure()

				if (!strategy.shouldReconnect()) {
					const failure = error instanceof Error ? error : new Error(String(error))
					// Network exception with no broker response — outcome is ambiguous for
					// any batch that was already transmitted (everSent=true).
					this.failPreparedBatches(pendingBatches, () => failure, true)
					return
				}

				await this.sleep(strategy.nextDelay())
			}
		}
	}

	private async sendPreparedBatchesOnce(preparedBatches: PreparedProduceBatch[]): Promise<ProduceAttemptOutcome> {
		const batchesByBroker = await this.groupPreparedBatchesByLeader(preparedBatches)
		const outcomes = await Promise.all(
			Array.from(batchesByBroker.entries()).map(async ([, { broker, batches }]) => {
				try {
					return await this.sendPreparedBatchesToBroker(broker, batches, preparedBatches)
				} catch (error) {
					return {
						retryBatches: batches,
						stop: false,
						retryError: error instanceof Error ? error : new Error(String(error)),
					}
				}
			})
		)

		if (outcomes.some(outcome => outcome.stop)) {
			return { retryBatches: [], stop: true }
		}

		return {
			retryBatches: outcomes.flatMap(outcome => outcome.retryBatches),
			stop: false,
			retryError: outcomes.find(outcome => outcome.retryError)?.retryError,
		}
	}

	private async groupPreparedBatchesByLeader(
		preparedBatches: PreparedProduceBatch[]
	): Promise<Map<number, { broker: Broker; batches: PreparedProduceBatch[] }>> {
		const batchesByBroker = new Map<number, { broker: Broker; batches: PreparedProduceBatch[] }>()
		for (const prepared of preparedBatches) {
			const { batch } = prepared
			const broker = await this.cluster.getLeaderForPartition(batch.topic, batch.partition)
			const existing = batchesByBroker.get(broker.nodeId) ?? { broker, batches: [] }
			existing.batches.push(prepared)
			batchesByBroker.set(broker.nodeId, existing)
		}
		return batchesByBroker
	}

	private async sendPreparedBatchesToBroker(
		broker: Broker,
		preparedBatches: PreparedProduceBatch[],
		allPendingBatches: PreparedProduceBatch[]
	): Promise<ProduceAttemptOutcome> {
		const request = this.buildProduceRequest(preparedBatches)

		await this.acquireInflightSlot()
		try {
			// Mark every batch as transmitted BEFORE we await the network send.
			// Once broker.produce() is called the broker may have appended the batch —
			// even a connection-drop or timeout after this point means the outcome is
			// ambiguous.  failPreparedBatch uses everSent to decide whether it is safe
			// to roll back the reserved sequence or whether it must fence + reinit.
			for (const prepared of preparedBatches) {
				prepared.everSent = true
			}
			const requestStart = performance.now()
			const response = await broker.produce(request)
			const requestEnd = performance.now()
			if (this.trace) {
				this.trace({
					stage: 'produce_request',
					durationMs: requestEnd - requestStart,
					recordCount: preparedBatches.reduce((sum, p) => sum + p.recordCount, 0),
					bytes: preparedBatches.reduce((sum, p) => sum + p.encodedBatch.length, 0),
					brokerId: broker.nodeId,
				})
			}

			const retryBatches: PreparedProduceBatch[] = []
			for (const prepared of preparedBatches) {
				const { batch } = prepared

				// Stale-epoch guard: if a fenceAndReinit occurred while this request was
				// in flight, this response belongs to the old epoch.  fenceAndReinit already
				// rejected the messages — skip without touching any sequence/commit state.
				// This protects handleBatchSuccess (commitSequence), handleDuplicateSequence,
				// and all error paths uniformly.
				if (
					this.config.idempotent &&
					(prepared.encodedProducerId !== this.producerId ||
						prepared.encodedProducerEpoch !== this.producerEpoch)
				) {
					this.logger.debug('skipping stale-epoch response', {
						topic: batch.topic,
						partition: batch.partition,
						encodedEpoch: prepared.encodedProducerEpoch,
						currentEpoch: this.producerEpoch,
					})
					continue
				}

				const topicResponse = response.topics.find(t => t.name === batch.topic)
				const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === batch.partition)

				if (!partitionResponse) {
					// The broker responded to the ProduceRequest but did not include this
					// partition.  Since the request was transmitted (everSent=true) the outcome
					// is ambiguous — treat the same as a connection failure: fence + reinit.
					const ambiguousError = new Error(
						`Ambiguous produce outcome (no broker response for ${batch.topic}-${batch.partition}) — fencing producer`
					)
					this.fenceAndReinit(ambiguousError, allPendingBatches)
					return { retryBatches: [], stop: true }
				}

				if (partitionResponse.errorCode !== ErrorCode.None) {
					const errorCode = partitionResponse.errorCode

					if (
						this.config.idempotent &&
						(errorCode === ErrorCode.ProducerFenced || errorCode === ErrorCode.InvalidProducerEpoch)
					) {
						this.handleFencingError(prepared, allPendingBatches)
						return { retryBatches: [], stop: true }
					}

					if (this.config.idempotent && errorCode === ErrorCode.DuplicateSequenceNumber) {
						this.handleDuplicateSequence(prepared, partitionResponse, allPendingBatches)
						continue
					}

					if (this.config.idempotent && errorCode === ErrorCode.OutOfOrderSequenceNumber) {
						this.handleOutOfOrderSequence(prepared, allPendingBatches)
						return { retryBatches: [], stop: true }
					}

					if (isRetriableError(errorCode)) {
						retryBatches.push(prepared)
						if (shouldRefreshMetadata(errorCode)) {
							await this.cluster.refreshMetadata([batch.topic])
							this.partitionCache.delete(batch.topic)
						}
						continue
					}

					// Non-retriable partition error (e.g. MessageTooLarge, InvalidRecord).
					// The broker explicitly rejected the batch — the outcome is definitive,
					// not ambiguous.  Roll back the sequence and reject the messages.
					this.failPreparedBatch(
						prepared,
						new Error(
							`Produce failed for ${batch.topic}-${batch.partition}: ${ErrorCode[errorCode] ?? errorCode}`
						)
					)
					continue
				}

				this.handleBatchSuccess(prepared, partitionResponse)
			}

			return { retryBatches, stop: false }
		} finally {
			this.releaseInflightSlot()
		}
	}

	private buildProduceRequest(preparedBatches: PreparedProduceBatch[]): ProduceRequest {
		const topicMap = new Map<string, Array<{ partitionIndex: number; records: Buffer }>>()
		for (const { batch, encodedBatch } of preparedBatches) {
			const partitions = topicMap.get(batch.topic) ?? []
			partitions.push({ partitionIndex: batch.partition, records: encodedBatch })
			topicMap.set(batch.topic, partitions)
		}

		return {
			transactionalId: this.config.transactionalId,
			acks: this.config.acks,
			timeoutMs: this.config.requestTimeoutMs,
			topics: Array.from(topicMap.entries()).map(([name, partitions]) => ({
				name,
				partitions,
			})),
		}
	}

	/**
	 * Fail a collection of prepared batches, choosing between a definitive rollback
	 * (outcome certain — broker explicitly rejected or batch was never transmitted)
	 * and a fence+reinit (outcome ambiguous — connection failure after transmission).
	 *
	 * @param ambiguous - true when the failure is a network exception that occurred
	 *   AFTER the ProduceRequest was transmitted (everSent=true batches may have been
	 *   appended by the broker).  false when the broker's own response is the final
	 *   authority (retriable error exhaustion, explicit non-retriable error code).
	 */
	private failPreparedBatches(
		preparedBatches: PreparedProduceBatch[],
		errorFor: (prepared: PreparedProduceBatch) => Error,
		ambiguous = false
	): void {
		// An idempotent producer that already transmitted a batch (everSent=true) and
		// lost the connection before any broker response cannot know whether the broker
		// appended the records.  Reusing the same sequence number under the same epoch
		// would let a future DuplicateSequenceNumber response resolve a DIFFERENT batch's
		// messages with the wrong offsets (Defect B / false-ack scenario).
		// Fence + reinit so the next epoch starts at sequence 0.
		if (ambiguous && this.config.idempotent && preparedBatches.some(p => p.everSent)) {
			const firstSent = preparedBatches.find(p => p.everSent)!
			const error = errorFor(firstSent)
			const ambiguousError = new Error(
				`Ambiguous produce outcome after send — fencing producer for safety: ${error.message}`
			)
			this.fenceAndReinit(ambiguousError, preparedBatches)
			return
		}

		for (const prepared of preparedBatches) {
			this.failPreparedBatch(prepared, errorFor(prepared))
		}
	}

	private failPreparedBatch(prepared: PreparedProduceBatch, error: Error): void {
		const { batch, recordCount, batchId, messagesToSend } = prepared
		if (this.config.idempotent) {
			// Roll back the reserved sequence regardless of everSent when the outcome is
			// DEFINITIVE (broker explicitly rejected the batch via an error code, or this
			// is a pre-send encoding failure).  The caller is responsible for ensuring
			// this method is only called for definitive failures — ambiguous outcomes
			// (connection failures, no broker response) go through fenceAndReinit instead.
			this.rollbackSequence(batch.topic, batch.partition, recordCount)
			this.inflightBatches.delete(batchId)
		}
		for (const msg of messagesToSend) {
			msg.reject(error)
		}
	}

	/**
	 * Handle successful batch response
	 */
	private handleBatchSuccess(
		prepared: {
			batch: PartitionBatch
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		},
		partitionResponse: { baseOffset: bigint; logAppendTimeMs: bigint }
	): void {
		const { batch, recordCount, batchId, messagesToSend } = prepared
		const baseOffset = partitionResponse.baseOffset
		const timestamp = new Date(Number(partitionResponse.logAppendTimeMs))

		for (let i = 0; i < messagesToSend.length; i++) {
			messagesToSend[i]!.resolve({
				topic: batch.topic,
				partition: batch.partition,
				offset: baseOffset + BigInt(i),
				timestamp,
			})
		}

		if (this.config.idempotent) {
			this.commitSequence(batch.topic, batch.partition, recordCount)
			this.inflightBatches.delete(batchId)
		}

		this.rotateStickyPartition(batch.topic)
	}

	/**
	 * Handle duplicate sequence error.
	 *
	 * DuplicateSequenceNumber means the broker already appended a batch with this
	 * producer/epoch/baseSequence.  We treat it as success — the broker is telling us
	 * the exact same batch was already written (genuine retry dedup).
	 *
	 * The stale-epoch guard at the top of the response loop already ensures this
	 * method is only reached for batches whose encodedProducerId/Epoch matches the
	 * current epoch.  The check below is defense-in-depth.
	 */
	private handleDuplicateSequence(
		prepared: PreparedProduceBatch,
		partitionResponse: { baseOffset: bigint; logAppendTimeMs: bigint },
		allPendingBatches: PreparedProduceBatch[]
	): void {
		const { batch, baseSequence, encodedProducerId, encodedProducerEpoch } = prepared

		// Guard: producer identity must match what was encoded.
		// If the epoch changed (reinit happened between prepare and this response),
		// this response belongs to a different producer era and is not a legitimate
		// dedup for this batch's messages.
		if (encodedProducerId !== this.producerId || encodedProducerEpoch !== this.producerEpoch) {
			this.logger.error('duplicate sequence number with stale producer identity — fencing producer', {
				topic: batch.topic,
				partition: batch.partition,
				baseSequence,
				encodedProducerId: encodedProducerId.toString(),
				encodedProducerEpoch,
				currentProducerId: this.producerId.toString(),
				currentProducerEpoch: this.producerEpoch,
			})
			this.fenceAndReinit(
				new Error(
					`DuplicateSequenceNumber for ${batch.topic}-${batch.partition} with stale producer identity ` +
						`(encoded epoch=${encodedProducerEpoch} current epoch=${this.producerEpoch}) — sequence state corrupted`
				),
				allPendingBatches
			)
			return
		}

		this.logger.debug('duplicate sequence number - treating as success (genuine retry dedup)', {
			topic: batch.topic,
			partition: batch.partition,
			baseSequence,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.handleBatchSuccess(prepared, partitionResponse)
	}

	/**
	 * Handle out-of-order sequence error
	 *
	 * OOO means the per-partition sequence is desynced from the broker; recovery
	 * is the same as a fence — re-init the producer. Matches Java client behavior.
	 */
	private handleOutOfOrderSequence(
		prepared: {
			batch: PartitionBatch
			baseSequence: number
			messagesToSend: QueuedMessage[]
		},
		allPrepared: Array<{
			batch: PartitionBatch
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		}>
	): void {
		const { batch, baseSequence } = prepared

		this.logger.error('out of order sequence number, fencing producer for re-init', {
			topic: batch.topic,
			partition: batch.partition,
			baseSequence,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.fenceAndReinit(
			new Error(`Out of order sequence for ${batch.topic}-${batch.partition}: baseSequence=${baseSequence}`),
			allPrepared
		)
	}

	/**
	 * Handle producer fencing error
	 */
	private handleFencingError(
		prepared: { batch: PartitionBatch },
		allPrepared: Array<{
			batch: PartitionBatch
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		}>
	): void {
		const { batch } = prepared

		this.logger.info('producer fenced', {
			topic: batch.topic,
			partition: batch.partition,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.fenceAndReinit(new ProducerFencedError(this.producerId, this.producerEpoch), allPrepared)
	}

	/**
	 * Mark the producer fenced, reject every in-flight batch with `error`, and
	 * kick off async re-initialization. Shared by ProducerFenced/InvalidEpoch,
	 * OutOfOrderSequence, and ambiguous-outcome terminal failures — all are
	 * unrecoverable without a fresh producerId/epoch.
	 *
	 * NOTE: initializeIdempotentProducer() clears sequences and reservedSequences,
	 * so the rollbacks here do not affect correctness of the next epoch — they are
	 * kept only for accounting consistency within the current epoch's lifetime.
	 */
	private fenceAndReinit(
		error: Error,
		allPrepared: Array<{
			batch: PartitionBatch
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		}>
	): void {
		// Guard against re-entrant fencing (e.g. multiple batches in the same
		// ProduceRequest all fail with an ambiguous outcome and each independently
		// calls fenceAndReinit).
		if (this.idempotentState === 'fenced' || this.idempotentState === 'initializing') {
			// Already fencing/re-initializing — just reject the caller's messages.
			for (const p of allPrepared) {
				for (const msg of p.messagesToSend) {
					msg.reject(error)
				}
				this.inflightBatches.delete(p.batchId)
			}
			return
		}

		this.idempotentState = 'fenced'
		this.accumulator.setFenced(true)

		for (const p of allPrepared) {
			for (const msg of p.messagesToSend) {
				msg.reject(error)
			}
			this.rollbackSequence(p.batch.topic, p.batch.partition, p.recordCount)
			this.inflightBatches.delete(p.batchId)
		}

		// Reject and clear any split-remainder batches queued in pendingBatches.
		// These are PartitionBatch objects with raw QueuedMessage arrays — reject
		// their messages so they do not dangle after the epoch changes.
		for (const pendingBatch of this.pendingBatches) {
			for (const msg of pendingBatch.messages) {
				msg.reject(error)
			}
		}
		this.pendingBatches = []

		this.failAllInflightBatches(error)
		this.accumulator.clearWithRejection(error)

		this.initializeIdempotentProducer().catch((reinitError: Error) => {
			this.logger.error('failed to re-initialize after fencing', { error: reinitError.message })
		})
	}

	/**
	 * Send one or more messages to a topic
	 *
	 * Messages are queued locally and batched according to lingerMs
	 * and maxBatchBytes configuration. Returns a Promise that resolves
	 * when all messages have been acknowledged by the broker.
	 *
	 * @param topicDef - Topic definition with encode function
	 * @param messages - Single message or array of messages
	 * @returns SendResult for single message, SendResult[] for array
	 */
	send(topic: string, messages: ProducerMessage<Buffer, Buffer | string>): Promise<SendResult>
	send(topic: string, messages: ProducerMessage<Buffer, Buffer | string>[]): Promise<SendResult[]>
	send<V, K>(topicDef: TopicDefinition<V, K>, messages: ProducerMessage<V, K>): Promise<SendResult>
	send<V, K>(topicDef: TopicDefinition<V, K>, messages: ProducerMessage<V, K>[]): Promise<SendResult[]>
	async send(
		topic: string | TopicDefinition<unknown, unknown>,
		messages: ProducerMessage<unknown, unknown> | ProducerMessage<unknown, unknown>[]
	): Promise<SendResult | SendResult[]> {
		if (this.state !== 'running') {
			throw new Error('Producer is not running')
		}

		// Block send() on transactional producer - must use transaction()
		if (this.config.transactionalId) {
			throw new Error(
				'send() cannot be used with a transactional producer. ' +
					'Use transaction(async (tx) => { await tx.send(...) }) instead.'
			)
		}

		const topicDef =
			typeof topic === 'string'
				? ({
						topic,
					} satisfies TopicDefinition<Buffer, Buffer>)
				: topic

		return this.trackSend(this.internalSend(topicDef, messages))
	}

	/**
	 * Resolve key/value encoders for a topic definition.
	 */
	private resolveEncoders<V, K>(
		topicDef: TopicDefinition<V, K>
	): {
		encodeKey: (key: K | null | undefined) => Buffer | null
		encodeValue: (value: V) => Buffer
	} {
		const encodeKey = (key: K | null | undefined): Buffer | null => {
			if (key === null || key === undefined) {
				return null
			}
			if (topicDef.key) {
				const encoded = topicDef.key.encode(key)
				if (!Buffer.isBuffer(encoded)) {
					throw new Error(`Key codec for '${topicDef.topic}' must return a Buffer`)
				}
				return encoded
			}
			if (Buffer.isBuffer(key)) {
				return key
			}
			if (typeof key === 'string') {
				return Buffer.from(key, 'utf-8')
			}
			throw new Error(`No key codec provided for non-Buffer key in topic '${topicDef.topic}'`)
		}

		const encodeValue = (value: V): Buffer => {
			if (topicDef.value) {
				const encoded = topicDef.value.encode(value)
				if (!Buffer.isBuffer(encoded)) {
					throw new Error(`Value codec for '${topicDef.topic}' must return a Buffer`)
				}
				return encoded
			}
			if (!Buffer.isBuffer(value)) {
				throw new Error(
					`TopicDefinition for '${topicDef.topic}' expects Buffer values when no codec is provided`
				)
			}
			return value
		}

		return { encodeKey, encodeValue }
	}

	/**
	 * Register a send's promise so flush() can wait for it, removing it once settled.
	 * Returns the same promise so callers can `return this.trackSend(...)`. The internal
	 * tracking chain swallows rejections — the caller still holds the promise and observes
	 * any failure itself.
	 */
	private trackSend<T>(promise: Promise<T>): Promise<T> {
		this.activeSends.add(promise)
		const remove = (): void => void this.activeSends.delete(promise)
		void promise.then(remove, remove)
		return promise
	}

	/**
	 * Resolve the target partition for a message, applying the partitioner and sticky
	 * fallback for keyless messages. Throws if the resolved partition is out of range.
	 */
	private resolvePartition<V, K>(
		topic: string,
		msg: ProducerMessage<V, K>,
		keyBuffer: Buffer | null,
		valueBuffer: Buffer | null,
		partitionCount: number
	): number {
		let partition = msg.partition
		if (partition === undefined) {
			partition = this.config.partitioner(topic, keyBuffer, valueBuffer ?? EMPTY_BUFFER, partitionCount)
			if (partition === -1) {
				partition = this.getStickyPartition(topic, partitionCount)
			}
		}
		if (partition < 0 || partition >= partitionCount) {
			throw new Error(`Invalid partition ${partition} for topic ${topic} (has ${partitionCount})`)
		}
		return partition
	}

	/**
	 * Encode and append every message of one send as a single accumulator transaction
	 * (so a multi-message send stays in one batch), returning a per-message promise that
	 * settles when the broker acks or the send fails.
	 */
	private enqueueMessages<V, K>(
		topicDef: TopicDefinition<V, K>,
		msgArray: ProducerMessage<V, K>[],
		partitionCount: number
	): Promise<SendResult>[] {
		const { encodeKey, encodeValue } = this.resolveEncoders(topicDef)

		this.accumulator.beginAppendTransaction()
		const promises = msgArray.map(
			msg =>
				new Promise<SendResult>((resolve, reject) => {
					try {
						const valueBuffer = msg.value === null ? null : encodeValue(msg.value)
						const keyBuffer = encodeKey(msg.key)
						const partition = this.resolvePartition(
							topicDef.topic,
							msg,
							keyBuffer,
							valueBuffer,
							partitionCount
						)

						const headers: Record<string, Buffer> = {}
						if (msg.headers) {
							for (const [k, v] of Object.entries(msg.headers)) {
								headers[k] = Buffer.isBuffer(v) ? v : Buffer.from(v, 'utf-8')
							}
						}

						this.accumulator.append({
							topic: topicDef.topic,
							partition,
							key: keyBuffer,
							value: valueBuffer,
							headers,
							timestamp: BigInt(msg.timestamp?.getTime() ?? Date.now()),
							resolve,
							reject,
						})
					} catch (error) {
						reject(error instanceof Error ? error : new Error(String(error)))
					}
				})
		)
		this.accumulator.endAppendTransaction()
		return promises
	}

	/**
	 * Internal send implementation (bypasses transactional check)
	 */
	private async internalSend<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]> {
		await this.ensureInitialized()

		const msgArray = Array.isArray(messages) ? messages : [messages]
		const isSingle = !Array.isArray(messages)
		const partitionCount = await this.getPartitionCount(topicDef.topic)

		const promises = this.enqueueMessages(topicDef, msgArray, partitionCount)

		const results = await Promise.all(promises)
		return isSingle ? results[0]! : results
	}

	/**
	 * Flush all pending messages
	 *
	 * Forces all queued messages to be sent immediately and waits
	 * for acknowledgment from the broker.
	 */
	async flush(): Promise<void> {
		// Force lingering batches out now instead of waiting for lingerMs, then wait for
		// every outstanding send to settle. allSettled so one failed send doesn't reject
		// flush(); failures surface on the individual send() promises.
		this.accumulator.flush()
		await Promise.allSettled(this.activeSends)
	}

	/**
	 * Disconnect the producer
	 *
	 * Flushes all pending messages before disconnecting.
	 *
	 * @throws Error if a transaction is active - await producer.transaction() first
	 */
	async disconnect(): Promise<void> {
		if (this.state === 'stopping' || this.state === 'idle') {
			return
		}

		// Throw if transaction is active - caller must await transaction() first
		if (this.config.transactionalId && this.transactionState !== 'idle') {
			throw new Error(
				`Cannot disconnect while transaction is in state '${this.transactionState}'. ` +
					`Await producer.transaction() before calling disconnect().`
			)
		}

		this.state = 'stopping'
		this.logger.info('disconnecting producer')

		try {
			await this.flush()
		} catch (error) {
			this.logger.warn('flush() failed during disconnect', {
				error: error instanceof Error ? error.message : String(error),
			})
		}

		// If flush() threw, batches may still be in pendingBatches / inflightBatches /
		// the accumulator. Reject everything so caller promises don't hang.
		const disconnectError = new Error('Producer disconnected')
		for (const batch of this.pendingBatches) {
			for (const msg of batch.messages) {
				msg.reject(disconnectError)
			}
		}
		this.pendingBatches.length = 0
		this.failAllInflightBatches(disconnectError)
		this.accumulator.clearWithRejection(disconnectError)

		this.state = 'idle'
		this.logger.info('producer disconnected')
	}

	// ==================== Transaction API ====================

	/**
	 * Execute operations within a transaction
	 *
	 * The transaction is automatically committed if the function resolves,
	 * or aborted if it throws an error.
	 *
	 * @param fn - Function containing transaction operations
	 * @param options - Transaction options
	 * @returns Result of the transaction function
	 *
	 * @example
	 * ```typescript
	 * await producer.transaction(async (tx) => {
	 *   await tx.send(ordersTopic, { key: orderId, value: order });
	 *   await tx.sendOffsets({
	 *     groupId: 'my-group',
	 *     offsets: [{ topic: 'input', partition: 0, offset: 100n }]
	 *   });
	 * });
	 * ```
	 */
	async transaction<T>(fn: (tx: ProducerTransaction) => Promise<T>, options: TransactionOptions = {}): Promise<T> {
		if (!this.config.transactionalId) {
			throw new Error('transaction() requires transactionalId to be configured')
		}

		if (this.transactionState !== 'idle') {
			throw new InvalidTxnStateError(this.config.transactionalId, this.transactionState, ['idle'])
		}

		await this.ensureInitialized()

		this.transactionState = 'in_transaction'
		this.partitionsInTransaction.clear()
		this.offsetsToCommit.clear()

		const timeoutMs = options.timeoutMs ?? this.config.transactionTimeoutMs

		const abortController = new AbortController()
		let timeoutId: ReturnType<typeof setTimeout> | null = null

		const tx: ProducerTransaction = {
			signal: abortController.signal,

			send: async (
				topic: string | TopicDefinition<unknown, unknown>,
				messages: ProducerMessage<unknown, unknown> | ProducerMessage<unknown, unknown>[]
			): Promise<SendResult | SendResult[]> => {
				if (this.transactionState !== 'in_transaction') {
					throw new InvalidTxnStateError(this.config.transactionalId!, this.transactionState, [
						'in_transaction',
					])
				}

				const topicDef =
					typeof topic === 'string'
						? ({
								topic,
							} satisfies TopicDefinition<Buffer, Buffer>)
						: topic

				return this.trackSend(this.transactionalSend(topicDef, messages))
			},

			sendOffsets: async (params: SendOffsetsParams): Promise<void> => {
				if (this.transactionState !== 'in_transaction') {
					throw new InvalidTxnStateError(this.config.transactionalId!, this.transactionState, [
						'in_transaction',
					])
				}
				await this.prepareOffsetsForCommit(params)
			},
		}

		try {
			const result = await Promise.race([
				fn(tx),
				new Promise<never>((_, reject) => {
					timeoutId = setTimeout(() => {
						abortController.abort()
						reject(new Error(`Transaction timeout after ${timeoutMs}ms`))
					}, timeoutMs)
				}),
			])

			if (timeoutId) clearTimeout(timeoutId)

			await this.commitTransaction()
			return result as T
		} catch (error) {
			if (timeoutId) clearTimeout(timeoutId)

			abortController.abort()
			await this.abortTransaction()

			if (error instanceof TransactionAbortedError) {
				throw error
			}
			throw new TransactionAbortedError(
				this.config.transactionalId,
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	/**
	 * Send within a transaction - adds partition to transaction first
	 */
	private async transactionalSend<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]> {
		const isSingle = !Array.isArray(messages)
		const msgArray = Array.isArray(messages) ? messages : [messages]

		const { encodeKey, encodeValue } = this.resolveEncoders(topicDef)

		// Resolve partitions once and freeze them on the message. Re-invoking a
		// stateful partitioner (e.g. round-robin) in internalSend would advance
		// the counter, so addPartitionsToTxn and the actual writes would diverge.
		const partitionCount = await this.getPartitionCount(topicDef.topic)
		const partitionsToAdd = new Set<number>()
		const resolvedMessages: Array<ProducerMessage<V, K> & { partition: number }> = msgArray.map(msg => {
			let partition = msg.partition
			if (partition === undefined) {
				const valueBuffer = msg.value === null ? null : encodeValue(msg.value)
				const partitionValue = valueBuffer ?? EMPTY_BUFFER
				const keyBuffer = encodeKey(msg.key)
				partition = this.config.partitioner(topicDef.topic, keyBuffer, partitionValue, partitionCount)
				if (partition === -1) {
					partition = this.getStickyPartition(topicDef.topic, partitionCount)
				}
			}
			const key = `${topicDef.topic}:${partition}`
			if (!this.partitionsInTransaction.has(key)) {
				partitionsToAdd.add(partition)
			}
			return { ...msg, partition }
		})

		if (partitionsToAdd.size > 0) {
			await this.addPartitionsToTransaction(topicDef.topic, Array.from(partitionsToAdd))
		}

		return this.internalSend(topicDef, isSingle ? resolvedMessages[0]! : resolvedMessages)
	}

	/**
	 * Add partitions to the current transaction
	 */
	private async addPartitionsToTransaction(topic: string, partitions: number[]): Promise<void> {
		let coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)

		await retry(
			async () => {
				const response = await coordinator.addPartitionsToTxn({
					transactionalId: this.config.transactionalId!,
					producerId: this.producerId,
					producerEpoch: this.producerEpoch,
					topics: [
						{
							name: topic,
							partitions,
						},
					],
				})

				// A top-level error (v4+) means the broker rejected the whole request; the
				// per-partition results are empty/meaningless, so check it before they are.
				if (response.errorCode !== ErrorCode.None) {
					if (isCoordinatorErrorCode(response.errorCode)) {
						this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
						coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
					}

					throw new KafkaProtocolError(
						response.errorCode,
						`AddPartitionsToTxn failed for transaction ${this.config.transactionalId}`
					)
				}

				for (const topicResult of response.results) {
					for (const partitionResult of topicResult.resultsByPartition) {
						if (partitionResult.errorCode === ErrorCode.None) {
							continue
						}

						if (isCoordinatorErrorCode(partitionResult.errorCode)) {
							this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
							coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
						}

						throw new KafkaProtocolError(
							partitionResult.errorCode,
							`AddPartitionsToTxn failed for ${topicResult.name}-${partitionResult.partitionIndex}`
						)
					}
				}

				// Success - mark partitions as part of this transaction
				for (const partition of partitions) {
					this.partitionsInTransaction.add(`${topic}:${partition}`)
				}

				this.logger.debug('added partitions to transaction', {
					transactionalId: this.config.transactionalId,
					topic,
					partitions,
				})
			},
			{
				...createRetryStrategyOptions(this.config),
				shouldRetry: error => error instanceof KafkaProtocolError && error.retriable,
			}
		)
	}

	/**
	 * Prepare offsets to be committed with the transaction
	 */
	private async prepareOffsetsForCommit(params: SendOffsetsParams): Promise<void> {
		// Extract groupId from either groupId or consumerGroupMetadata
		const groupId = params.consumerGroupMetadata?.groupId ?? params.groupId
		if (!groupId) {
			throw new Error('sendOffsets requires either groupId or consumerGroupMetadata')
		}

		let coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)

		await retry(
			async () => {
				// Register the group with the transaction coordinator
				const response = await coordinator.addOffsetsToTxn({
					transactionalId: this.config.transactionalId!,
					producerId: this.producerId,
					producerEpoch: this.producerEpoch,
					groupId,
				})

				if (response.errorCode === ErrorCode.None) {
					return
				}

				// Handle coordinator move - invalidate cache and re-fetch
				if (isCoordinatorErrorCode(response.errorCode)) {
					this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
					coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
				}

				throw new KafkaProtocolError(response.errorCode, `AddOffsetsToTxn failed for group ${groupId}`)
			},
			{
				...createRetryStrategyOptions(this.config),
				shouldRetry: error => error instanceof KafkaProtocolError && error.retriable,
			}
		)

		// Store offsets and group metadata for commit during transaction commit phase
		const existing = this.offsetsToCommit.get(groupId)
		this.offsetsToCommit.set(groupId, {
			offsets: [...(existing?.offsets ?? []), ...params.offsets],
			groupMetadata: params.consumerGroupMetadata ?? existing?.groupMetadata,
		})

		this.logger.debug('prepared offsets for transaction commit', {
			transactionalId: this.config.transactionalId,
			groupId,
			offsetCount: params.offsets.length,
			hasGroupMetadata: !!params.consumerGroupMetadata,
		})
	}

	/**
	 * Commit the current transaction
	 */
	private async commitTransaction(): Promise<void> {
		if (this.transactionState !== 'in_transaction') {
			throw new InvalidTxnStateError(this.config.transactionalId!, this.transactionState, ['in_transaction'])
		}

		this.transactionState = 'committing'

		try {
			// Flush any pending messages to ensure all are sent
			await this.flush()

			// Commit offsets for each consumer group
			for (const [groupId, { offsets, groupMetadata }] of this.offsetsToCommit) {
				await this.commitTransactionOffsets(groupId, offsets, groupMetadata)
			}

			// End transaction with commit (with retry for transient errors)
			let coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)

			await retry(
				async () => {
					const response = await coordinator.endTxn({
						transactionalId: this.config.transactionalId!,
						producerId: this.producerId,
						producerEpoch: this.producerEpoch,
						committed: true,
					})

					if (response.errorCode === ErrorCode.None) {
						return
					}

					// Handle coordinator move - invalidate cache and re-fetch
					if (isCoordinatorErrorCode(response.errorCode)) {
						this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
						coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
					}

					throw new KafkaProtocolError(response.errorCode, 'EndTxn commit failed')
				},
				{
					...createRetryStrategyOptions(this.config),
					shouldRetry: error => error instanceof KafkaProtocolError && error.retriable,
				}
			)

			this.logger.info('transaction committed', {
				transactionalId: this.config.transactionalId,
				partitionCount: this.partitionsInTransaction.size,
				groupCount: this.offsetsToCommit.size,
			})
			this.transactionState = 'idle'
			this.partitionsInTransaction.clear()
			this.offsetsToCommit.clear()
		} catch (error) {
			this.transactionState = 'committing'
			throw error
		}
	}

	/**
	 * Abort the current transaction
	 */
	private async abortTransaction(): Promise<void> {
		if (this.transactionState !== 'in_transaction' && this.transactionState !== 'committing') {
			// Already idle or in error state
			return
		}

		this.transactionState = 'aborting'

		try {
			const abortError = new TransactionAbortedError(this.config.transactionalId!)
			this.accumulator.clearWithRejection(abortError)

			const coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
			const response = await coordinator.endTxn({
				transactionalId: this.config.transactionalId!,
				producerId: this.producerId,
				producerEpoch: this.producerEpoch,
				committed: false,
			})

			if (response.errorCode !== ErrorCode.None) {
				const error = new KafkaProtocolError(response.errorCode, 'EndTxn abort failed')
				this.logger.error('EndTxn abort failed', { errorCode: response.errorCode })
				// Emit error so operators can monitor abort failures
				this.emitError(error)
				// Don't throw - abort should succeed even if RPC fails
			}

			this.logger.info('transaction aborted', {
				transactionalId: this.config.transactionalId,
			})
		} finally {
			this.transactionState = 'idle'
			this.partitionsInTransaction.clear()
			this.offsetsToCommit.clear()
		}
	}

	/**
	 * Commit offsets as part of transaction using TxnOffsetCommit
	 */
	private async commitTransactionOffsets(
		groupId: string,
		offsets: TopicPartitionOffset[],
		groupMetadata?: GroupMetadata
	): Promise<void> {
		// Deduplicate offsets - keep highest offset per (topic, partition)
		const deduped = new Map<string, TopicPartitionOffset>()
		for (const offset of offsets) {
			const key = `${offset.topic}:${offset.partition}`
			const existing = deduped.get(key)
			if (!existing || offset.offset > existing.offset) {
				deduped.set(key, offset)
			}
		}

		// Group deduplicated offsets by topic
		const topicMap = new Map<
			string,
			Array<{
				partitionIndex: number
				committedOffset: bigint
				committedLeaderEpoch?: number
				committedMetadata?: string | null
			}>
		>()

		for (const offset of deduped.values()) {
			const partitions = topicMap.get(offset.topic) ?? []
			partitions.push({
				partitionIndex: offset.partition,
				committedOffset: offset.offset,
				committedLeaderEpoch: offset.leaderEpoch,
				committedMetadata: offset.metadata ?? null,
			})
			topicMap.set(offset.topic, partitions)
		}

		// Use group metadata if provided (EOS with consumer), otherwise producer-only defaults
		const generationId = groupMetadata?.generationId ?? -1
		const memberId = groupMetadata?.memberId ?? ''
		const groupInstanceId = groupMetadata?.groupInstanceId ?? null

		// TxnOffsetCommit goes to the GROUP coordinator (not transaction coordinator)
		let groupCoordinator = await this.cluster.getCoordinator('GROUP', groupId)

		await retry(
			async () => {
				const response = await groupCoordinator.txnOffsetCommit({
					transactionalId: this.config.transactionalId!,
					groupId,
					producerId: this.producerId,
					producerEpoch: this.producerEpoch,
					generationId,
					memberId,
					groupInstanceId,
					topics: Array.from(topicMap.entries()).map(([name, partitions]) => ({
						name,
						partitions,
					})),
				})

				for (const topic of response.topics) {
					for (const partition of topic.partitions) {
						if (partition.errorCode === ErrorCode.None) continue

						if (isCoordinatorErrorCode(partition.errorCode)) {
							this.cluster.invalidateCoordinator('GROUP', groupId)
							groupCoordinator = await this.cluster.getCoordinator('GROUP', groupId)
						}

						throw new KafkaProtocolError(
							partition.errorCode,
							`TxnOffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
						)
					}
				}
			},
			{
				...createRetryStrategyOptions(this.config),
				shouldRetry: error => error instanceof KafkaProtocolError && error.retriable,
			}
		)
	}

	/**
	 * Get partition count for a topic (cached with TTL)
	 */
	private async getPartitionCount(topic: string): Promise<number> {
		const cached = this.partitionCache.get(topic)
		const now = Date.now()

		// Return cached value if fresh
		if (cached && now - cached.fetchedAt < Producer.PARTITION_CACHE_TTL_MS) {
			return cached.count
		}

		// Topic creation is asynchronous; even after CreateTopics returns successfully, the topic may not be
		// immediately visible in Metadata. Retry a few times before failing.
		const maxAttempts = 5
		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			const metadata = await this.cluster.refreshMetadata([topic])
			const topicMeta = metadata.topics.get(topic)
			if (topicMeta) {
				const count = topicMeta.partitions.size
				this.partitionCache.set(topic, { count, fetchedAt: Date.now() })
				return count
			}

			if (attempt < maxAttempts) {
				await this.sleep(100 * attempt)
			}
		}

		throw new Error(`Topic ${topic} not found`)
	}

	/**
	 * Get sticky partition for keyless messages
	 */
	private getStickyPartition(topic: string, partitionCount: number): number {
		const state = this.stickyPartitions.get(topic)
		if (!state) {
			// Random initial partition
			const partition = Math.floor(Math.random() * partitionCount)
			this.stickyPartitions.set(topic, { partition, partitionCount })
			return partition
		}

		// If partition count changed, keep the current partition within the new range.
		if (state.partitionCount !== partitionCount) {
			const nextPartition = state.partition % partitionCount
			this.stickyPartitions.set(topic, { partition: nextPartition, partitionCount })
			return nextPartition
		}

		return state.partition
	}

	/**
	 * Rotate sticky partition (call after batch completes)
	 */
	private rotateStickyPartition(topic: string): void {
		const state = this.stickyPartitions.get(topic)
		if (!state) return
		this.stickyPartitions.set(topic, {
			partition: (state.partition + 1) % state.partitionCount,
			partitionCount: state.partitionCount,
		})
	}

	/**
	 * Ensure idempotent producer is initialized before sending
	 *
	 * This is a no-op for non-idempotent producers.
	 * For idempotent producers, it waits for initialization to complete.
	 */
	private async ensureInitialized(): Promise<void> {
		if (!this.config.idempotent) {
			return
		}

		// If already running, we're good
		if (this.idempotentState === 'running') {
			return
		}

		// If in error state, fail immediately
		if (this.idempotentState === 'error') {
			throw new Error('Idempotent producer is in error state')
		}

		// If initializing, wait for existing promise
		if (this.idempotentState === 'initializing' && this.initPromise) {
			await this.initPromise
			return
		}

		// If fenced, trigger re-initialization
		if (this.idempotentState === 'fenced') {
			await this.initializeIdempotentProducer()
			return
		}

		// If idle, start initialization
		if (this.idempotentState === 'idle') {
			await this.initializeIdempotentProducer()
		}
	}

	/**
	 * Initialize idempotent producer by obtaining producer ID from broker
	 *
	 * Transitions: idle/fenced -> initializing -> running (or error)
	 */
	private async initializeIdempotentProducer(): Promise<void> {
		this.idempotentState = 'initializing'
		this.logger.debug('initializing idempotent producer')

		this.initPromise = this.doInitializeIdempotentProducer()

		try {
			await this.initPromise
		} finally {
			this.initPromise = null
		}
	}

	/**
	 * Actual initialization logic - separated for proper promise handling
	 */
	private async doInitializeIdempotentProducer(): Promise<void> {
		// Retry coordinator discovery + InitProducerId until maxBlockMs, riding through a loading or
		// briefly-unreachable coordinator. Non-retriable protocol errors fail fast.
		const deadlineAt = Date.now() + this.config.maxBlockMs
		const backoff = new ReconnectionStrategy({
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
			maxAttempts: Number.MAX_SAFE_INTEGER,
		})
		let lastError: Error = new Error('InitProducerId timed out before the coordinator was ready')

		while (Date.now() < deadlineAt) {
			let response: Awaited<ReturnType<Broker['initProducerId']>>
			try {
				let broker: Broker

				if (this.config.transactionalId) {
					// Transactional: must use transaction coordinator
					broker = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId)
				} else {
					// Non-transactional: any broker works
					broker = await (this.cluster as unknown as { getAnyBroker: () => Promise<Broker> }).getAnyBroker()
				}

				response = await broker.initProducerId({
					transactionalId: this.config.transactionalId,
					transactionTimeoutMs: this.config.transactionalId ? this.config.transactionTimeoutMs : 0,
					producerId: this.producerId !== -1n ? this.producerId : undefined,
					producerEpoch: this.producerEpoch !== -1 ? this.producerEpoch : undefined,
				})
			} catch (error) {
				// Transport failure: re-discover the coordinator and retry until the deadline.
				if (this.config.transactionalId) {
					this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId)
				}
				lastError = error instanceof Error ? error : new Error(String(error))
				const delay = backoff.nextDelay()
				backoff.recordFailure()
				await this.sleep(delay)
				continue
			}

			// Protocol-level error handling — outside the try so a non-retriable code escapes the
			// loop instead of being re-caught and treated as a transport failure.
			if (response.errorCode !== ErrorCode.None) {
				const errorCode = response.errorCode

				if (isRetriableError(errorCode)) {
					// If our transaction coordinator moved, invalidate so the next attempt re-discovers it.
					if (
						this.config.transactionalId &&
						(errorCode === ErrorCode.NotCoordinator || errorCode === ErrorCode.CoordinatorNotAvailable)
					) {
						this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId)
					}
					lastError = new Error(`InitProducerId failed: ${ErrorCode[errorCode] ?? errorCode}`)
					const delay = backoff.nextDelay()
					backoff.recordFailure()
					await this.sleep(delay)
					continue
				}

				// CLUSTER_AUTHORIZATION_FAILED or other non-retriable errors — fail fast.
				this.idempotentState = 'error'
				throw new Error(`InitProducerId failed: ${ErrorCode[errorCode] ?? errorCode}`)
			}

			// Success - store producer identity
			this.producerId = response.producerId
			this.producerEpoch = response.producerEpoch

			// Reset sequences on (re)initialization
			this.sequences.clear()
			this.reservedSequences.clear()

			// Reset transaction state on (re)initialization
			this.transactionState = 'idle'
			this.partitionsInTransaction.clear()
			this.offsetsToCommit.clear()

			this.accumulator.setFenced(false)

			this.idempotentState = 'running'
			this.logger.info('idempotent producer initialized', {
				producerId: this.producerId.toString(),
				producerEpoch: this.producerEpoch,
				transactionalId: this.config.transactionalId,
			})
			return
		}

		this.idempotentState = 'error'
		throw lastError
	}

	/**
	 * Reserve sequence numbers for a batch (called before send)
	 *
	 * With multiple in-flight batches, we must reserve sequence ranges upfront
	 * to prevent duplicate sequences. The reserved count is committed on success
	 * or rolled back on fatal failure.
	 *
	 * If the batch would cross the sequence wrap boundary (2^31-1 -> 0), we only
	 * reserve up to the boundary to prevent OUT_OF_ORDER_SEQUENCE_NUMBER errors.
	 * The caller must split the batch and handle the remainder separately.
	 *
	 * @returns Object with baseSequence and actualCount (may be less than recordCount at wrap)
	 */
	private reserveSequence(
		topic: string,
		partition: number,
		recordCount: number
	): { baseSequence: number; reservedCount: number } {
		const key = `${topic}:${partition}`
		const committed = this.sequences.get(key) ?? 0
		const reserved = this.reservedSequences.get(key) ?? 0
		const baseSequence = (committed + reserved) & 0x7fffffff

		const MAX_SEQUENCE = 0x7fffffff

		// Check if batch would cross the wrap boundary
		// Sequences within a single batch must be contiguous without wrapping
		let reservedCount = recordCount
		if (baseSequence + recordCount - 1 > MAX_SEQUENCE) {
			// Only reserve up to the boundary
			reservedCount = MAX_SEQUENCE - baseSequence + 1
			this.logger.info('sequence wrap - splitting batch', {
				topic,
				partition,
				baseSequence,
				requestedCount: recordCount,
				reservedCount,
				remainingCount: recordCount - reservedCount,
			})
		}

		// Reserve the range
		this.reservedSequences.set(key, reserved + reservedCount)
		return { baseSequence, reservedCount }
	}

	/**
	 * Commit reserved sequences after successful send
	 *
	 * Moves the reserved count to committed and advances the sequence.
	 */
	private commitSequence(topic: string, partition: number, recordCount: number): void {
		const key = `${topic}:${partition}`
		const committed = this.sequences.get(key) ?? 0
		const reserved = this.reservedSequences.get(key) ?? 0

		// Advance committed sequence
		const next = (committed + recordCount) & 0x7fffffff
		this.sequences.set(key, next)

		// Reduce reserved count
		this.reservedSequences.set(key, Math.max(0, reserved - recordCount))
	}

	/**
	 * Rollback reserved sequences on fatal failure
	 *
	 * Called when a batch fails with a non-retriable error or fencing.
	 * Does NOT roll back on retriable errors - the batch will retry with same sequence.
	 */
	private rollbackSequence(topic: string, partition: number, recordCount: number): void {
		const key = `${topic}:${partition}`
		const reserved = this.reservedSequences.get(key) ?? 0
		this.reservedSequences.set(key, Math.max(0, reserved - recordCount))
	}

	/**
	 * Fail all in-flight batches due to fencing
	 *
	 * Called when producer is fenced to reject all pending batches,
	 * not just the one that triggered the fencing error.
	 */
	private failAllInflightBatches(error: Error): void {
		for (const [, batch] of this.inflightBatches) {
			// Reject all messages in the batch
			for (const msg of batch.messages) {
				msg.reject(error)
			}
			// Rollback reserved sequences
			this.rollbackSequence(batch.topic, batch.partition, batch.recordCount)
		}
		this.inflightBatches.clear()
	}

	/**
	 * Acquire an in-flight slot
	 *
	 * Waits until an in-flight slot is available, enforcing maxInFlight at producer level.
	 */
	private async acquireInflightSlot(): Promise<void> {
		// Spin-wait until we have a slot available
		while (this.inflightCount >= this.config.maxInFlight) {
			await this.sleep(1)
		}
		this.inflightCount++
	}

	/**
	 * Release an in-flight slot
	 */
	private releaseInflightSlot(): void {
		this.inflightCount = Math.max(0, this.inflightCount - 1)
	}

	/**
	 * Resolve user config to internal config
	 */
	private resolveConfig(config: ProducerConfig): ResolvedProducerConfig {
		// Transactional mode implies idempotent mode
		const isTransactional = config.transactionalId !== undefined
		const idempotent = isTransactional ? true : (config.idempotent ?? DEFAULT_CONFIG.idempotent)

		// Validate transactional constraints
		if (isTransactional) {
			// Transactional producer requires idempotent mode
			if (config.idempotent === false) {
				throw new Error('Transactional producer requires idempotent=true (or leave unset)')
			}
		}

		// Validate idempotent mode constraints
		if (idempotent) {
			// Idempotent producer requires acks='all'
			if (config.acks !== undefined && config.acks !== 'all') {
				throw new Error('Idempotent producer requires acks="all"')
			}

			// Idempotent producer requires retries >= 1
			if (config.retries !== undefined && config.retries < 1) {
				throw new Error('Idempotent producer requires retries >= 1')
			}

			// Idempotent producer requires maxInFlight <= 5 (Kafka protocol limit)
			if (config.maxInFlight !== undefined && config.maxInFlight > 5) {
				throw new Error('Idempotent producer requires maxInFlight <= 5')
			}
		}

		const acks = this.resolveAcks(config.acks ?? DEFAULT_CONFIG.acks)
		const compression = this.resolveCompression(config.compression ?? DEFAULT_CONFIG.compression)
		const partitioner = this.resolvePartitioner(config.partitioner ?? DEFAULT_CONFIG.partitioner)

		// For idempotent mode, ensure proper defaults
		const retries = config.retries ?? DEFAULT_CONFIG.retries
		const maxInFlight = config.maxInFlight ?? DEFAULT_CONFIG.maxInFlight

		return {
			lingerMs: config.lingerMs ?? DEFAULT_CONFIG.lingerMs,
			maxBatchBytes: config.maxBatchBytes ?? DEFAULT_CONFIG.maxBatchBytes,
			acks,
			retries: idempotent ? Math.max(retries, 1) : retries,
			retryBackoffMs: config.retryBackoffMs ?? DEFAULT_CONFIG.retryBackoffMs,
			maxRetryBackoffMs: config.maxRetryBackoffMs ?? DEFAULT_CONFIG.maxRetryBackoffMs,
			compression,
			partitioner,
			requestTimeoutMs: config.requestTimeoutMs ?? DEFAULT_CONFIG.requestTimeoutMs,
			transactionalId: config.transactionalId ?? null,
			idempotent,
			maxInFlight: idempotent ? Math.min(maxInFlight, 5) : maxInFlight,
			transactionTimeoutMs: config.transactionTimeoutMs ?? 60000,
			maxBlockMs: config.maxBlockMs ?? DEFAULT_CONFIG.maxBlockMs,
		}
	}

	private resolveAcks(acks: Acks): number {
		switch (acks) {
			case 'all':
				return -1
			case 'leader':
				return 1
			case 'none':
				return 0
		}
	}

	private resolveCompression(compression: CompressionName): CompressionType {
		switch (compression) {
			case 'none':
				return CompressionType.None
			case 'gzip':
				return CompressionType.Gzip
			case 'snappy':
				return CompressionType.Snappy
			case 'lz4':
				return CompressionType.Lz4
			case 'zstd':
				return CompressionType.Zstd
		}
	}

	private resolvePartitioner(partitioner: Partitioner): PartitionerFunction {
		if (typeof partitioner === 'function') {
			return partitioner
		}
		switch (partitioner) {
			case 'murmur2':
				return murmur2Partitioner
			case 'round-robin':
				return createRoundRobinPartitioner()
		}
	}

	private emitError(error: Error): void {
		if (this.listenerCount('error') > 0) {
			this.emit('error', error)
			return
		}
		this.logger.error('producer error', { error: error.message })
	}

	private sleep(ms: number): Promise<void> {
		return sleepMs(ms)
	}
}
