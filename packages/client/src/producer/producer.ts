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
	KafkaError,
	ProducerFencedError,
	InvalidTxnStateError,
	TransactionAbortedError,
	KafkaProtocolError,
} from '@/client/errors.js'
import { createRecordBatch, encodeRecordBatch, encodeRecordBatchSync } from '@/protocol/records/record-batch.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { ReconnectionStrategy } from '@/network/reconnection.js'
import { noopLogger, type Logger } from '@/logger.js'
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

		// Group batches by topic:partition - only take first batch per partition
		// to maintain ordering (remaining batches wait for next drain)
		const batchesByPartition = new Map<string, PartitionBatch[]>()
		const firstBatchPerPartition: PartitionBatch[] = []

		for (const batch of batches) {
			const key = `${batch.topic}:${batch.partition}`
			const existing = batchesByPartition.get(key)
			if (!existing) {
				// First batch for this partition - include in this drain
				batchesByPartition.set(key, [batch])
				firstBatchPerPartition.push(batch)
			} else {
				// Additional batch for same partition - defer to next drain
				existing.push(batch)
			}
		}

		// Re-queue deferred batches (those after the first for each partition)
		for (const [, partitionBatches] of batchesByPartition) {
			for (let i = 1; i < partitionBatches.length; i++) {
				this.pendingBatches.push(partitionBatches[i]!)
			}
		}

		// Schedule another drain if there are deferred batches
		if (this.pendingBatches.length > 0) {
			this.scheduleDrain()
		}

		// Now send first batch per partition, grouped by broker
		if (firstBatchPerPartition.length > 0) {
			const promise = this.sendBatchesGroupedByBroker(firstBatchPerPartition)
			this.inflight.add(promise)
			promise.finally(() => {
				this.inflight.delete(promise)
			})
		}
	}

	/**
	 * Send batches grouped by broker
	 *
	 * Groups the given batches by their leader broker and sends
	 * one ProduceRequest per broker containing all partitions.
	 */
	private async sendBatchesGroupedByBroker(batches: PartitionBatch[]): Promise<void> {
		const topics = Array.from(new Set(batches.map(b => b.topic)))

		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		let lastError: unknown

		while (strategy.shouldReconnect()) {
			try {
				// Look up leader for each batch and group by broker
				const batchesByBroker = new Map<Broker, PartitionBatch[]>()

				for (const batch of batches) {
					const leader = await this.cluster.getLeaderForPartition(batch.topic, batch.partition)
					const existing = batchesByBroker.get(leader) ?? []
					existing.push(batch)
					batchesByBroker.set(leader, existing)
				}

				// Send one request per broker in parallel.
				// We use allSettled to ensure we don't emit unhandled rejections here; individual messages will be
				// resolved/rejected by sendBrokerBatches().
				const sendPromises: Promise<void>[] = []
				for (const [broker, brokerBatches] of batchesByBroker) {
					sendPromises.push(this.sendBrokerBatches(broker, brokerBatches))
				}

				await Promise.allSettled(sendPromises)
				return
			} catch (error) {
				lastError = error
				strategy.recordFailure()

				if (error instanceof KafkaError && shouldRefreshMetadata(error.errorCode)) {
					await this.cluster.refreshMetadata(topics)
					for (const topic of topics) {
						this.partitionCache.delete(topic)
					}
				}

				if (!strategy.shouldReconnect()) {
					break
				}

				await this.sleep(strategy.nextDelay())
			}
		}

		const finalError =
			lastError instanceof Error ? lastError : new Error(`Failed to send batches: ${String(lastError)}`)

		// We failed before we could hand the batches to sendBrokerBatches(), so reject all message promises and
		// mark the batches as completed to unblock flush/drain.
		for (const batch of batches) {
			for (const msg of batch.messages) {
				msg.reject(finalError)
			}
			this.accumulator.batchCompleted()
		}
	}

	/**
	 * Send multiple partition batches to a single broker in one ProduceRequest
	 */
	private async sendBrokerBatches(broker: Broker, batches: PartitionBatch[]): Promise<void> {
		await this.acquireInflightSlot()

		try {
			await this.doSendBrokerBatches(broker, batches)
		} finally {
			this.releaseInflightSlot()
			for (let i = 0; i < batches.length; i++) {
				this.accumulator.batchCompleted()
			}
		}
	}

	/**
	 * Internal implementation of broker batch send
	 */
	private async doSendBrokerBatches(broker: Broker, batches: PartitionBatch[]): Promise<void> {
		// Prepare all batches: reserve sequences, encode record batches
		const preparedBatches: Array<{
			batch: PartitionBatch
			encodedBatch: Buffer
			baseSequence: number
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		}> = []

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
					this.pendingBatches.push(remainingBatch)
					this.scheduleDrain()
					recordCount = reservation.reservedCount
				}
			}

			const batchId = Symbol('batch')
			if (this.config.idempotent) {
				this.inflightBatches.set(batchId, { topic, partition, messages: messagesToSend, recordCount })
			}

			const records = messagesToSend.map(msg => ({
				key: msg.key,
				value: msg.value,
				headers: Object.fromEntries(Object.entries(msg.headers).map(([k, v]) => [k, v])),
				timestamp: Number(msg.timestamp),
			}))

			const baseTimestamp =
				messagesToSend.length > 0
					? messagesToSend.reduce(
							(min, msg) => (msg.timestamp < min ? msg.timestamp : min),
							messagesToSend[0]!.timestamp
						)
					: undefined

			const encodeStart = performance.now()
			const recordBatch = createRecordBatch(
				records,
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

		if (preparedBatches.length === 0) return

		const topicMap = new Map<string, Array<{ partitionIndex: number; records: Buffer }>>()
		for (const { batch, encodedBatch } of preparedBatches) {
			const partitions = topicMap.get(batch.topic) ?? []
			partitions.push({ partitionIndex: batch.partition, records: encodedBatch })
			topicMap.set(batch.topic, partitions)
		}

		const request: ProduceRequest = {
			transactionalId: this.config.transactionalId,
			acks: this.config.acks,
			timeoutMs: this.config.requestTimeoutMs,
			topics: Array.from(topicMap.entries()).map(([name, partitions]) => ({
				name,
				partitions,
			})),
		}

		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		while (strategy.shouldReconnect()) {
			try {
				const requestStart = performance.now()
				const response = await broker.produce(request)
				const requestEnd = performance.now()
				if (this.trace) {
					const totalRecords = preparedBatches.reduce((sum, p) => sum + p.recordCount, 0)
					const totalBytes = preparedBatches.reduce((sum, p) => sum + p.encodedBatch.length, 0)
					this.trace({
						stage: 'produce_request',
						durationMs: requestEnd - requestStart,
						recordCount: totalRecords,
						bytes: totalBytes,
						brokerId: broker.nodeId,
					})
				}

				let hasRetriableError = false
				const batchesToRetry: typeof preparedBatches = []

				for (const prepared of preparedBatches) {
					const { batch, recordCount, batchId, messagesToSend } = prepared
					const topicResponse = response.topics.find(t => t.name === batch.topic)
					const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === batch.partition)

					if (!partitionResponse) {
						const error = new Error(`No response for ${batch.topic}-${batch.partition}`)
						for (const msg of messagesToSend) {
							msg.reject(error)
						}
						if (this.config.idempotent) {
							this.rollbackSequence(batch.topic, batch.partition, recordCount)
							this.inflightBatches.delete(batchId)
						}
						continue
					}

					if (partitionResponse.errorCode !== ErrorCode.None) {
						const errorCode = partitionResponse.errorCode

						// Handle fencing errors
						if (
							this.config.idempotent &&
							(errorCode === ErrorCode.ProducerFenced || errorCode === ErrorCode.InvalidProducerEpoch)
						) {
							this.handleFencingError(prepared, preparedBatches)
							return
						}

						// Handle duplicate sequence (treat as success)
						if (this.config.idempotent && errorCode === ErrorCode.DuplicateSequenceNumber) {
							this.handleDuplicateSequence(prepared, partitionResponse)
							continue
						}

						// Handle out-of-order sequence
						if (this.config.idempotent && errorCode === ErrorCode.OutOfOrderSequenceNumber) {
							this.handleOutOfOrderSequence(prepared)
							continue
						}

						// Handle retriable errors
						if (isRetriableError(errorCode)) {
							hasRetriableError = true
							batchesToRetry.push(prepared)
							if (shouldRefreshMetadata(errorCode)) {
								await this.cluster.refreshMetadata([batch.topic])
								this.partitionCache.delete(batch.topic)
							}
							continue
						}

						// Non-retriable error
						if (this.config.idempotent) {
							this.rollbackSequence(batch.topic, batch.partition, recordCount)
							this.inflightBatches.delete(batchId)
						}
						const error = new Error(
							`Produce failed for ${batch.topic}-${batch.partition}: ${ErrorCode[errorCode] ?? errorCode}`
						)
						for (const msg of messagesToSend) {
							msg.reject(error)
						}
						continue
					}

					// Success
					this.handleBatchSuccess(prepared, partitionResponse)
				}

				// If no retriable errors, we're done
				if (!hasRetriableError || batchesToRetry.length === 0) {
					return
				}

				// Retry only the failed batches
				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())

					// Rebuild request with only failed batches
					const retryTopicMap = new Map<string, Array<{ partitionIndex: number; records: Buffer }>>()
					for (const { batch, encodedBatch } of batchesToRetry) {
						const partitions = retryTopicMap.get(batch.topic) ?? []
						partitions.push({ partitionIndex: batch.partition, records: encodedBatch })
						retryTopicMap.set(batch.topic, partitions)
					}

					request.topics = Array.from(retryTopicMap.entries()).map(([name, partitions]) => ({
						name,
						partitions,
					}))

					// Update preparedBatches to only contain retries for next iteration
					preparedBatches.length = 0
					preparedBatches.push(...batchesToRetry)
					continue
				}

				// Max retries exceeded for remaining batches
				for (const prepared of batchesToRetry) {
					const { batch, recordCount, batchId, messagesToSend } = prepared
					if (this.config.idempotent) {
						this.rollbackSequence(batch.topic, batch.partition, recordCount)
						this.inflightBatches.delete(batchId)
					}
					const error = new Error(
						`Produce failed for ${batch.topic}-${batch.partition}: max retries exceeded`
					)
					for (const msg of messagesToSend) {
						msg.reject(error)
					}
				}
				return
			} catch (error) {
				strategy.recordFailure()

				if (!strategy.shouldReconnect()) {
					// Max retries exceeded
					for (const prepared of preparedBatches) {
						const { batch, recordCount, batchId, messagesToSend } = prepared
						if (this.config.idempotent) {
							this.rollbackSequence(batch.topic, batch.partition, recordCount)
							this.inflightBatches.delete(batchId)
						}
						for (const msg of messagesToSend) {
							msg.reject(error instanceof Error ? error : new Error(String(error)))
						}
					}
					return
				}

				await this.sleep(strategy.nextDelay())
			}
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
	 * Handle duplicate sequence error (treat as success)
	 */
	private handleDuplicateSequence(
		prepared: {
			batch: PartitionBatch
			baseSequence: number
			recordCount: number
			batchId: symbol
			messagesToSend: QueuedMessage[]
		},
		partitionResponse: { baseOffset: bigint; logAppendTimeMs: bigint }
	): void {
		const { batch, baseSequence } = prepared

		this.logger.debug('duplicate sequence number - treating as success', {
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
	 */
	private handleOutOfOrderSequence(prepared: {
		batch: PartitionBatch
		baseSequence: number
		recordCount: number
		batchId: symbol
		messagesToSend: QueuedMessage[]
	}): void {
		const { batch, baseSequence, recordCount, batchId, messagesToSend } = prepared

		this.logger.error('out of order sequence number', {
			topic: batch.topic,
			partition: batch.partition,
			baseSequence,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.rollbackSequence(batch.topic, batch.partition, recordCount)
		this.inflightBatches.delete(batchId)

		const error = new Error(
			`Out of order sequence for ${batch.topic}-${batch.partition}: baseSequence=${baseSequence}`
		)
		for (const msg of messagesToSend) {
			msg.reject(error)
		}
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

		this.idempotentState = 'fenced'
		this.accumulator.setFenced(true)

		const fenceError = new ProducerFencedError(this.producerId, this.producerEpoch)

		// Fail all batches in this request
		for (const p of allPrepared) {
			for (const msg of p.messagesToSend) {
				msg.reject(fenceError)
			}
			this.rollbackSequence(p.batch.topic, p.batch.partition, p.recordCount)
			this.inflightBatches.delete(p.batchId)
		}

		// Fail all other in-flight batches
		this.failAllInflightBatches(fenceError)
		this.accumulator.clearWithRejection(fenceError)

		// Trigger re-initialization
		this.initializeIdempotentProducer().catch((error: Error) => {
			this.logger.error('failed to re-initialize after fencing', { error: error.message })
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

		return this.internalSend(topicDef, messages)
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
	 * Internal send implementation (bypasses transactional check)
	 */
	private async internalSend<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]> {
		// Lazy initialization for idempotent producer
		await this.ensureInitialized()

		const msgArray = Array.isArray(messages) ? messages : [messages]
		const isSingle = !Array.isArray(messages)

		const { encodeKey, encodeValue } = this.resolveEncoders(topicDef)

		// Get partition count for topic
		const partitionCount = await this.getPartitionCount(topicDef.topic)

		// Begin transaction to collect all messages before flushing
		// This prevents fragmentation when sending large arrays
		this.accumulator.beginAppendTransaction()

		const promises = msgArray.map(msg => {
			return new Promise<SendResult>((resolve, reject) => {
				try {
					const valueBuffer = msg.value === null ? null : encodeValue(msg.value)
					const partitionValue = valueBuffer ?? EMPTY_BUFFER
					const keyBuffer = encodeKey(msg.key)

					const headers: Record<string, Buffer> = {}
					if (msg.headers) {
						for (const [k, v] of Object.entries(msg.headers)) {
							headers[k] = Buffer.isBuffer(v) ? v : Buffer.from(v, 'utf-8')
						}
					}

					let partition = msg.partition
					if (partition === undefined) {
						partition = this.config.partitioner(topicDef.topic, keyBuffer, partitionValue, partitionCount)

						// Handle sticky partitioner for keyless messages
						if (partition === -1) {
							partition = this.getStickyPartition(topicDef.topic, partitionCount)
						}
					}

					// Validate partition
					if (partition < 0 || partition >= partitionCount) {
						reject(
							new Error(
								`Invalid partition ${partition} for topic ${topicDef.topic} (has ${partitionCount})`
							)
						)
						return
					}

					const queuedMessage: QueuedMessage = {
						topic: topicDef.topic,
						partition,
						key: keyBuffer,
						value: valueBuffer,
						headers,
						timestamp: BigInt(msg.timestamp?.getTime() ?? Date.now()),
						resolve,
						reject,
					}

					this.accumulator.append(queuedMessage)
				} catch (error) {
					reject(error instanceof Error ? error : new Error(String(error)))
				}
			})
		})

		// End transaction - flush all accumulated batches at once
		this.accumulator.endAppendTransaction()

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
		this.accumulator.flush()
		await Promise.all(this.inflight)
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

		await this.flush()
		this.accumulator.clear()

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

				return this.transactionalSend(topicDef, messages)
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
		const msgArray = Array.isArray(messages) ? messages : [messages]

		const { encodeKey, encodeValue } = this.resolveEncoders(topicDef)

		// Determine partitions that will be used
		const partitionCount = await this.getPartitionCount(topicDef.topic)
		const partitionsToAdd = new Set<number>()

		for (const msg of msgArray) {
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
		}

		// Add new partitions to transaction before sending
		if (partitionsToAdd.size > 0) {
			await this.addPartitionsToTransaction(topicDef.topic, Array.from(partitionsToAdd))
		}

		// Now do the actual send (uses internal send to bypass transactional check)
		return this.internalSend(topicDef, messages)
	}

	/**
	 * Add partitions to the current transaction
	 */
	private async addPartitionsToTransaction(topic: string, partitions: number[]): Promise<void> {
		let coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)

		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		while (strategy.shouldReconnect()) {
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

			// Check for errors in response
			let hasError = false
			let retriableError = false
			let errorCode = ErrorCode.None
			let errorMessage = ''

			for (const topicResult of response.results) {
				for (const partitionResult of topicResult.resultsByPartition) {
					if (partitionResult.errorCode !== ErrorCode.None) {
						hasError = true
						errorCode = partitionResult.errorCode
						errorMessage = `AddPartitionsToTxn failed for ${topicResult.name}-${partitionResult.partitionIndex}`

						// Handle coordinator move - invalidate cache and re-fetch
						if (
							partitionResult.errorCode === ErrorCode.NotCoordinator ||
							partitionResult.errorCode === ErrorCode.CoordinatorNotAvailable
						) {
							this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
							coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
							retriableError = true
							break
						}

						if (isRetriableError(partitionResult.errorCode)) {
							retriableError = true
							break
						}
					}
				}
				if (hasError) break
			}

			if (!hasError) {
				// Success - mark partitions as part of this transaction
				for (const partition of partitions) {
					this.partitionsInTransaction.add(`${topic}:${partition}`)
				}

				this.logger.debug('added partitions to transaction', {
					transactionalId: this.config.transactionalId,
					topic,
					partitions,
				})
				return
			}

			if (retriableError) {
				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())
					continue
				}
			}

			throw new KafkaProtocolError(errorCode, errorMessage)
		}

		throw new Error('AddPartitionsToTxn failed after max retries')
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

		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		while (strategy.shouldReconnect()) {
			// Register the group with the transaction coordinator
			const response = await coordinator.addOffsetsToTxn({
				transactionalId: this.config.transactionalId!,
				producerId: this.producerId,
				producerEpoch: this.producerEpoch,
				groupId,
			})

			if (response.errorCode === ErrorCode.None) {
				break
			}

			// Handle coordinator move - invalidate cache and re-fetch
			if (
				response.errorCode === ErrorCode.NotCoordinator ||
				response.errorCode === ErrorCode.CoordinatorNotAvailable
			) {
				this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
				coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())
					continue
				}
			}

			if (isRetriableError(response.errorCode)) {
				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())
					continue
				}
			}

			throw new KafkaProtocolError(response.errorCode, `AddOffsetsToTxn failed for group ${groupId}`)
		}

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

			const strategy = new ReconnectionStrategy({
				maxAttempts: this.config.retries + 1,
				initialDelayMs: this.config.retryBackoffMs,
				maxDelayMs: this.config.maxRetryBackoffMs,
			})

			while (strategy.shouldReconnect()) {
				const response = await coordinator.endTxn({
					transactionalId: this.config.transactionalId!,
					producerId: this.producerId,
					producerEpoch: this.producerEpoch,
					committed: true,
				})

				if (response.errorCode === ErrorCode.None) {
					break
				}

				// Handle coordinator move - invalidate cache and re-fetch
				if (
					response.errorCode === ErrorCode.NotCoordinator ||
					response.errorCode === ErrorCode.CoordinatorNotAvailable
				) {
					this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId!)
					coordinator = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId!)
					strategy.recordFailure()
					if (strategy.shouldReconnect()) {
						await this.sleep(strategy.nextDelay())
						continue
					}
				}

				if (isRetriableError(response.errorCode)) {
					strategy.recordFailure()
					if (strategy.shouldReconnect()) {
						await this.sleep(strategy.nextDelay())
						continue
					}
				}

				throw new KafkaProtocolError(response.errorCode, 'EndTxn commit failed')
			}

			this.logger.info('transaction committed', {
				transactionalId: this.config.transactionalId,
				partitionCount: this.partitionsInTransaction.size,
				groupCount: this.offsetsToCommit.size,
			})
		} finally {
			this.transactionState = 'idle'
			this.partitionsInTransaction.clear()
			this.offsetsToCommit.clear()
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

		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		while (strategy.shouldReconnect()) {
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

			// Check for errors - collect all partition errors
			let hasError = false
			let coordinatorError: ErrorCode | null = null

			for (const topic of response.topics) {
				for (const partition of topic.partitions) {
					if (partition.errorCode !== ErrorCode.None) {
						hasError = true
						if (
							partition.errorCode === ErrorCode.NotCoordinator ||
							partition.errorCode === ErrorCode.CoordinatorNotAvailable
						) {
							coordinatorError = partition.errorCode
						}
					}
				}
			}

			if (!hasError) {
				return // Success
			}

			// Handle coordinator move - invalidate cache and re-fetch
			if (coordinatorError !== null) {
				this.cluster.invalidateCoordinator('GROUP', groupId)
				groupCoordinator = await this.cluster.getCoordinator('GROUP', groupId)
				strategy.recordFailure()
				if (strategy.shouldReconnect()) {
					await this.sleep(strategy.nextDelay())
					continue
				}
			}

			// Throw first error found
			for (const topic of response.topics) {
				for (const partition of topic.partitions) {
					if (partition.errorCode !== ErrorCode.None) {
						throw new KafkaProtocolError(
							partition.errorCode,
							`TxnOffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
						)
					}
				}
			}
		}
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
		const strategy = new ReconnectionStrategy({
			maxAttempts: this.config.retries + 1,
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
		})

		while (strategy.shouldReconnect()) {
			try {
				let broker: Broker

				if (this.config.transactionalId) {
					// Transactional: must use transaction coordinator
					broker = await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId)
				} else {
					// Non-transactional: any broker works
					broker = await (this.cluster as unknown as { getAnyBroker: () => Promise<Broker> }).getAnyBroker()
				}

				const response = await broker.initProducerId({
					transactionalId: this.config.transactionalId,
					transactionTimeoutMs: this.config.transactionalId ? this.config.transactionTimeoutMs : 0,
					producerId: this.producerId !== -1n ? this.producerId : undefined,
					producerEpoch: this.producerEpoch !== -1 ? this.producerEpoch : undefined,
				})

				// Check for errors
				if (response.errorCode !== ErrorCode.None) {
					const errorCode = response.errorCode

					// Retriable errors (including coordinator movement) should backoff and retry.
					if (isRetriableError(errorCode)) {
						// If our transaction coordinator moved, invalidate cache so the next attempt re-discovers it.
						if (
							this.config.transactionalId &&
							(errorCode === ErrorCode.NotCoordinator || errorCode === ErrorCode.CoordinatorNotAvailable)
						) {
							this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId)
						}

						strategy.recordFailure()
						if (strategy.shouldReconnect()) {
							await this.sleep(strategy.nextDelay())
							continue
						}
					}

					// CLUSTER_AUTHORIZATION_FAILED or other non-retriable errors
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
			} catch (error) {
				strategy.recordFailure()

				if (!strategy.shouldReconnect()) {
					this.idempotentState = 'error'
					throw error
				}

				await this.sleep(strategy.nextDelay())
			}
		}

		this.idempotentState = 'error'
		throw new Error('Failed to initialize idempotent producer: max retries exceeded')
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
		return new Promise(resolve => setTimeout(resolve, ms))
	}
}
