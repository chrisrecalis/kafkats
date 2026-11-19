/**
 * Kafka Producer - high-level producer API with implicit batching
 */

import { EventEmitter } from 'node:events'
import { AsyncLocalStorage } from 'node:async_hooks'
import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import type { InitProducerIdResponse } from '@/protocol/messages/responses/init-producer-id.js'
import { SHUTDOWN_DEADLINE_MS, withTimeout } from '@/utils/timeout.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { ErrorCode, isRetriableError } from '@/protocol/messages/error-codes.js'
import {
	shouldRefreshMetadata,
	CommitFailedError,
	ProducerFencedError,
	InvalidTxnStateError,
	TransactionAbortedError,
	KafkaError,
	KafkaProtocolError,
	TimeoutError,
	isConnectionFailure,
	isRetriable,
} from '@/client/errors.js'
import { createRecordBatch, encodeRecordBatch, encodeRecordBatchSync } from '@/protocol/records/record-batch.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { ReconnectionStrategy } from '@/network/reconnection.js'
import { noopLogger, type Logger } from '@/logger.js'
import { abortable, deadlineSignal } from '@/utils/abortable.js'
import { sleep as sleepMs } from '@/utils/sleep.js'
import { createRetryStrategyOptions, retry, type RetryOptions } from '@/utils/retry.js'
import type { ConsumeContext } from '@/consumer/types.js'
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
	TransactionOffset,
	SendOffsetsParams,
	ProducerTransaction,
	TransactionHandle,
	BeginTransactionOptions,
	ProducerCallOptions,
	DisconnectOptions,
	GroupMetadata,
	ProducerTraceFn,
	ProducerMetrics,
} from './types.js'
import { SendOperation, diagnostics, register, trace, type ProduceRequestDiagnostics } from '@/diagnostics.js'

const EMPTY_BUFFER = Buffer.alloc(0)
/** What sends that disconnect() did not wait for are rejected with. */
const DISCONNECTED = 'Producer disconnected'

// Producers whose transaction callback is running in the current async context.
// Detects producer.transaction() called from inside that same producer's own
// callback, which would otherwise deadlock on the transaction queue. Module-level
// (rather than per-producer) so producer instances stay collectible without an
// explicit AsyncLocalStorage#disable() on disconnect.
const activeTransactionScopes = new AsyncLocalStorage<ReadonlySet<Producer>>()

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
	 * we must NOT reuse the sequence.  Instead the epoch fails (failEpoch), so the
	 * next send uses a fresh epoch whose sequence starts at 0.
	 */
	everSent: boolean
	/**
	 * The producerId and producerEpoch encoded into this batch's RecordBatch header. Compared against
	 * the current ones when the response arrives to detect a new epoch begun while it was in flight
	 * (see isStaleEpoch): the response does not match this batch any more, not even as a duplicate.
	 */
	encodedProducerId: bigint
	encodedProducerEpoch: number
	/** Why the last attempt to produce this batch failed: the cause given once its retries are used up. */
	lastError?: Error
}

type ProduceAttemptOutcome = {
	retryBatches: PreparedProduceBatch[]
	stop: boolean
}

/**
 * How one coordinator lookup and InitProducerId ended: the broker's answer, why there is none, or that the
 * initialization gave up before InitProducerId was sent (then nothing was).
 */
type InitAttemptOutcome = { response: InitProducerIdResponse } | { error: unknown } | { abandoned: true }

/** One coordinator lookup and InitProducerId. */
type InitAttempt = {
	/** Settles with how the attempt ended; never rejects. */
	readonly outcome: Promise<InitAttemptOutcome>
	/** Whether InitProducerId was sent: from then on the broker may apply it, whoever awaits its answer. */
	readonly sent: boolean
}

/**
 * One transaction of this producer, from its begin to its end: what it registered with the coordinator and how its
 * handle ended it. Its sends, registrations and end use only this, never the bookkeeping of another transaction, so
 * nothing of one transaction reaches the next, not even an answer of the coordinator that arrives late.
 */
class Transaction {
	/** What the handle did: 'in_transaction' while it takes sends, then 'committing' or 'aborting', then the outcome. */
	status: TransactionState | 'committed' | 'aborted' = 'in_transaction'
	/** Partitions added to it (AddPartitionsToTxn answered), as `topic:partition`. */
	readonly partitions = new Set<string>()
	/** Offsets to commit with it (TxnOffsetCommit), by group, once AddOffsetsToTxn added the group. */
	readonly offsets = new Map<string, { offsets: TransactionOffset[]; groupMetadata?: GroupMetadata }>()
	/** sendOffsets() calls that have not settled: its commit and its abort wait for them. */
	readonly registrations = new Set<Promise<void>>()
	/**
	 * Whether the coordinator began it: an AddPartitionsToTxn or AddOffsetsToTxn of it succeeded, maybe after it began
	 * to abort (Java's `transactionStarted`). Only then does its commit send EndTxn: a classic coordinator rejects EndTxn
	 * for a transaction it never began.
	 */
	started = false
	/**
	 * An AddPartitionsToTxn or AddOffsetsToTxn of it got no answer (the connection failed): the coordinator may have
	 * applied it, and begun the transaction. Its abort then sends EndTxn too, and an INVALID_TXN_STATE answer means it
	 * never began.
	 */
	addUnanswered = false
	/** Its first definitive send or registration failure, or the failure of its epoch: its commit aborts instead. */
	abortCause: Error | null = null
	/** An EndTxn(commit) of it was sent without an answer: the broker may have committed it. */
	commitOutcomeUnknown = false
	/**
	 * Aborted, with a TransactionAbortedError, once it aborts (the handle's signal): a send or registration in progress
	 * then starts no request any more, and what it was answered is not added.
	 */
	readonly ending = new AbortController()

	constructor(
		readonly transactionalId: string,
		/** The producer id and epoch it began with: what it sends carries them, or it is not sent. */
		readonly producerId: bigint,
		readonly producerEpoch: number
	) {}

	/** Whether sends and registrations may still be added to it: until it aborts, or committed. */
	get accepting(): boolean {
		return !this.ending.signal.aborted && (this.status === 'in_transaction' || this.status === 'committing')
	}

	/** Abort its signal with `reason`: what it has in progress is rejected with it. */
	end(reason = new TransactionAbortedError(this.transactionalId)): void {
		if (!this.ending.signal.aborted) this.ending.abort(reason)
	}

	/** The answer to an AddPartitionsToTxn or AddOffsetsToTxn of it, which records one that got none. */
	async answerToAdd<T>(request: Promise<T>): Promise<T> {
		try {
			return await request
		} catch (error) {
			this.addUnanswered = true
			throw error
		}
	}
}

/** The transaction a handle stands for, while it is this producer's open transaction. */
type OpenTransaction = {
	readonly handle: TransactionHandle
	readonly transaction: Transaction
	/** The producer it runs on: this one, or a lane of a pool, which is what a fence makes fatal. */
	readonly producer: Producer
	/** Resolves once the transaction ended (committed, aborted or failed) and the next one may begin. */
	readonly ended: Promise<void>
}

function isCoordinatorErrorCode(errorCode: ErrorCode): boolean {
	return errorCode === ErrorCode.NotCoordinator || errorCode === ErrorCode.CoordinatorNotAvailable
}

/** The result of a send() call, from the promises of its messages. */
function settleSend(promises: Promise<SendResult>[], isSingle: boolean): Promise<SendResult | SendResult[]> {
	return isSingle ? promises[0]! : Promise.all(promises)
}

function toError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error))
}

/** A failure the producer stopped retrying: trying again may succeed. Its cause is the last failure. */
function retriableFailure(message: string, cause: Error | undefined): KafkaError {
	const error = new KafkaError(
		cause ? `${message}: ${cause.message}` : message,
		cause instanceof KafkaError ? cause.errorCode : undefined,
		true
	)
	if (cause) error.cause = cause
	return error
}

/**
 * Run `operation` with a signal that aborts with the reason of `options.signal`, or with a {@link TimeoutError} once
 * `options.timeoutMs` passed. Without either the operation is unbounded.
 */
async function bounded<T>(
	what: string,
	{ timeoutMs, signal }: ProducerCallOptions = {},
	operation: (stop: AbortSignal | undefined) => Promise<T>
): Promise<T> {
	if (timeoutMs === undefined) return operation(signal)
	const timeout = new AbortController()
	const timer = setTimeout(() => timeout.abort(new TimeoutError(`${what} did not finish`, timeoutMs)), timeoutMs)
	try {
		return await operation(signal ? AbortSignal.any([signal, timeout.signal]) : timeout.signal)
	} finally {
		clearTimeout(timer)
	}
}

/**
 * KIP-447: offsets committed with group metadata are fenced by its generation and member, which must therefore be
 * those of a joined member. The broker accepts an unfenced TxnOffsetCommit in any group state, so a generation of -1
 * or an empty member id would commit without any fencing.
 */
function assertJoinedMember({ groupId, generationId, memberId }: GroupMetadata): void {
	if (generationId < 0) {
		throw new CommitFailedError(
			groupId,
			new KafkaProtocolError(ErrorCode.IllegalGeneration, `sendOffsets with generation ${generationId}`)
		)
	}
	if (memberId === '') {
		throw new CommitFailedError(
			groupId,
			new KafkaProtocolError(ErrorCode.UnknownMemberId, 'sendOffsets without a member id')
		)
	}
}

/** Poll interval for CONCURRENT_TRANSACTIONS — matches the Java client's ADD_PARTITIONS_RETRY_BACKOFF_MS. */
const CONCURRENT_TRANSACTIONS_POLL_MS = 20

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
	transactionConcurrency: 1,
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
	/**
	 * 'fenced': the epoch failed (see {@link failEpoch}) and sends nothing more. An idempotent producer is initializing
	 * again; a transactional one bumps the epoch when its transaction aborts. 'error': fatal ({@link fatalError}).
	 */
	private idempotentState: IdempotentState = 'idle'
	private initPromise: Promise<void> | null = null
	/**
	 * The InitProducerId an initialization gave up on (its maxBlockMs passed, or it was stopped) after sending it: the
	 * next initialization awaits it before it sends another (Java keeps the pending InitProducerId result), so that an
	 * answer the broker gives late never bumps the epoch behind a newer InitProducerId of this producer, which would
	 * fence it. An attempt given up before it sent InitProducerId sends nothing and is not kept.
	 */
	private pendingInitAttempt: InitAttempt | null = null
	/**
	 * Why the producer is in an 'error' state: a failed initialization, a fence, an abort that did not finish or a
	 * commit of unknown outcome. Later calls fail with it, or with an InvalidTxnStateError.
	 */
	private fatalError: Error | undefined
	/** Why the last epoch failed ({@link failEpoch}): its batches still waiting to be sent are rejected with it. */
	private epochFailure: Error | undefined

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

	// Sealed batches waiting to be sent, by `topic:partition`, each queue in append order.
	// A drain takes from the partitions without an in-flight send and groups them by broker.
	private pendingBatches = new Map<string, PartitionBatch[]>()
	private drainScheduled = false
	// Every outstanding send() promise. A send's promise settles only once all its
	// messages are acked (including retries), so flush() can wait for in-progress sends
	// by awaiting these directly — no need to inspect internal batch/drain state.
	private activeSends = new Set<Promise<unknown>>()
	// Batches currently being produced (also for non-idempotent producers, unlike inflightBatches),
	// so disconnect() can reject their callers when the broker does not answer.
	private sendingBatches = new Set<PreparedProduceBatch[]>()
	// Partitions with an in-flight send. A partition stays muted until its send
	// settles, so at most one batch per partition is ever in flight (preserves
	// per-partition ordering for the idempotent producer).
	private mutedPartitions = new Set<string>()
	// Per topic with sends that have not appended yet: the tail of the chain that keeps the
	// asynchronous part of send() in call order (see inSendOrder).
	private readonly sendOrder = new Map<string, Promise<unknown>>()
	// Number of sends that have entered inSendOrder: tells which of them were already waiting
	// when a partition lookup started.
	private sendsQueued = 0
	// Per topic with waiting sends: its last failed partition lookup, which also answers the sends
	// that were waiting when it started (see inSendOrder).
	private readonly failedLookups = new Map<string, { asOf: number; error: unknown }>()
	// Sends that have not appended yet (see inSendOrder), by how to reject them: disconnect() does
	// not wait for them beyond its deadline.
	private readonly preparingSends = new Set<(error: Error) => void>()

	// Transaction state (only used when transactionalId is set)
	private txnState: TransactionState = 'idle'
	// FIFO queue serializing transactions: the protocol allows one open transaction
	// per producer, so concurrent callers wait instead of throwing.
	private transactionTail: Promise<void> = Promise.resolve()
	// Number of transactions admitted (open + waiting to begin).
	private transactionQueueDepth = 0
	// Number of transaction() calls that have not settled: disconnect() refuses to run under them.
	private transactionCalls = 0
	private openTransaction: OpenTransaction | null = null
	// Aborted by disconnect(): rejects the transactions still waiting to begin, and stops an initialization.
	private readonly closing = new AbortController()

	// Transaction lane pool (transactionConcurrency > 1 only): each lane is a full
	// Producer with its own transactional ID, so lane transactions genuinely overlap.
	private readonly lanes: Producer[] | null = null
	private freeLanes: Producer[] = []
	private laneWaiters: Array<(lane: Producer) => void> = []

	constructor(cluster: Cluster, config: ProducerConfig = {}) {
		super()
		this.cluster = cluster
		this.logger = (cluster as unknown as { logger?: Logger }).logger?.child({ component: 'producer' }) ?? noopLogger
		this.trace = config.trace

		this.config = this.resolveConfig(config)
		this.accumulator = new RecordAccumulator(this.config, this.logger)
		register({ type: 'producer', clientId: cluster.clientId }, this)
		this.accumulator.on('batchReady', batch => {
			const key = `${batch.topic}:${batch.partition}`
			const queue = this.pendingBatches.get(key)
			if (queue) {
				queue.push(batch)
			} else {
				this.pendingBatches.set(key, [batch])
			}
			// A batch for a partition with an in-flight send cannot go out yet: the drain that
			// runs when that send settles picks it up.
			if (!this.mutedPartitions.has(key)) {
				this.scheduleDrain()
			}
		})

		if (this.config.transactionConcurrency > 1) {
			const lanes: Producer[] = []
			for (let laneId = 0; laneId < this.config.transactionConcurrency; laneId += 1) {
				// Lane 0 keeps the base ID so raising the setting still fences a predecessor.
				const lane = new Producer(cluster, {
					...config,
					transactionalId:
						laneId === 0 ? this.config.transactionalId! : `${this.config.transactionalId}-${laneId}`,
					transactionConcurrency: 1,
				})
				lane.on('error', error => this.emit('error', error))
				lanes.push(lane)
			}
			this.lanes = lanes
			this.freeLanes = [...lanes]
		}

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
		// While a transaction aborts, a batch not taken yet is rejected, not sent (Java's abortUndrainedBatches): it
		// would go out after the abort marker, under the same epoch, into the next transaction.
		const aborting = this.txnState === 'aborting' ? this.openTransaction?.transaction : undefined
		if (aborting) {
			this.rejectAndClearPendingBatches(toError(aborting.ending.signal.reason))
			return
		}

		// Take one batch per partition, and never from a partition that already has an
		// in-flight send (muted). This preserves per-partition ordering for the idempotent
		// producer: sending a second batch while an earlier one is in flight would let a
		// retriable failure on the earlier batch reorder sequences, so the later
		// (higher-sequence) batch reaches the broker first and trips
		// OutOfOrderSequenceNumber. Batches of a muted partition stay queued and are drained
		// when its in-flight send settles (see the finally below).
		//
		// The batches at the head of a partition's queue are merged into a single batch (in
		// order, up to maxBatchBytes). Every send() call seals its own batch right away, so
		// without this, concurrent sends to a busy partition would each cost a round trip;
		// with it, a busy partition batches whatever arrived while it was sending. Merging
		// stops at the first batch that does not fit: it and everything after it wait.
		const picked: string[] = []
		const toSend: PartitionBatch[] = []

		for (const [key, queue] of this.pendingBatches) {
			if (this.mutedPartitions.has(key)) continue

			const first = queue[0]!
			let taken = 1
			while (taken < queue.length && first.sizeBytes + queue[taken]!.sizeBytes <= this.config.maxBatchBytes) {
				const batch = queue[taken]!
				for (const message of batch.messages) first.messages.push(message)
				first.sizeBytes += batch.sizeBytes
				// The merged batch is sent (and accounted for) as part of `first`.
				this.accumulator.batchCompleted()
				taken++
			}
			if (taken === queue.length) {
				this.pendingBatches.delete(key)
			} else {
				queue.splice(0, taken)
			}

			picked.push(key)
			toSend.push(first)
		}

		if (toSend.length === 0) return

		// Mute the partitions we are about to send so subsequent drains skip them.
		for (const key of picked) {
			this.mutedPartitions.add(key)
		}

		const promise = this.sendBatchesWithRetries(toSend)
		this.inflight.add(promise)
		promise.finally(() => {
			this.inflight.delete(promise)
			for (const key of picked) {
				this.mutedPartitions.delete(key)
			}
			// Now-unmuted partitions may have batches waiting.
			if (this.pendingBatches.size > 0) {
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
					const key = `${topic}:${partition}`
					const queue = this.pendingBatches.get(key)
					if (queue) {
						queue.unshift(remainingBatch)
					} else {
						this.pendingBatches.set(key, [remainingBatch])
					}
					// The remainder never went through flushBatch(), but its send will call
					// batchCompleted() — account for it or the pending counter goes negative.
					this.accumulator.batchEnqueued()
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
		this.sendingBatches.add(preparedBatches)
		try {
			await this.sendPreparedBatchesWithRetry(preparedBatches)
		} finally {
			this.sendingBatches.delete(preparedBatches)
		}
	}

	private async sendPreparedBatchesWithRetry(preparedBatches: PreparedProduceBatch[]): Promise<void> {
		const strategy = new ReconnectionStrategy(createRetryStrategyOptions(this.config))
		let pendingBatches = preparedBatches

		// A disconnected producer sends nothing more (see sendPreparedBatchesToBroker).
		while (this.state !== 'idle' && strategy.shouldReconnect()) {
			const outcome = await this.sendPreparedBatchesOnce(pendingBatches)
			// A batch whose epoch failed meanwhile is not retried.
			pendingBatches = this.ofLiveEpoch(outcome.retryBatches)
			if (outcome.stop || pendingBatches.length === 0) {
				return
			}

			strategy.recordFailure()
			if (!strategy.shouldReconnect()) {
				this.failExhaustedBatches(pendingBatches)
				return
			}
			await this.sleep(strategy.nextDelay())
		}
	}

	private async sendPreparedBatchesOnce(preparedBatches: PreparedProduceBatch[]): Promise<ProduceAttemptOutcome> {
		const { batchesByBroker, leaderless } = await this.groupPreparedBatchesByLeader(preparedBatches)
		const outcomes = await Promise.all(
			Array.from(batchesByBroker.entries()).map(async ([, { broker, batches }]) => {
				try {
					return await this.sendPreparedBatchesToBroker(broker, batches, preparedBatches)
				} catch (error) {
					// broker.produce() rejected before any response arrived.
					const failure = toError(error)
					for (const prepared of batches) prepared.lastError = failure
					return { retryBatches: batches, stop: false }
				}
			})
		)

		if (outcomes.some(outcome => outcome.stop)) {
			return { retryBatches: [], stop: true }
		}

		return { retryBatches: [...leaderless, ...outcomes.flatMap(outcome => outcome.retryBatches)], stop: false }
	}

	/**
	 * The batches per partition leader. Those whose leader cannot be looked up (none elected, broker unknown) are
	 * `leaderless`, with the failure as their `lastError`: they are retried while the others are sent.
	 */
	private async groupPreparedBatchesByLeader(preparedBatches: PreparedProduceBatch[]): Promise<{
		batchesByBroker: Map<number, { broker: Broker; batches: PreparedProduceBatch[] }>
		leaderless: PreparedProduceBatch[]
	}> {
		const batchesByBroker = new Map<number, { broker: Broker; batches: PreparedProduceBatch[] }>()
		const leaderless: PreparedProduceBatch[] = []
		for (const prepared of preparedBatches) {
			const { batch } = prepared
			let broker: Broker
			try {
				broker = await this.cluster.getLeaderForPartition(batch.topic, batch.partition)
			} catch (error) {
				prepared.lastError = toError(error)
				leaderless.push(prepared)
				continue
			}
			const existing = batchesByBroker.get(broker.nodeId) ?? { broker, batches: [] }
			existing.batches.push(prepared)
			batchesByBroker.set(broker.nodeId, existing)
		}
		return { batchesByBroker, leaderless }
	}

	private async sendPreparedBatchesToBroker(
		broker: Broker,
		batches: PreparedProduceBatch[],
		allPendingBatches: PreparedProduceBatch[]
	): Promise<ProduceAttemptOutcome> {
		await this.acquireInflightSlot()
		try {
			// disconnect() has returned and rejected these messages: writing them now (a retry of
			// a request that was still in flight then) would contradict what the caller was told.
			if (this.state === 'idle') return { retryBatches: [], stop: true }
			const preparedBatches = this.ofLiveEpoch(batches)
			if (preparedBatches.length === 0) return { retryBatches: [], stop: false }
			const request = this.buildProduceRequest(preparedBatches)

			// Mark as transmitted before awaiting: once broker.produce() is called the broker
			// may have appended the batch, so any failure after this point is ambiguous.
			for (const prepared of preparedBatches) {
				prepared.everSent = true
			}
			const requestStart = performance.now()
			const response = await trace(
				diagnostics.produceRequest,
				(): ProduceRequestDiagnostics => ({
					clientId: this.cluster.clientId,
					brokerId: broker.nodeId,
					partitions: preparedBatches.length,
					records: preparedBatches.reduce((sum, p) => sum + p.recordCount, 0),
					bytes: preparedBatches.reduce((sum, p) => sum + p.encodedBatch.length, 0),
				}),
				async produceRequest => {
					const produced = await broker.produce(request)
					if (produceRequest) {
						produceRequest.rejected = produced.topics.flatMap(topic =>
							topic.partitions
								.filter(partition => partition.errorCode !== ErrorCode.None)
								.map(p => ({ topic: topic.name, partition: p.partitionIndex, errorCode: p.errorCode }))
						)
					}
					return produced
				}
			)
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

				// A new epoch since this request went out makes the response belong to the old one,
				// whose failure (failEpoch) already rejected its messages: skip it without touching
				// any sequence/commit state.
				if (this.isStaleEpoch(prepared)) {
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
					// Broker responded but omitted this partition: the outcome is ambiguous
					// (the request was transmitted). Only an idempotent producer has sequence
					// state to protect — its epoch fails like after a connection failure. Otherwise
					// treat it as retriable and let the retry budget decide.
					if (this.config.idempotent) {
						const ambiguousError = retriableFailure(
							`Ambiguous produce outcome (no broker response for ${batch.topic}-${batch.partition})`,
							undefined
						)
						this.failEpoch(ambiguousError, allPendingBatches)
						return { retryBatches: [], stop: true }
					}
					prepared.lastError = new Error(
						`The produce response has no result for ${batch.topic}-${batch.partition}`
					)
					retryBatches.push(prepared)
					continue
				}

				if (partitionResponse.errorCode !== ErrorCode.None) {
					const errorCode = partitionResponse.errorCode

					if (
						this.config.idempotent &&
						(errorCode === ErrorCode.ProducerFenced || errorCode === ErrorCode.InvalidProducerEpoch)
					) {
						this.handleFencingError(prepared, errorCode, allPendingBatches)
						return { retryBatches: [], stop: true }
					}

					if (this.config.idempotent && errorCode === ErrorCode.DuplicateSequenceNumber) {
						this.handleDuplicateSequence(prepared, partitionResponse)
						continue
					}

					if (this.config.idempotent && errorCode === ErrorCode.OutOfOrderSequenceNumber) {
						this.handleOutOfOrderSequence(prepared, allPendingBatches)
						return { retryBatches: [], stop: true }
					}

					if (isRetriableError(errorCode)) {
						prepared.lastError = new KafkaProtocolError(
							errorCode,
							`Produce to ${batch.topic}-${batch.partition}`
						)
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
						new KafkaProtocolError(errorCode, `Produce failed for ${batch.topic}-${batch.partition}`)
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
	 * True when a prepared batch was encoded under a producer identity that no longer
	 * matches the current one — i.e. a new epoch began between when the
	 * batch was prepared and when its response arrived. Only meaningful for idempotent
	 * producers (non-idempotent batches encode -1/-1 and are never compared).
	 */
	private isStaleEpoch(prepared: PreparedProduceBatch): boolean {
		return (
			this.config.idempotent &&
			(prepared.encodedProducerId !== this.producerId || prepared.encodedProducerEpoch !== this.producerEpoch)
		)
	}

	/** Reject every message still queued in pendingBatches (split remainders) and clear the queue. */
	private rejectAndClearPendingBatches(error: Error): void {
		for (const queue of this.pendingBatches.values()) {
			for (const batch of queue) {
				for (const msg of batch.messages) {
					msg.reject(error)
				}
				// Every queued batch holds a pendingBatches count (flushBatch or batchEnqueued)
				// and will never reach batchCompleted() — settle the accounting here.
				this.accumulator.batchCompleted()
			}
		}
		this.pendingBatches.clear()
	}

	/**
	 * Fail batches whose retries are used up, with a retriable KafkaError whose cause is the batch's last failure.
	 *
	 * For an idempotent producer, if any batch was already transmitted (everSent) its
	 * append outcome is unknown — reusing its sequence could later let a
	 * DuplicateSequenceNumber response resolve a different batch's messages with the wrong
	 * offsets (a false ack). So the epoch fails ({@link failEpoch}): the next one restarts at
	 * sequence 0. Otherwise (nothing transmitted, or a non-idempotent producer) we roll back
	 * each batch's reserved sequence and reject its messages.
	 *
	 * Definitive single-batch rejections (an explicit broker error code) call
	 * failPreparedBatch directly instead.
	 */
	private failExhaustedBatches(preparedBatches: PreparedProduceBatch[]): void {
		const exhausted = ({ batch, lastError }: PreparedProduceBatch) =>
			retriableFailure(`Produce failed for ${batch.topic}-${batch.partition}: max retries exceeded`, lastError)

		if (this.config.idempotent) {
			const firstSent = preparedBatches.find(p => p.everSent)
			if (firstSent) {
				const ambiguousError = retriableFailure('Ambiguous produce outcome after send', exhausted(firstSent))
				this.failEpoch(ambiguousError, preparedBatches)
				return
			}
		}

		for (const prepared of preparedBatches) {
			this.failPreparedBatch(prepared, exhausted(prepared))
		}
	}

	private failPreparedBatch(prepared: PreparedProduceBatch, error: Error): void {
		const { batch, recordCount, batchId, messagesToSend } = prepared
		if (this.config.idempotent) {
			// Safe to roll back: this path is only for definitive failures (explicit broker
			// error code, or a pre-send encoding failure) where the sequence was not consumed.
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
		// logAppendTimeMs is -1 for CreateTime topics — fall back to the timestamp
		// encoded into each record (Java parity).
		const logAppendTimeMs = partitionResponse.logAppendTimeMs

		for (let i = 0; i < messagesToSend.length; i++) {
			const message = messagesToSend[i]!
			message.resolve({
				topic: batch.topic,
				partition: batch.partition,
				offset: baseOffset + BigInt(i),
				timestamp: new Date(Number(logAppendTimeMs === -1n ? message.timestamp : logAppendTimeMs)),
			})
		}

		if (this.config.idempotent) {
			this.commitSequence(batch.topic, batch.partition, recordCount)
			this.inflightBatches.delete(batchId)
		}

		this.rotateStickyPartition(batch.topic)
	}

	/**
	 * DuplicateSequenceNumber means the broker already appended a batch with this producer id, epoch and base
	 * sequence: the exact same batch was already written (genuine retry dedup), so it is a success. Only answers for
	 * the current epoch get here: the response loop skips those of an earlier one.
	 */
	private handleDuplicateSequence(
		prepared: PreparedProduceBatch,
		partitionResponse: { baseOffset: bigint; logAppendTimeMs: bigint }
	): void {
		this.logger.debug('duplicate sequence number - treating as success (genuine retry dedup)', {
			topic: prepared.batch.topic,
			partition: prepared.batch.partition,
			baseSequence: prepared.baseSequence,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.handleBatchSuccess(prepared, partitionResponse)
	}

	/** Out-of-order sequence: the per-partition sequence is desynced from the broker, the epoch fails (Java). */
	private handleOutOfOrderSequence(prepared: PreparedProduceBatch, allPrepared: PreparedProduceBatch[]): void {
		const { batch, baseSequence } = prepared

		this.logger.error('out of order sequence number: the producer epoch failed', {
			topic: batch.topic,
			partition: batch.partition,
			baseSequence,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.failEpoch(
			new KafkaProtocolError(
				ErrorCode.OutOfOrderSequenceNumber,
				`Out of order sequence for ${batch.topic}-${batch.partition}: baseSequence=${baseSequence}`
			),
			allPrepared
		)
	}

	/**
	 * PRODUCER_FENCED (a `ProducerFencedError`) or INVALID_PRODUCER_EPOCH (KIP-588: an epoch the partition already saw
	 * superseded, which only the next EndTxn tells apart from a fence) answered to a produce: the epoch fails.
	 */
	private handleFencingError(
		prepared: PreparedProduceBatch,
		errorCode: ErrorCode,
		allPrepared: PreparedProduceBatch[]
	): void {
		const { batch } = prepared

		this.logger.warn('produce answered with a fenced epoch', {
			topic: batch.topic,
			partition: batch.partition,
			errorCode,
			producerId: this.producerId.toString(),
			producerEpoch: this.producerEpoch,
		})

		this.failEpoch(
			errorCode === ErrorCode.ProducerFenced
				? new ProducerFencedError(this.producerId, this.producerEpoch)
				: new KafkaProtocolError(errorCode, `Produce to ${batch.topic}-${batch.partition}`),
			allPrepared
		)
	}

	/**
	 * The current epoch can send nothing more (an out-of-order sequence, an outcome the retries left unknown, a fenced
	 * epoch): rejects with `error` every send of it (`allPrepared`, those in flight and those queued; one still to be
	 * sent is dropped by {@link ofLiveEpoch}), then decides, as the only place that does, what follows:
	 *
	 * - Idempotent producer: InitProducerId with its producer id and epoch bumps the epoch at once.
	 * - Transactional producer (Java's TransactionManager): a fence (`ProducerFencedError`, also from a coordinator) is
	 *   fatal, the producer is never initialized again: a successor with the same transactional id (or the transaction
	 *   timeout) took the epoch, and the coordinator, which checks only the producer id, would abort the successor's
	 *   ongoing transaction for an InitProducerId with the stale epoch. Any other failure leaves the open transaction
	 *   abortable only, with no InitProducerId now: during the ongoing transaction, one would fence this producer itself.
	 *   Its abort bumps the epoch once EndTxn confirmed it current ({@link abortTransaction}). With no transaction open,
	 *   no EndTxn could confirm it: fatal too.
	 *
	 * An answer for an epoch that already failed (another batch of the same request, a late answer) only rejects its
	 * batches, unless it fences a transactional producer.
	 */
	private failEpoch(error: Error, allPrepared: readonly PreparedProduceBatch[] = []): void {
		const transactional = this.config.transactionalId !== null
		const fatal = transactional && (error instanceof ProducerFencedError || this.txnState === 'idle')
		const current = this.idempotentState === 'running' && !allPrepared.some(p => this.isStaleEpoch(p))
		if (this.txnState === 'error' || (!current && !fatal)) {
			this.rejectBatches(allPrepared, error)
			return
		}

		this.epochFailure = error
		this.accumulator.setFenced(true)
		// The rollbacks only keep the accounting of this epoch: the next one starts its sequences at 0.
		for (const p of allPrepared) {
			for (const msg of p.messagesToSend) {
				msg.reject(error)
			}
			this.rollbackSequence(p.batch.topic, p.batch.partition, p.recordCount)
			this.inflightBatches.delete(p.batchId)
		}
		// Split-remainder batches would otherwise dangle once the epoch changes.
		this.rejectAndClearPendingBatches(error)
		this.failAllInflightBatches(error)
		this.accumulator.clearWithRejection(error)

		if (fatal) {
			this.idempotentState = 'error'
			this.enterFatalState(error, 'the producer epoch failed and cannot be bumped')
		} else if (transactional) {
			this.idempotentState = 'fenced'
			// The open transaction ended with this epoch: it can only abort.
			if (this.openTransaction) this.recordTransactionFailure(this.openTransaction.transaction, error)
		} else {
			this.idempotentState = 'fenced'
			this.initializeIdempotentProducer().catch((reinitError: Error) => {
				this.logger.error('failed to re-initialize after the epoch failed', { error: reinitError.message })
			})
		}
	}

	/**
	 * The batches that may go out: those of an idempotent producer only while it runs at the identity they were encoded
	 * with. The others belong to an epoch that failed and are dropped, their messages rejected with its failure (a batch
	 * taken for sending before the failure and prepared after it was not among those failEpoch rejected): nothing more
	 * goes out under that epoch, neither a retry nor a request that would race the abort marker of its transaction.
	 */
	private ofLiveEpoch(batches: PreparedProduceBatch[]): PreparedProduceBatch[] {
		if (!this.config.idempotent) return batches
		const live = batches.filter(p => this.idempotentState === 'running' && !this.isStaleEpoch(p))
		if (live.length < batches.length) {
			this.rejectBatches(
				batches.filter(p => !live.includes(p)),
				this.epochFailure ?? new Error('The producer epoch failed')
			)
		}
		return live
	}

	/** Reject the messages of batches whose epoch failed: their sequences die with it. */
	private rejectBatches(batches: readonly PreparedProduceBatch[], error: Error): void {
		for (const p of batches) {
			for (const msg of p.messagesToSend) {
				msg.reject(error)
			}
			this.inflightBatches.delete(p.batchId)
		}
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

		return this.trackSend(this.tracedSend(topicDef, messages, false, m => this.internalSend(topicDef, m)))
	}

	/**
	 * Send as one traced operation (see `diagnostics.send`). Headers that subscribers ask to add are
	 * merged into copies of the messages; without any, the caller's messages are sent as they are.
	 */
	private tracedSend<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[],
		transactional: boolean,
		send: (messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]) => Promise<SendResult | SendResult[]>
	): Promise<SendResult | SendResult[]> {
		const list = Array.isArray(messages) ? messages : [messages]
		return trace(
			diagnostics.send,
			() => new SendOperation(this.cluster.clientId, topicDef.topic, transactional, list),
			sending => {
				const added = sending?.added
				if (!added) return send(messages)
				const withHeaders = list.map((message, i) =>
					added[i] ? { ...message, headers: { ...message.headers, ...added[i] } } : message
				)
				return send(Array.isArray(messages) ? withHeaders : withHeaders[0]!)
			}
		)
	}

	/**
	 * What is waiting inside this producer right now. Lets a monitor see backpressure building up
	 * before `send()` latency shows it.
	 */
	metrics(): ProducerMetrics {
		let queuedBatches = 0
		let queuedRecords = 0
		let queuedBytes = 0
		for (const queue of this.pendingBatches.values()) {
			for (const batch of queue) {
				queuedBatches++
				queuedRecords += batch.messages.length
				queuedBytes += batch.sizeBytes
			}
		}
		return {
			queuedBatches,
			queuedRecords,
			queuedBytes,
			inFlightRequests: this.inflightCount,
			unacknowledgedSends: this.activeSends.size,
		}
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
	private internalSend<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]> {
		const msgArray = Array.isArray(messages) ? messages : [messages]
		const isSingle = !Array.isArray(messages)

		const partitionCount = this.partitionCountToAppendNow(topicDef.topic)
		if (partitionCount !== undefined) {
			return settleSend(this.enqueueMessages(topicDef, msgArray, partitionCount), isSingle)
		}
		return this.inSendOrder(topicDef.topic, () => this.append(topicDef, msgArray)).then(promises =>
			settleSend(promises, isSingle)
		)
	}

	/**
	 * Run the asynchronous preparation of a send (initialization, metadata lookup, transaction
	 * registration) and its append, after every earlier send() to the same topic has appended its
	 * messages. Sends to other topics are not held up: order only matters within a partition.
	 *
	 * The preparation takes a varying number of asynchronous steps (a cache hit or a metadata
	 * request, a partition already in the transaction or not), so without this two send() calls
	 * issued back to back could append in the wrong order. Appending in call order is what makes
	 * "records of a partition are written in the order they were sent" hold for sends that are
	 * not awaited one by one.
	 *
	 * A send that has nothing to wait for skips this and appends synchronously, but only while no
	 * send to its topic is in here (see partitionCountToAppendNow), so it cannot overtake one.
	 */
	private inSendOrder<T>(topic: string, prepareAndAppend: () => Promise<T>): Promise<T> {
		const position = ++this.sendsQueued
		const turn = (): Promise<T> => {
			if (this.state === 'idle') return Promise.reject(new Error(DISCONNECTED))
			// A lookup that started while this send was waiting did not find the topic's partitions.
			// That answers this send too: looking again for every waiting send would fail them one
			// lookup (about a second each) after another.
			const failed = this.failedLookups.get(topic)
			return failed && position <= failed.asOf ? Promise.reject(failed.error) : prepareAndAppend()
		}
		const run = (this.sendOrder.get(topic) ?? Promise.resolve()).then(turn, turn)
		// The topic stays in the map until its last waiting send has appended (or failed), never
		// less: that is what keeps a later send from appending right away and overtaking it.
		const done = () => {
			if (this.sendOrder.get(topic) !== tail) return
			this.sendOrder.delete(topic)
			this.failedLookups.delete(topic)
		}
		const tail = run.then(done, done)
		this.sendOrder.set(topic, tail)
		// Settles with the send's turn, or earlier when disconnect() gives up waiting for it.
		return new Promise<T>((resolve, reject) => {
			this.preparingSends.add(reject)
			const prepared = (): void => void this.preparingSends.delete(reject)
			run.then(resolve, reject).then(prepared, prepared)
		})
	}

	/**
	 * The partition count to append a send with right away, or undefined when the send must go
	 * through {@link inSendOrder}: an earlier send is still waiting there (appending now would
	 * overtake it), the idempotent producer is not initialized, or the partition count is not
	 * cached.
	 */
	private partitionCountToAppendNow(topic: string): number | undefined {
		if (this.sendOrder.has(topic)) return undefined
		if (this.config.idempotent && this.idempotentState !== 'running') return undefined
		return this.cachedPartitionCount(topic)
	}

	/**
	 * Append messages to the accumulator. Must run inside {@link inSendOrder}. A send of a transaction passes its
	 * `assertLive`, run right before the append: the transaction's producer is initialized when the transaction
	 * begins, never here.
	 */
	private async append<V, K>(
		topicDef: TopicDefinition<V, K>,
		msgArray: ProducerMessage<V, K>[],
		assertLive?: () => void
	): Promise<Promise<SendResult>[]> {
		if (assertLive === undefined) await this.ensureInitialized()
		const partitionCount = await this.getPartitionCount(topicDef.topic)
		// disconnect() returned while this send was preparing: it has been rejected there and
		// must not be produced any more.
		if (this.state === 'idle') throw new Error(DISCONNECTED)
		assertLive?.()
		return this.enqueueMessages(topicDef, msgArray, partitionCount)
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
		if (this.lanes) {
			await Promise.all(this.lanes.map(lane => lane.flush()))
		}
		this.accumulator.flush()
		await Promise.allSettled(this.activeSends)
	}

	/**
	 * Disconnect the producer
	 *
	 * Flushes all pending messages before disconnecting, within `timeoutMs` (default 5000): against an unresponsive
	 * broker, whatever is not acknowledged by then is rejected. A transaction left open by a
	 * {@link beginTransaction} handle is aborted within the same budget (one that is committing is waited for), without
	 * the epoch bump that follows the abort of a transaction whose epoch failed; past it, the broker aborts the
	 * transaction after `transactionTimeoutMs`. Transactions still waiting to begin are rejected. Also allowed in the
	 * 'error' transaction state.
	 *
	 * @throws Error while a `transaction()` call is active or queued - await it first
	 */
	async disconnect(options: DisconnectOptions = {}): Promise<void> {
		if (this.state === 'stopping' || this.state === 'idle') {
			return
		}

		if (this.transactionCalls > 0) {
			throw new Error(
				`Cannot disconnect while transactions are active or queued (state '${this.transactionState}', ` +
					`queued ${this.transactionQueueDepth}). Await producer.transaction() before calling disconnect().`
			)
		}

		const deadlineAt = Date.now() + (options.timeoutMs ?? SHUTDOWN_DEADLINE_MS)
		const remainingMs = () => Math.max(0, deadlineAt - Date.now())
		this.state = 'stopping'
		this.closing.abort(new Error(DISCONNECTED))
		this.logger.info('disconnecting producer')

		// Lanes run no transaction() call (the pool does), so their disconnects cannot throw.
		if (this.lanes) {
			await Promise.all(this.lanes.map(lane => lane.disconnect({ timeoutMs: remainingMs() })))
		}

		await this.endOpenTransaction(remainingMs)

		try {
			// Bounded: against an unresponsive broker the remaining batches are rejected below
			// instead of waiting out the request timeouts.
			await withTimeout(this.flush(), remainingMs(), 'flush')
		} catch (error) {
			this.logger.warn('flush() failed during disconnect', {
				error: error instanceof Error ? error.message : String(error),
			})
		}

		// If flush() threw, batches may still be in pendingBatches / inflightBatches / the
		// accumulator, and sends may still be preparing (e.g. waiting for metadata). Reject
		// everything so caller promises don't hang; a send that finishes preparing after this
		// finds the producer disconnected and appends nothing.
		const disconnectError = new Error(DISCONNECTED)
		this.state = 'idle'
		for (const reject of this.preparingSends) reject(disconnectError)
		this.preparingSends.clear()
		this.rejectAndClearPendingBatches(disconnectError)
		this.failAllInflightBatches(disconnectError)
		for (const prepared of this.sendingBatches) {
			for (const { messagesToSend } of prepared) {
				for (const msg of messagesToSend) {
					msg.reject(disconnectError)
				}
			}
		}
		this.accumulator.clearWithRejection(disconnectError)

		this.logger.info('producer disconnected')
	}

	/**
	 * Abort the transaction a handle left open (Java parity: close() aborts an incomplete transaction), or wait for
	 * the one committing, within the disconnect budget.
	 */
	private async endOpenTransaction(remainingMs: () => number): Promise<void> {
		const open = this.openTransaction
		if (!open) return
		// Rejects while committing (waited for below) or when the abort fails, which the 'error' event reports.
		open.handle.abort({ timeoutMs: remainingMs() }).catch(() => {})
		try {
			await withTimeout(open.ended, remainingMs(), 'open transaction')
		} catch (error) {
			this.logger.warn('the open transaction did not end during disconnect', { error: toError(error).message })
		}
	}

	// ==================== Transaction API ====================

	/**
	 * State of this producer's transaction: 'idle' between transactions, 'error' once it can run none any more
	 * (a fence, an abort that did not finish, or a commit whose outcome is unknown; replace the producer, disconnect() is
	 * still allowed). With `transactionConcurrency` above 1 it sums up the lanes: 'error' if one is in it, else the
	 * state of a lane with a transaction, else 'idle'.
	 */
	get transactionState(): TransactionState {
		if (!this.lanes) return this.txnState
		const states = this.lanes.map(lane => lane.txnState)
		return states.includes('error') ? 'error' : (states.find(state => state !== 'idle') ?? 'idle')
	}

	/**
	 * Begin a transaction and return its handle, which commits or aborts it.
	 *
	 * Transactions run one at a time per transactional ID (with `transactionConcurrency` N, N at a time), first in
	 * first out together with transaction() calls: this waits until earlier ones ended. The first transaction
	 * initializes the producer (InitProducerId), which fences older producers with the same transactional ID.
	 *
	 * Without `signal`, an open transaction has no client-side timeout; the broker aborts it after
	 * `transactionTimeoutMs`. Aborting `signal` rejects the call while it waits, and aborts the transaction once open
	 * until the handle's `commit()` or `abort()` is called; those are bounded by their own `timeoutMs` and `signal`.
	 *
	 * @throws ProducerFencedError once the producer was fenced, InvalidTxnStateError in the other cases of the 'error'
	 * state
	 *
	 * @example
	 * ```typescript
	 * const tx = await producer.beginTransaction()
	 * try {
	 *   await tx.send(ordersTopic, { key: orderId, value: order })
	 *   await tx.sendOffsets({ consumerGroupMetadata, offsets })
	 * } catch (error) {
	 *   await tx.abort()
	 *   throw error
	 * }
	 * await tx.commit()
	 * ```
	 */
	async beginTransaction(options: BeginTransactionOptions = {}): Promise<TransactionHandle> {
		this.assertCanBegin('beginTransaction()')
		return (await this.begin(options.signal)).handle
	}

	/**
	 * Execute operations within a transaction
	 *
	 * The transaction is automatically committed if the function resolves,
	 * or aborted if it throws an error.
	 *
	 * A transactional ID can have one open transaction at a time (a Kafka
	 * protocol constraint). Concurrent transaction() calls wait for capacity
	 * instead of throwing, so this is safe to call from handlers running with
	 * partitionConcurrency > 1. Set `transactionConcurrency` above 1 to let up
	 * to N transactions run genuinely in parallel (backed by a pool of internal
	 * transactional IDs); calls are admitted first-in first-out. Independent
	 * transactions have no ordering guarantee relative to each other. Calling
	 * transaction() from inside this producer's own transaction callback throws.
	 *
	 * The transaction timeout starts when the transaction begins, not while it
	 * waits in the queue.
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
		this.assertCanBegin('transaction()')
		this.transactionCalls++
		try {
			const { handle: tx, producer } = await this.begin(undefined)
			const timeoutMs = options.timeoutMs ?? this.config.transactionTimeoutMs
			let timeoutId: ReturnType<typeof setTimeout> | undefined
			let result: T
			try {
				const scope = new Set(activeTransactionScopes.getStore())
				scope.add(this)
				result = await activeTransactionScopes.run(scope, () =>
					Promise.race([
						fn(tx),
						new Promise<never>((_, reject) => {
							timeoutId = setTimeout(
								() => reject(new Error(`Transaction timeout after ${timeoutMs}ms`)),
								timeoutMs
							)
						}),
					])
				)
			} catch (error) {
				// A failed abort leaves the producer in the 'error' state and is reported by the 'error' event.
				await tx.abort().catch(() => {})
				// The fence of the producer it ran on: with transactionConcurrency above 1, a lane.
				throw (
					producer.fence ??
					(error instanceof TransactionAbortedError
						? error
						: new TransactionAbortedError(this.config.transactionalId!, toError(error)))
				)
			} finally {
				clearTimeout(timeoutId)
			}
			await tx.commit()
			return result
		} finally {
			this.transactionCalls--
		}
	}

	private assertCanBegin(method: string): void {
		if (!this.config.transactionalId) {
			throw new Error(`${method} requires transactionalId to be configured`)
		}
		if (activeTransactionScopes.getStore()?.has(this)) {
			throw new Error(
				`producer.${method} was called from inside an active transaction callback. ` +
					'Nested transactions are not supported — use tx.send()/tx.sendOffsets() instead.'
			)
		}
	}

	/** Wait for capacity, then open a transaction here or on a lane. */
	private async begin(signal: AbortSignal | undefined): Promise<OpenTransaction> {
		const queuedAhead = this.transactionQueueDepth++
		try {
			if (queuedAhead >= (this.lanes?.length ?? 1)) {
				this.logger.debug('transaction queued behind active transactions', { queued: queuedAhead })
				// Observability only - a throwing listener must not corrupt queue state.
				try {
					this.emit('transaction:queued', { queued: queuedAhead })
				} catch (error) {
					this.logger.warn('transaction:queued listener threw', { error: toError(error).message })
				}
			}
			const stop = signal ? AbortSignal.any([signal, this.closing.signal]) : this.closing.signal
			stop.throwIfAborted()
			const open = this.lanes ? await this.openOnLane(stop, signal) : await this.openHere(stop, signal)
			void open.ended.then(() => this.transactionQueueDepth--)
			return open
		} catch (error) {
			this.transactionQueueDepth--
			throw error
		}
	}

	/** Take this producer's turn (FIFO), initialize it, and open the transaction. */
	private async openHere(stop: AbortSignal, signal: AbortSignal | undefined): Promise<OpenTransaction> {
		// A fenced producer begins nothing any more: no need to wait for the turn of its open transaction (Java throws).
		const fence = this.fence
		if (fence) throw fence
		const previous = this.transactionTail
		let release!: () => void
		this.transactionTail = new Promise(resolve => {
			release = resolve
		})
		try {
			await abortable(previous, stop)
		} catch (error) {
			// Given up while waiting: pass the turn on once it comes.
			void previous.then(release)
			throw error
		}
		try {
			// Only a producer in a fatal state can be non-idle here: transactions take turns and never overlap.
			if (this.txnState !== 'idle') {
				throw this.fence ?? new InvalidTxnStateError(this.config.transactionalId!, this.txnState, ['idle'])
			}
			await abortable(this.ensureInitialized(), stop)
			stop.throwIfAborted()
		} catch (error) {
			release()
			throw error
		}
		return this.open(release, signal)
	}

	/**
	 * Open the transaction on a free lane, or wait FIFO for the next one released. Transactions on different lanes may
	 * commit out of call order.
	 */
	private async openOnLane(stop: AbortSignal, signal: AbortSignal | undefined): Promise<OpenTransaction> {
		const lane = await this.acquireLane(stop)
		try {
			const open = await lane.begin(signal)
			void open.ended.then(() => this.releaseLane(lane))
			return open
		} catch (error) {
			this.releaseLane(lane)
			throw error
		}
	}

	private acquireLane(stop: AbortSignal): Promise<Producer> {
		const lane = this.freeLanes.pop()
		if (lane) {
			return Promise.resolve(lane)
		}
		return new Promise((resolve, reject) => {
			const take = (lane: Producer) => {
				stop.removeEventListener('abort', giveUp)
				resolve(lane)
			}
			const giveUp = () => {
				this.laneWaiters.splice(this.laneWaiters.indexOf(take), 1)
				reject(stop.reason)
			}
			stop.addEventListener('abort', giveUp, { once: true })
			this.laneWaiters.push(take)
		})
	}

	private releaseLane(lane: Producer): void {
		const waiter = this.laneWaiters.shift()
		if (waiter) {
			waiter(lane)
		} else {
			this.freeLanes.push(lane)
		}
	}

	/**
	 * Open a transaction (this producer's turn taken and the producer initialized) and create its handle. The turn is
	 * passed on once the handle's commit or abort ended. Aborting `signal` aborts the transaction while it is open.
	 */
	private open(release: () => void, signal: AbortSignal | undefined): OpenTransaction {
		const transactionalId = this.config.transactionalId!
		this.txnState = 'in_transaction'
		const transaction = new Transaction(transactionalId, this.producerId, this.producerEpoch)
		let markEnded!: () => void
		const ended = new Promise<void>(resolve => {
			markEnded = resolve
		})

		// Once the producer is fenced, every call of the handle rejects with the fence, also once the transaction ended
		// (Java: every transactional call in FATAL_ERROR); while it is open, its end runs first (commit, abort).
		const assertNotFenced = (): void => {
			const fence = this.fence
			if (fence) throw fence
		}
		// The handle's status, not the producer's state: that can change under it (a fence makes it fatal).
		const assertOpen = (): void => {
			const state = transaction.status === 'in_transaction' ? this.txnState : transaction.status
			if (state !== 'in_transaction') {
				assertNotFenced()
				throw new InvalidTxnStateError(transactionalId, state, ['in_transaction'])
			}
		}
		const assertSendable = (): void => {
			if (transaction.status === 'in_transaction') this.assertLive(transaction)
			assertOpen()
		}
		const end = (next: 'committing' | 'aborting', run: () => Promise<void>): Promise<void> => {
			transaction.status = next
			return run().finally(() => {
				signal?.removeEventListener('abort', abortOnSignal)
				this.openTransaction = null
				release()
				markEnded()
			})
		}
		const commit = async (stop: AbortSignal | undefined): Promise<void> => {
			try {
				await this.commitTransaction(transaction, stop)
				transaction.status = 'committed'
			} catch (error) {
				const cause = toError(error)
				if (transaction.commitOutcomeUnknown) {
					const unknown = new TransactionAbortedError(transactionalId, cause, 'unknown')
					transaction.end(unknown)
					transaction.status = 'error'
					this.enterFatalState(cause, 'EndTxn(commit) was not confirmed: the commit outcome is unknown')
					throw unknown
				}
				transaction.end(new TransactionAbortedError(transactionalId, cause))
				// A failed abort leaves the producer in the 'error' state and is reported by the 'error' event.
				await this.abortTransaction(transaction, stop).catch(() => {})
				transaction.status = this.txnState === 'error' ? 'error' : 'aborted'
				throw (
					this.fence ??
					(error instanceof TransactionAbortedError
						? error
						: new TransactionAbortedError(transactionalId, cause))
				)
			}
		}
		const abort = async (stop: AbortSignal | undefined): Promise<void> => {
			transaction.end()
			try {
				await this.abortTransaction(transaction, stop)
				transaction.status = 'aborted'
			} catch (error) {
				transaction.status = 'error'
				throw error
			}
		}

		const handle: TransactionHandle = {
			signal: transaction.ending.signal,

			send: async (
				topic: string | TopicDefinition<unknown, unknown>,
				messages: ProducerMessage<unknown, unknown> | ProducerMessage<unknown, unknown>[]
			): Promise<SendResult | SendResult[]> => {
				assertSendable()
				const topicDef =
					typeof topic === 'string' ? ({ topic } satisfies TopicDefinition<Buffer, Buffer>) : topic
				const promise = this.trackSend(
					this.tracedSend(topicDef, messages, true, m => this.transactionalSend(transaction, topicDef, m))
				)
				// Even a fire-and-forget failure must poison the transaction so commit aborts.
				promise.catch(error => this.recordTransactionFailure(transaction, error))
				return promise
			},

			sendOffsets: async (
				contextOrParams: ConsumeContext | SendOffsetsParams,
				offsets?: TransactionOffset[]
			): Promise<void> => {
				assertSendable()
				// Like a failed send, a failed sendOffsets() of an open transaction (even one not awaited, also for its
				// arguments) poisons it: committing without the offsets would let the input be processed again.
				const registration = (async () => {
					const params = this.resolveSendOffsetsParams(contextOrParams, offsets)
					const groupId = params.consumerGroupMetadata?.groupId ?? params.groupId
					if (!groupId) {
						throw new Error('sendOffsets requires either groupId or consumerGroupMetadata')
					}
					if (params.consumerGroupMetadata) {
						assertJoinedMember(params.consumerGroupMetadata)
					}
					await this.registerOffsets(transaction, groupId, params)
				})().catch((error: unknown) => {
					this.recordTransactionFailure(transaction, error)
					throw error
				})
				transaction.registrations.add(registration)
				try {
					await registration
				} finally {
					transaction.registrations.delete(registration)
				}
			},

			commit: async (options?: ProducerCallOptions): Promise<void> => {
				if (transaction.status !== 'in_transaction') {
					assertNotFenced()
					throw new InvalidTxnStateError(transactionalId, transaction.status, ['in_transaction'])
				}
				return end('committing', () => bounded('Transaction commit', options, commit))
			},

			abort: async (options?: ProducerCallOptions): Promise<void> => {
				if (transaction.status === 'in_transaction') {
					return end('aborting', () => bounded('Transaction abort', options, abort))
				}
				assertNotFenced()
				if (transaction.status === 'committing' || transaction.status === 'committed') {
					throw new InvalidTxnStateError(transactionalId, transaction.status, ['in_transaction'])
				}
				// Ended (or ending) without a commit: nothing more to do once that settled.
				await bounded('Transaction abort', options, stop => abortable(ended, stop))
				assertNotFenced()
			},
		}
		const abortOnSignal = (): void => void handle.abort().catch(() => {})
		signal?.addEventListener('abort', abortOnSignal, { once: true })

		this.openTransaction = { handle, transaction, ended, producer: this }
		return this.openTransaction
	}

	/**
	 * What `transaction` sends (records, partitions and offsets to add) goes out under the producer id and epoch it
	 * began with, while it accepts sends, or not at all: once the epoch failed (failEpoch), the transaction can only
	 * abort; a fenced producer can do nothing more (Java fails sends in an error state); once the transaction aborts,
	 * a send or registration in progress is rejected. Checked when a send starts, right before each of its requests
	 * and its append, and when a registration was answered; past its start, a commit flushes the sends made before it.
	 */
	private assertLive(transaction: Transaction): void {
		const sameEpoch =
			this.idempotentState === 'running' &&
			this.producerId === transaction.producerId &&
			this.producerEpoch === transaction.producerEpoch
		if (!sameEpoch) {
			throw (
				this.fence ??
				new TransactionAbortedError(transaction.transactionalId, transaction.abortCause ?? undefined)
			)
		}
		if (!transaction.accepting) {
			throw (
				transaction.ending.signal.reason ??
				new InvalidTxnStateError(transaction.transactionalId, transaction.status, ['in_transaction'])
			)
		}
	}

	/**
	 * Send within a transaction - adds partition to transaction first. {@link assertLive} runs right before every
	 * request and append, so that nothing goes out once its producer was fenced or the transaction aborts; an append
	 * without an await before it is covered by the check of the send's start.
	 */
	private async transactionalSend<V, K>(
		transaction: Transaction,
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]> {
		const isSingle = !Array.isArray(messages)
		const msgArray = Array.isArray(messages) ? messages : [messages]

		// No send waiting before this one: resolve the partitions now, and if they are all part of
		// the transaction already, append right away (synchronously after the send's start checked the epoch).
		const partitionCount = this.partitionCountToAppendNow(topicDef.topic)
		const resolvedNow =
			partitionCount === undefined ? undefined : this.resolveMessagePartitions(topicDef, msgArray, partitionCount)
		if (resolvedNow && this.partitionsNotIn(transaction, topicDef.topic, resolvedNow).length === 0) {
			return settleSend(this.enqueueMessages(topicDef, resolvedNow, partitionCount!), isSingle)
		}

		// In send order: a partition is registered by the first send that needs it, and every
		// later send finds it registered instead of issuing its own AddPartitionsToTxn. Nothing
		// is appended to a partition before it is part of the transaction.
		const promises = await this.inSendOrder(topicDef.topic, async () => {
			const resolved =
				resolvedNow ??
				this.resolveMessagePartitions(topicDef, msgArray, await this.getPartitionCount(topicDef.topic))
			const partitionsToAdd = this.partitionsNotIn(transaction, topicDef.topic, resolved)
			if (partitionsToAdd.length > 0) {
				await this.addPartitionsToTransaction(transaction, topicDef.topic, partitionsToAdd)
			}
			return this.append(topicDef, resolved, () => this.assertLive(transaction))
		})

		return settleSend(promises, isSingle)
	}

	/** The partitions of these messages that are not part of `transaction` yet. */
	private partitionsNotIn(transaction: Transaction, topic: string, messages: Array<{ partition: number }>): number[] {
		const partitions = new Set<number>()
		for (const { partition } of messages) {
			if (!transaction.partitions.has(`${topic}:${partition}`)) partitions.add(partition)
		}
		return Array.from(partitions)
	}

	/**
	 * Resolve the partition of each message once and freeze it on the message. Re-invoking a
	 * stateful partitioner (e.g. round-robin) in append would advance the counter, so
	 * addPartitionsToTxn and the actual writes would diverge.
	 */
	private resolveMessagePartitions<V, K>(
		topicDef: TopicDefinition<V, K>,
		msgArray: ProducerMessage<V, K>[],
		partitionCount: number
	): Array<ProducerMessage<V, K> & { partition: number }> {
		const { encodeKey, encodeValue } = this.resolveEncoders(topicDef)
		return msgArray.map(msg => {
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
			return { ...msg, partition }
		})
	}

	/**
	 * An AddPartitionsToTxn or AddOffsetsToTxn of `transaction` (`request` sends it and throws for an error it is
	 * answered), retried with {@link coordinatorRequest}; {@link assertLive} runs before every attempt. Once the
	 * transaction aborts, no attempt starts any more; the answer of one in flight is awaited, so that the transaction
	 * knows whether the coordinator began it (`started`, or `addUnanswered` when the connection failed). Then, or once
	 * the epoch failed, it rejects like a send of the transaction, also when that attempt failed.
	 */
	private async addToTransaction(
		transaction: Transaction,
		request: (coordinator: Broker) => Promise<void>
	): Promise<void> {
		try {
			await this.coordinatorRequest(
				'TRANSACTION',
				transaction.transactionalId,
				async coordinator => {
					this.assertLive(transaction)
					await request(coordinator)
				},
				transaction.ending.signal
			)
		} catch (error) {
			this.assertLive(transaction)
			throw error
		}
		// Of this transaction even when answered once it aborts: then what it added is not used (assertLive).
		transaction.started = true
	}

	/** Add partitions to `transaction` (AddPartitionsToTxn, see {@link addToTransaction}). */
	private async addPartitionsToTransaction(
		transaction: Transaction,
		topic: string,
		partitions: number[]
	): Promise<void> {
		const { transactionalId, producerId, producerEpoch } = transaction
		await this.addToTransaction(transaction, async coordinator => {
			const response = await transaction.answerToAdd(
				coordinator.addPartitionsToTxn({
					transactionalId,
					producerId,
					producerEpoch,
					topics: [{ name: topic, partitions }],
				})
			)

			// A top-level error (v4+) means the broker rejected the whole request; the
			// per-partition results are empty/meaningless, so check it before they are.
			if (response.errorCode !== ErrorCode.None) {
				throw this.coordinatorError(
					response.errorCode,
					`AddPartitionsToTxn failed for transaction ${transactionalId}`
				)
			}

			for (const topicResult of response.results) {
				for (const partitionResult of topicResult.resultsByPartition) {
					if (partitionResult.errorCode !== ErrorCode.None) {
						throw this.coordinatorError(
							partitionResult.errorCode,
							`AddPartitionsToTxn failed for ${topicResult.name}-${partitionResult.partitionIndex}`
						)
					}
				}
			}
		})

		for (const partition of partitions) {
			transaction.partitions.add(`${topic}:${partition}`)
		}
		this.logger.debug('added partitions to transaction', { transactionalId, topic, partitions })
	}

	/**
	 * Resolve the public context-bound API into the existing low-level offset commit shape.
	 */
	private resolveSendOffsetsParams(
		contextOrParams: ConsumeContext | SendOffsetsParams,
		offsets?: TransactionOffset[]
	): SendOffsetsParams {
		if ('offsets' in contextOrParams) {
			if (offsets !== undefined) {
				throw new Error('sendOffsets does not accept a second offsets argument with SendOffsetsParams')
			}
			return contextOrParams
		}

		const resolvedOffsets = offsets ?? [
			{
				topic: contextOrParams.topic,
				partition: contextOrParams.partition,
				offset: contextOrParams.offset + 1n,
			},
		]

		if (contextOrParams.consumerGroupMetadata) {
			return { consumerGroupMetadata: contextOrParams.consumerGroupMetadata, offsets: resolvedOffsets }
		}

		return { groupId: contextOrParams.groupId, offsets: resolvedOffsets }
	}

	/**
	 * Register the offsets of a group with `transaction` (AddOffsetsToTxn, see {@link addToTransaction}); they are
	 * committed with TxnOffsetCommit when it commits. An answer that arrives once the transaction aborted adds no
	 * offsets: this rejects with `TransactionAbortedError`.
	 */
	private async registerOffsets(transaction: Transaction, groupId: string, params: SendOffsetsParams): Promise<void> {
		const { transactionalId, producerId, producerEpoch } = transaction
		await this.addToTransaction(transaction, async coordinator => {
			const response = await transaction.answerToAdd(
				coordinator.addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId })
			)
			if (response.errorCode !== ErrorCode.None) {
				throw this.coordinatorError(response.errorCode, `AddOffsetsToTxn failed for group ${groupId}`)
			}
		})
		// A late answer: the transaction aborted (or its epoch failed) while the request was in flight.
		this.assertLive(transaction)

		const existing = transaction.offsets.get(groupId)
		transaction.offsets.set(groupId, {
			offsets: [...(existing?.offsets ?? []), ...params.offsets],
			groupMetadata: params.consumerGroupMetadata ?? existing?.groupMetadata,
		})

		this.logger.debug('prepared offsets for transaction commit', {
			transactionalId,
			groupId,
			offsetCount: params.offsets.length,
			hasGroupMetadata: !!params.consumerGroupMetadata,
		})
	}

	/**
	 * A request to the transaction or group coordinator, retried with {@link transactionalRetryOptions}. The coordinator
	 * is looked up for every attempt and forgotten after a coordinator error or a connection failure, so that a retry
	 * finds it where it moved. A fence ({@link coordinatorError}) makes the producer fatal at once.
	 *
	 * Once `stop` aborted, no lookup or backoff is awaited and no attempt starts any more: it rejects with the reason
	 * of `stop`. A request in flight then is awaited, unless `request` bounds it by `stop` itself (EndTxn and
	 * TxnOffsetCommit, whose callers are bounded; a registration of a transaction waits for its answer).
	 */
	private coordinatorRequest<T>(
		type: 'TRANSACTION' | 'GROUP',
		key: string,
		request: (coordinator: Broker) => Promise<T>,
		stop?: AbortSignal
	): Promise<T> {
		const attempt = async (): Promise<T> => {
			const coordinator = await abortable(this.cluster.getCoordinator(type, key), stop)
			stop?.throwIfAborted()
			try {
				return await request(coordinator)
			} catch (error) {
				if (error instanceof ProducerFencedError) {
					this.failEpoch(error)
				}
				if (
					isConnectionFailure(error) ||
					(error instanceof KafkaProtocolError && isCoordinatorErrorCode(error.errorCode))
				) {
					this.cluster.invalidateCoordinator(type, key)
				}
				throw error
			}
		}
		return retry(attempt, this.transactionalRetryOptions(stop))
	}

	/**
	 * Retry options for transactional coordinator requests: retriable errors and connection failures are retried
	 * (`retries`, bounded by maxBlockMs) until `stop` aborts, which ends a backoff at once (the next attempt then
	 * rejects with its reason). CONCURRENT_TRANSACTIONS
	 * is polled fast and outside the retry budget: the broker returns it while the
	 * previous transaction's markers are still being written, which clears in
	 * milliseconds — waiting retryBackoffMs would floor every back-to-back
	 * transaction at ~100ms, and burning attempts on it would let a slow marker
	 * write exhaust the budget. maxBlockMs bounds the poll.
	 */
	private transactionalRetryOptions(stop: AbortSignal | undefined): RetryOptions {
		return {
			...createRetryStrategyOptions(this.config),
			maxElapsedMs: this.config.maxBlockMs,
			signal: stop,
			resolveOnAbort: true,
			shouldRetry: error => !stop?.aborted && (isRetriable(error) || isConnectionFailure(error)),
			freeRetry: error =>
				error instanceof KafkaProtocolError && error.errorCode === ErrorCode.ConcurrentTransactions
					? CONCURRENT_TRANSACTIONS_POLL_MS
					: undefined,
		}
	}

	/**
	 * Remember the first definitive failure of a send or offset registration of `transaction` so that its commit
	 * aborts instead of committing a partial transaction (Java abortable-error parity). Rejections that arrive once
	 * its abort is underway are expected and not recorded.
	 */
	private recordTransactionFailure(transaction: Transaction, error: unknown): void {
		if (!transaction.accepting) {
			return
		}
		transaction.abortCause ??= toError(error)
	}

	/**
	 * Commit `transaction`: flush its sends, wait for its offset registrations, commit its offsets, EndTxn(commit).
	 * Aborting `stop` rejects with its reason and sends nothing more.
	 */
	private async commitTransaction(transaction: Transaction, stop: AbortSignal | undefined): Promise<void> {
		if (this.txnState !== 'in_transaction') {
			throw this.fence ?? new InvalidTxnStateError(transaction.transactionalId, this.txnState, ['in_transaction'])
		}

		this.txnState = 'committing'

		// The sends and offset registrations made before the commit (a send may still add its partitions): their
		// offsets are committed, their failures abort.
		await abortable(Promise.all([this.flush(), Promise.allSettled(transaction.registrations)]), stop)

		// A send or registration failed definitively (or the epoch failed) — committing now would
		// expose a partial transaction. Surface the failure so the transaction aborts instead.
		if (transaction.abortCause) {
			throw transaction.abortCause
		}

		for (const [groupId, { offsets, groupMetadata }] of transaction.offsets) {
			await this.commitTransactionOffsets(transaction, groupId, offsets, groupMetadata, stop)
		}

		// The coordinator never began the transaction (nothing produced, no offsets added), and classic coordinators
		// reject EndTxn for it with INVALID_TXN_STATE — just reset local state.
		if (!transaction.started) {
			this.txnState = 'idle'
			return
		}

		await this.endTransaction(transaction, true, stop)

		// debug, not info: EOS loops commit per batch, so this fires at batch rate.
		this.logger.debug('transaction committed', {
			transactionalId: transaction.transactionalId,
			partitionCount: transaction.partitions.size,
			groupCount: transaction.offsets.size,
		})
		this.txnState = 'idle'
	}

	/**
	 * Abort `transaction`: reject its sends not taken for sending yet (Java's abortUndrainedBatches, also for those
	 * queued while it aborts: see drainPendingBatches), wait until none of its sends, produce requests and offset
	 * registrations is in progress (none starts a request any more: the transaction's signal aborted; Java holds EndTxn
	 * back while a batch is incomplete), EndTxn(abort) if the coordinator may have begun it, then, if the epoch failed
	 * (failEpoch), bump it: InitProducerId with the producer id and epoch (KIP-360, Java's `beginCompletingTransaction`),
	 * which the coordinator accepts once EndTxn confirmed the epoch current and the transaction no longer ongoing. A
	 * disconnect() skips the bump, also one in progress: the transaction aborted and the producer goes away. If the abort
	 * does not finish (EndTxn or the bump fails, or `stop` aborts), the producer enters the 'error' state and this
	 * rejects. In the 'error' state, also when a fence met while waiting put it there, it sends nothing more and rejects
	 * with what put it there.
	 */
	private async abortTransaction(transaction: Transaction, stop: AbortSignal | undefined): Promise<void> {
		// A closure: the state assigned below can still turn 'error' while this waits.
		const assertNotFatal = (): void => {
			// Fatal (a fence, with a transaction open): nothing is sent, the coordinator aborted it (Java throws too).
			if (this.txnState === 'error') {
				throw (
					this.fatalError ??
					new InvalidTxnStateError(transaction.transactionalId, 'error', ['in_transaction'])
				)
			}
		}
		assertNotFatal()
		if (this.txnState !== 'in_transaction' && this.txnState !== 'committing') {
			// Already idle
			return
		}

		this.txnState = 'aborting'
		// Queued batches hold no sequence yet: dropping them leaves no gap.
		this.rejectAndClearPendingBatches(toError(transaction.ending.signal.reason))

		try {
			// A transactional ProduceRequest must not race the abort marker, and EndTxn follows the answers to the
			// registrations, which tell whether the coordinator began the transaction. A drain or a send's retry that
			// settles may start the next wait. Failures are expected — the sends are being aborted.
			for (;;) {
				const pending = [...this.activeSends, ...this.inflight, ...transaction.registrations]
				if (pending.length === 0) break
				await abortable(Promise.allSettled(pending), stop)
			}
			assertNotFatal()

			// Unless the coordinator may have begun the transaction, it has none to abort, and classic coordinators reject
			// EndTxn for it with INVALID_TXN_STATE — just reset local state. Unless the epoch failed: then only EndTxn's
			// answer can confirm it before the bump.
			const epochFailed = this.idempotentState === 'fenced'
			if (transaction.started || transaction.addUnanswered || epochFailed) {
				try {
					await this.endTransaction(transaction, false, stop)
				} catch (error) {
					// Not begun (an add that got no answer was not applied): nothing to abort, and the epoch is current.
					const notBegun =
						error instanceof KafkaProtocolError && error.errorCode === ErrorCode.InvalidTxnState
					if (transaction.started || !notBegun) throw error
				}
			}
			if (epochFailed) {
				try {
					await abortable(this.initializeIdempotentProducer(stop), stop)
				} catch (error) {
					// Stopped by disconnect(), which skips the bump: the transaction aborted, and the producer goes away.
					if (error !== this.closing.signal.reason) throw error
				}
			}

			this.txnState = 'idle'
			this.logger.debug('transaction aborted', {
				transactionalId: transaction.transactionalId,
			})
		} catch (error) {
			// The abort marker was never written (or the epoch it confirmed was not bumped): resetting to idle would let
			// the open transaction silently merge into the next one, or the next one fail. Enter a fatal state instead
			// (Java FATAL_ERROR parity) so the next transaction refuses to start.
			this.enterFatalState(toError(error), 'the abort did not complete')
			throw error
		}
	}

	/**
	 * EndTxn with the transaction's producer id and epoch, resent on retriable failures: the broker answers a resent
	 * commit of a committed transaction with success. A commit request that was sent and not answered (connection lost,
	 * or `stop` aborted while it was in flight) may have been applied: `commitOutcomeUnknown` records that.
	 */
	private async endTransaction(
		transaction: Transaction,
		committed: boolean,
		stop: AbortSignal | undefined
	): Promise<void> {
		const { transactionalId, producerId, producerEpoch } = transaction
		await this.coordinatorRequest(
			'TRANSACTION',
			transactionalId,
			async coordinator => {
				let response: Awaited<ReturnType<Broker['endTxn']>>
				try {
					response = await abortable(
						coordinator.endTxn({ transactionalId, producerId, producerEpoch, committed }),
						stop
					)
				} catch (error) {
					if (committed) transaction.commitOutcomeUnknown = true
					throw error
				}
				if (response.errorCode !== ErrorCode.None) {
					throw this.coordinatorError(response.errorCode, `EndTxn ${committed ? 'commit' : 'abort'} failed`)
				}
			},
			stop
		)
	}

	/** Enter the fatal transaction state: no transaction can begin any more; the producer must be replaced. */
	private enterFatalState(cause: Error, what: string): void {
		if (this.txnState === 'error') return
		this.txnState = 'error'
		this.fatalError = cause
		this.logger.error(`${what} — producer entering fatal transaction state`, { error: cause.message })
		// Emit error so operators can monitor fatal transaction failures
		this.emitError(cause)
	}

	/** The fence that made this producer fatal, if one did: what its later calls fail with. */
	private get fence(): ProducerFencedError | undefined {
		return this.fatalError instanceof ProducerFencedError ? this.fatalError : undefined
	}

	/**
	 * An error code a coordinator answered about this producer. PRODUCER_FENCED and INVALID_PRODUCER_EPOCH (from older
	 * coordinators) mean its epoch is no longer the transactional id's: a `ProducerFencedError` (Java parity).
	 */
	private coordinatorError(errorCode: ErrorCode, context: string): KafkaProtocolError | ProducerFencedError {
		if (errorCode !== ErrorCode.ProducerFenced && errorCode !== ErrorCode.InvalidProducerEpoch) {
			return new KafkaProtocolError(errorCode, context)
		}
		const cause = new KafkaProtocolError(errorCode, context)
		const fenced = new ProducerFencedError(
			this.producerId,
			this.producerEpoch,
			`Producer ${this.producerId} (epoch ${this.producerEpoch}) has been fenced: ${cause.message}`
		)
		fenced.cause = cause
		return fenced
	}

	/**
	 * Commit offsets of `transaction` with TxnOffsetCommit; aborting `stop` rejects with its reason, also while the
	 * request is in flight.
	 */
	private async commitTransactionOffsets(
		transaction: Transaction,
		groupId: string,
		offsets: TransactionOffset[],
		groupMetadata: GroupMetadata | undefined,
		stop: AbortSignal | undefined
	): Promise<void> {
		// One offset per (topic, partition): the highest, and the last one given for it (its metadata is the latest).
		const deduped = new Map<string, TransactionOffset>()
		for (const offset of offsets) {
			const key = `${offset.topic}:${offset.partition}`
			const existing = deduped.get(key)
			if (!existing || offset.offset >= existing.offset) {
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
		await this.coordinatorRequest(
			'GROUP',
			groupId,
			async coordinator => {
				const response = await abortable(
					coordinator.txnOffsetCommit({
						transactionalId: transaction.transactionalId,
						groupId,
						producerId: transaction.producerId,
						producerEpoch: transaction.producerEpoch,
						generationId,
						memberId,
						groupInstanceId,
						topics: Array.from(topicMap.entries()).map(([name, partitions]) => ({
							name,
							partitions,
						})),
					}),
					stop
				)

				for (const topic of response.topics) {
					for (const partition of topic.partitions) {
						if (partition.errorCode !== ErrorCode.None) {
							throw this.coordinatorError(
								partition.errorCode,
								`TxnOffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
							)
						}
					}
				}
			},
			stop
		)
	}

	/**
	 * The number of partitions of a topic, from this producer's metadata cache (kept for 5 minutes), looked up on a
	 * miss. Rejects when the topic does not exist, with a `TimeoutError` once `timeoutMs` passed, or with the reason of
	 * `signal`.
	 */
	async partitionCount(topic: string, options?: ProducerCallOptions): Promise<number> {
		return (
			this.cachedPartitionCount(topic) ??
			bounded(`Partition count lookup of ${topic}`, options, stop =>
				abortable(this.lookUpPartitionCount(topic, stop), stop)
			)
		)
	}

	/** The partition count of a topic. Must run inside {@link inSendOrder}. */
	private async getPartitionCount(topic: string): Promise<number> {
		const cached = this.cachedPartitionCount(topic)
		if (cached !== undefined) {
			return cached
		}

		// A failed lookup also answers the sends that are waiting for this topic right now.
		const asOf = this.sendsQueued
		try {
			return await this.lookUpPartitionCount(topic)
		} catch (error) {
			this.failedLookups.set(topic, { asOf, error })
			throw error
		}
	}

	/** Look the partition count up in fresh metadata; once `stop` aborted, no request is sent any more. */
	private async lookUpPartitionCount(topic: string, stop?: AbortSignal): Promise<number> {
		// Topic creation is asynchronous; even after CreateTopics returns successfully, the topic may not be
		// immediately visible in Metadata. Retry a few times before failing.
		const maxAttempts = 5
		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			stop?.throwIfAborted()
			const metadata = await this.cluster.refreshMetadata([topic])
			const topicMeta = metadata.topics.get(topic)
			if (topicMeta) {
				const count = topicMeta.partitions.size
				this.partitionCache.set(topic, { count, fetchedAt: Date.now() })
				return count
			}

			if (attempt < maxAttempts) {
				await sleepMs(100 * attempt, { signal: stop, resolveOnAbort: true })
			}
		}

		throw new Error(`Topic ${topic} not found`)
	}

	/** The cached partition count of a topic, if it is still fresh. */
	private cachedPartitionCount(topic: string): number | undefined {
		const cached = this.partitionCache.get(topic)
		if (cached && Date.now() - cached.fetchedAt < Producer.PARTITION_CACHE_TTL_MS) {
			return cached.count
		}
		return undefined
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

		// If in error state, fail immediately (with the failure that put it there)
		if (this.idempotentState === 'error') {
			throw this.fatalError ?? new Error('Idempotent producer is in error state')
		}

		// If initializing, wait for existing promise
		if (this.idempotentState === 'initializing' && this.initPromise) {
			await this.initPromise
			return
		}

		// If the epoch failed (only an idempotent producer gets here then: a transactional one bumps it when its
		// transaction aborts), trigger re-initialization
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
	 * Initialize idempotent producer by obtaining producer ID from broker: InitProducerId with the producer's identity
	 * once it has one, which bumps its epoch. A transactional producer initializes when its first transaction begins
	 * and bumps the epoch only in {@link abortTransaction}.
	 *
	 * Transitions: idle/fenced -> initializing -> running, error (a non-retriable failure), or back to idle/fenced (a
	 * timeout, a disconnect or `signal`: the next use initializes again)
	 */
	private async initializeIdempotentProducer(signal?: AbortSignal): Promise<void> {
		const from = this.idempotentState
		this.idempotentState = 'initializing'
		this.logger.debug('initializing idempotent producer')

		this.initPromise = this.doInitializeIdempotentProducer(
			from,
			signal ? AbortSignal.any([signal, this.closing.signal]) : this.closing.signal
		)

		try {
			await this.initPromise
		} finally {
			this.initPromise = null
		}
	}

	/**
	 * Actual initialization logic - separated for proper promise handling. Past maxBlockMs it rejects with a
	 * TimeoutError and goes back to `from`, so that the next use tries again (Java: `initTransactions()` may be called
	 * again after its TimeoutException); so it does once `stop` aborted, with its reason.
	 */
	private async doInitializeIdempotentProducer(from: IdempotentState, stop: AbortSignal): Promise<void> {
		// Retry coordinator discovery + InitProducerId until maxBlockMs, riding through a loading or
		// briefly-unreachable coordinator. Non-retriable protocol errors fail fast (Java: a fatal
		// TransactionManager error). maxBlockMs bounds the whole of it, the requests in flight and the
		// backoffs included, like Java's initTransactions(); an InitProducerId it cuts short stays pending
		// for the next initialization. Once given up (the deadline, or `stop`: a disconnect), an attempt
		// still looking up the coordinator sends nothing: a producer given up must not bump the epoch of
		// its transactional id behind a successor with the same id.
		const deadlineAt = Date.now() + this.config.maxBlockMs
		const timeLeft = () => deadlineAt - Date.now()
		const givenUp = deadlineSignal(this.config.maxBlockMs, stop)
		const backoff = new ReconnectionStrategy({
			initialDelayMs: this.config.retryBackoffMs,
			maxDelayMs: this.config.maxRetryBackoffMs,
			maxAttempts: Number.MAX_SAFE_INTEGER,
		})
		let lastError: Error = new Error('no attempt answered in time')
		const retryAfter = async (error: Error): Promise<void> => {
			lastError = error
			const delay = backoff.nextDelay()
			backoff.recordFailure()
			await sleepMs(Math.min(delay, Math.max(0, timeLeft())), { signal: givenUp.signal }).catch(() => {})
		}
		const maySend = (): boolean => !givenUp.signal.aborted && timeLeft() > 0

		try {
			while (maySend()) {
				const attempt = (this.pendingInitAttempt ??= this.initAttempt(maySend))
				let outcome: InitAttemptOutcome
				try {
					outcome = await abortable(attempt.outcome, givenUp.signal)
				} catch {
					// Given up while it runs: an InitProducerId sent stays pending, for the next initialization to await
					// first; an attempt still looking up the coordinator sends nothing.
					if (!attempt.sent) this.pendingInitAttempt = null
					break
				}
				this.pendingInitAttempt = null
				if ('abandoned' in outcome) break
				if ('error' in outcome) {
					const { error } = outcome
					// A broker's non-retriable answer to FindCoordinator (an authorization failure): fatal.
					if (error instanceof KafkaProtocolError && !error.retriable) {
						this.failInitialization(error)
					}
					// Transport failure: re-discover the coordinator and retry until the deadline.
					if (this.config.transactionalId) {
						this.cluster.invalidateCoordinator('TRANSACTION', this.config.transactionalId)
					}
					await retryAfter(toError(error))
					continue
				}
				const { response } = outcome

				// Protocol-level error handling: a retriable code is retried, any other one fails fast.
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
						await retryAfter(new KafkaProtocolError(errorCode, 'InitProducerId failed'))
						continue
					}

					// Authorization failures, a fence and the other non-retriable errors: fail fast.
					this.failInitialization(this.coordinatorError(errorCode, 'InitProducerId failed'))
				}

				// Success - store producer identity
				this.producerId = response.producerId
				this.producerEpoch = response.producerEpoch

				// Reset sequences on (re)initialization
				this.sequences.clear()
				this.reservedSequences.clear()

				this.accumulator.setFenced(false)

				this.idempotentState = 'running'
				this.logger.info('idempotent producer initialized', {
					producerId: this.producerId.toString(),
					producerEpoch: this.producerEpoch,
					transactionalId: this.config.transactionalId,
				})
				return
			}
		} finally {
			givenUp.clear()
		}

		this.idempotentState = from
		if (stop.aborted) throw toError(stop.reason)
		const id = this.config.transactionalId
		const timeout = new TimeoutError(
			`InitProducerId${id ? ` of transactional id '${id}'` : ''} did not complete within maxBlockMs: ${lastError.message}`,
			this.config.maxBlockMs
		)
		timeout.cause = lastError
		throw timeout
	}

	/**
	 * One coordinator lookup and InitProducerId with the producer's identity at the time it is sent. Once the lookup
	 * answered, `maySend()` (whether the initialization still wants it: not given up, its deadline not passed) is checked
	 * right before InitProducerId: an abandoned lookup starts no request.
	 */
	private initAttempt(maySend: () => boolean): InitAttempt {
		let sent = false
		const outcome = (async (): Promise<InitAttemptOutcome> => {
			try {
				const broker = this.config.transactionalId
					? // Transactional: must use transaction coordinator
						await this.cluster.getCoordinator('TRANSACTION', this.config.transactionalId)
					: // Non-transactional: any broker works
						await (this.cluster as unknown as { getAnyBroker: () => Promise<Broker> }).getAnyBroker()
				if (!maySend()) return { abandoned: true }
				sent = true
				const response = await broker.initProducerId({
					transactionalId: this.config.transactionalId,
					transactionTimeoutMs: this.config.transactionalId ? this.config.transactionTimeoutMs : 0,
					producerId: this.producerId !== -1n ? this.producerId : undefined,
					producerEpoch: this.producerEpoch !== -1 ? this.producerEpoch : undefined,
				})
				return { response }
			} catch (error) {
				return { error }
			}
		})()
		return {
			outcome,
			get sent() {
				return sent
			},
		}
	}

	/** A non-retriable initialization failure: the producer stays in the error state, which rethrows it. */
	private failInitialization(error: KafkaError): never {
		this.idempotentState = 'error'
		this.fatalError = error
		throw error
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
			this.logger.debug('sequence wrap - splitting batch', {
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

		const transactionConcurrency = config.transactionConcurrency ?? 1
		if (!Number.isInteger(transactionConcurrency) || transactionConcurrency < 1) {
			throw new Error('transactionConcurrency must be an integer >= 1')
		}
		if (transactionConcurrency > 1 && !isTransactional) {
			throw new Error('transactionConcurrency requires transactionalId to be configured')
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
			transactionConcurrency,
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
