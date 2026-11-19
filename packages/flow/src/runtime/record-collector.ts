/**
 * The output of one producer incarnation (KS `RecordCollector`): sends of every task, deferred acknowledgements,
 * per-task failure attribution and changelog positions, and the open transaction under exactly-once.
 */
import type { ConsumerGroupMetadata, Logger, SendResult } from '@kafkats/client'
import { TimeoutError, TransactionAbortedError } from '@kafkats/client'
import { FailedProcessingError, IllegalStateError, StreamsError, TaskMigratedError } from '@/api/errors.js'
import type { ErrorHandlerContext } from '@/api/handlers.js'
import { applyHandler } from '@/processor/node.js'
import type { CollectedRecord, SendKind, TaskOutput } from '@/processor/output.js'
import type { PartitionCountCache, ResolvedHandlers } from '@/runtime/config.js'
import { errorMessage, isFatalSendError, isFencedError, isRetriableError } from '@/runtime/errors.js'
import type { Clock, OffsetAndMetadata, ProducedMessage, ProducerPort, TransactionPort } from '@/runtime/io.js'
import { delay, raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StreamTask } from '@/runtime/task.js'

/** The input record a task was processing when it sent. */
type Input = StreamTask['input']

export interface RecordCollectorOptions {
	readonly eos: boolean
	readonly handlers: ResolvedHandlers
	readonly counts: PartitionCountCache
	/** Exactly-once: how long an initialization of the producer that times out is tried again (FlowConfig.taskTimeoutMs). */
	readonly taskTimeoutMs: number
	readonly clock: Clock
	readonly logger: Logger
}

/** Backoff between the initializations of a producer that timed out: Kafka Streams' task initialization backoff. */
const INIT_RETRY_INITIAL_BACKOFF_MS = 1000
const INIT_RETRY_MAX_BACKOFF_MS = 10_000

/**
 * The sends of one generation of the collector that were not answered yet. A generation ends when its sends are given
 * up (abort, discard, close): their late answers are then ignored. A counter, not a promise per send: acknowledgements
 * are deferred to the next flush, like the producer's own `flush()` in Kafka Streams.
 */
class Generation {
	private pending = 0
	private waiters: (() => void)[] = []
	/** The first run-wide failure: every later flush throws it, until the generation ends. */
	failure: { error: unknown } | undefined

	sent(): void {
		this.pending++
	}

	answered(): void {
		if (--this.pending === 0) {
			const waiters = this.waiters
			this.waiters = []
			for (const resolve of waiters) resolve()
		}
	}

	/** Resolves once every send of the generation, including those made meanwhile, was answered. */
	settled(): Promise<void> {
		return this.pending === 0 ? Promise.resolve() : new Promise(resolve => this.waiters.push(resolve))
	}
}

export class RecordCollector {
	private generation = new Generation()
	private isClosed = false
	/** Aborted at close(): partition count loads of this collector. */
	private readonly lifetime = new AbortController()
	private transaction: TransactionPort | undefined
	private opening: Promise<TransactionPort> | undefined
	/** Exactly-once: the producer's initialization, once per collector (a failed one is retried). */
	private initializing: Promise<void> | undefined
	/** Every abort so far (none rejects): the producer runs one transaction at a time, the next begins after them. */
	private aborting: Promise<unknown> | undefined
	/** Exactly-once: the last transaction was aborted and none was opened since, so the work done meanwhile is void. */
	private aborted = false
	/** Last acked offset per changelog partition, per task, since the last take. */
	private readonly changelogOffsets = new Map<StreamTask, Map<string, bigint>>()

	constructor(
		private readonly producer: ProducerPort,
		private readonly options: RecordCollectorOptions
	) {}

	/** The output of one task incarnation. */
	forTask(task: StreamTask): TaskOutput {
		const { counts } = this.options
		return {
			send: (kind, record) => this.send(task, kind, record),
			partitionCount: topic => counts.get(topic),
			loadPartitionCount: topic => this.loadPartitionCount(topic),
			close: () => this.changelogOffsets.delete(task),
		}
	}

	/**
	 * Exactly-once: initializes the producer once (KS `RecordCollector.initialize` at task initialization): started by
	 * the Run with the collector, awaited by every restore before it reads a changelog. A new producer with the
	 * transactional id of an earlier one fences it when it initializes, which aborts the transaction it left open:
	 * until then that transaction's records hold back the last stable offset of every changelog it wrote, and with it
	 * every restore that reads one, and its pending offsets hold back the group's offset fetch.
	 */
	async initialize(signal: AbortSignal): Promise<void> {
		if (!this.options.eos || this.isClosed) {
			return
		}
		this.initializing ??= this.initTransactions().catch((error: unknown) => {
			this.initializing = undefined
			throw error
		})
		await raceSignal(this.initializing, signal)
	}

	/**
	 * The producer's initialization, each attempt blocking up to the producer's `maxBlockMs` (KS
	 * `StreamsProducer.initTransaction`). An attempt that times out is tried again like Kafka Streams tries a task's
	 * initialization again (`TaskManager.addTaskToStateUpdater`): after a backoff, until `taskTimeoutMs` passed since the
	 * first timeout, then a TaskMigratedError. Stops with close().
	 */
	private async initTransactions(): Promise<void> {
		const { taskTimeoutMs, clock, logger } = this.options
		const { signal } = this.lifetime
		let deadline: number | undefined
		for (
			let backoff = INIT_RETRY_INITIAL_BACKOFF_MS;
			;
			backoff = Math.min(backoff * 2, INIT_RETRY_MAX_BACKOFF_MS)
		) {
			try {
				await this.producer.initTransactions({ signal })
				return
			} catch (error) {
				if (!(error instanceof TimeoutError) || signal.aborted) {
					throw error
				}
				const now = clock.now()
				deadline ??= now + taskTimeoutMs
				if (now > deadline) {
					throw new TaskMigratedError(
						`the exactly-once producer did not initialize within taskTimeoutMs (${taskTimeoutMs} ms): ${error.message}`,
						{ cause: error }
					)
				}
				logger.warn('initializing the exactly-once producer timed out: it is tried again', {
					error: error.message,
					retryInMs: backoff,
					giveUpInMs: deadline - now,
				})
				await delay(clock, backoff, signal)
			}
		}
	}

	/** Exactly-once: begins a transaction unless one is open; awaited before any slice that may send. */
	async ensureTransaction(signal: AbortSignal): Promise<void> {
		if (!this.options.eos || this.transaction !== undefined || this.isClosed) {
			return
		}
		const { generation } = this
		const transaction = await (this.opening ??= this.begin(signal))
		// An abort or close meanwhile owns the transaction it found opening.
		if (generation === this.generation && this.transaction === undefined) {
			this.transaction = transaction
			this.aborted = false
		}
	}

	get transactionOpen(): boolean {
		return this.transaction !== undefined
	}

	/**
	 * Awaits every send made so far, including those made while flushing. Throws the first run-wide failure, and
	 * TaskMigratedError when the sends were given up meanwhile (an abort, a discard or a close): their outcome is
	 * void, so nothing may be committed for them. A send that failed with a task-scoped retriable error under
	 * at-least-once marked its task corrupted instead (StreamTask.corrupted).
	 */
	async flush(signal: AbortSignal): Promise<void> {
		const { generation } = this
		await raceSignal(generation.settled(), signal)
		if (generation !== this.generation) {
			throw new TaskMigratedError('the sends were given up while they were flushed')
		}
		if (generation.failure) {
			throw generation.failure.error
		}
	}

	/** Last acked changelog offset per changelog partition written by `task` since the last take. */
	takeChangelogOffsets(task: StreamTask): Map<string, bigint> {
		const offsets = this.changelogOffsets.get(task) ?? new Map<string, bigint>()
		this.changelogOffsets.delete(task)
		return offsets
	}

	/**
	 * Exactly-once: sends the offsets in the transaction (opening one for offsets alone if none is) and commits it.
	 * Throws TaskMigratedError when an abort or a close took the transaction meanwhile: its records are void, so are
	 * the offsets of the records that sent them.
	 */
	async commitTransaction(
		offsets: readonly OffsetAndMetadata[],
		group: ConsumerGroupMetadata,
		signal: AbortSignal
	): Promise<void> {
		if (!this.aborted && this.transaction === undefined && offsets.length === 0) {
			return
		}
		if (!this.aborted) {
			await this.ensureTransaction(signal)
		}
		const { transaction } = this
		if (transaction === undefined) {
			throw new TaskMigratedError('the transaction was aborted or closed while it was committed')
		}
		if (offsets.length > 0) {
			await raceSignal(transaction.sendOffsets({ consumerGroupMetadata: group, offsets }), signal)
		}
		await transaction.commit({ signal })
		// An abort that took it over meanwhile may have let a new one open: that one stays.
		if (this.transaction === transaction) {
			this.transaction = undefined
		}
	}

	/**
	 * Exactly-once: gives up the sends made so far, waits until each was answered (a request answered after the abort
	 * marker would land outside any transaction), then aborts. Never throws.
	 */
	async abortTransaction(signal: AbortSignal): Promise<void> {
		this.aborted = true
		const answered = this.giveUp()
		// The abort owns the transaction it finds open or opening: no later caller may use it.
		const { opening, transaction } = this
		this.opening = undefined
		this.transaction = undefined
		const abort = (async () => {
			try {
				const aborted = transaction ?? (opening && (await raceSignal(opening, signal)))
				await raceSignal(answered, signal)
				await aborted?.abort({ signal })
			} catch (error) {
				this.options.logger.warn('aborting the transaction failed', { error: errorMessage(error) })
			}
		})()
		this.aborting = Promise.all([this.aborting, abort])
		await abort
	}

	/** At-least-once dirty: forgets the sends made so far and their failures; late answers are ignored. */
	discard(): void {
		void this.giveUp()
	}

	/** Later sends are dropped with a warning, late answers ignored. Does not close the producer. */
	close(): void {
		this.isClosed = true
		this.lifetime.abort()
		void this.giveUp()
	}

	/** Begins a transaction once every abort so far finished. */
	private begin(signal: AbortSignal): Promise<TransactionPort> {
		const { aborting } = this
		const opening: Promise<TransactionPort> = (async () => {
			if (aborting) {
				await raceSignal(aborting, signal)
			}
			return this.producer.beginTransaction({ signal })
		})().finally(() => {
			if (this.opening === opening) {
				this.opening = undefined
			}
		})
		return opening
	}

	/** Ends the current generation; resolves once its sends were answered. */
	private giveUp(): Promise<void> {
		const ended = this.generation
		this.generation = new Generation()
		this.changelogOffsets.clear()
		return ended.settled()
	}

	/** A load cut short by close() is runtime control (migrate), not a processing failure of the record. */
	private async loadPartitionCount(topic: string): Promise<number> {
		try {
			return await this.options.counts.load(topic, this.lifetime.signal)
		} catch (error) {
			if (this.isClosed) {
				throw new TaskMigratedError(`the output closed while loading the partition count of '${topic}'`, {
					cause: error,
				})
			}
			throw error
		}
	}

	private send(task: StreamTask, kind: SendKind, record: CollectedRecord): void {
		const { eos } = this.options
		if (this.isClosed || task.lost || !(task.state === 'RUNNING' || task.initializing)) {
			this.dropped('dropped a record of a task that is not running', task, kind, record)
			return
		}
		if (eos && this.transaction === undefined) {
			// A slice that outlived an abort (a close or recovery that stopped waiting for it): its work is void.
			if (this.aborted) {
				this.dropped('dropped a record sent after its transaction was aborted', task, kind, record)
				return
			}
			throw new IllegalStateError(`task ${task.id.key} sent a record with no open transaction`, {
				taskId: task.id,
			})
		}
		const message: ProducedMessage = {
			key: record.key,
			value: record.value,
			partition: record.partition,
			timestamp: record.timestamp === undefined ? undefined : new Date(record.timestamp),
			headers: record.headers,
		}
		const sent = eos ? this.transaction!.send(record.topic, message) : this.producer.send(record.topic, message)
		task.markCommitNeeded()
		// KS reports a failed send with the record context of the send, not of the ack.
		const { input } = task
		const { generation } = this
		generation.sent()
		sent.then(
			result => {
				// A corrupted or closed task is never checkpointed again: its next incarnation restores from the last commit.
				if (
					kind === 'changelog' &&
					generation === this.generation &&
					!task.corrupted &&
					task.state !== 'CLOSED'
				) {
					this.changelogAcked(task, result)
				}
				generation.answered()
			},
			(error: unknown) => {
				if (generation === this.generation) {
					try {
						this.sendFailed(task, input, kind, record, error)
					} catch (failure) {
						generation.failure ??= { error: failure }
					}
				}
				generation.answered()
			}
		)
	}

	private dropped(message: string, task: StreamTask, kind: SendKind, record: CollectedRecord): void {
		this.options.logger.warn(message, {
			task: task.id.key,
			state: task.state,
			lost: task.lost,
			topic: record.topic,
			kind,
		})
	}

	private changelogAcked(task: StreamTask, result: SendResult): void {
		let offsets = this.changelogOffsets.get(task)
		if (!offsets) {
			offsets = new Map()
			this.changelogOffsets.set(task, offsets)
		}
		const key = tpKey(result)
		const last = offsets.get(key)
		if (last === undefined || result.offset > last) {
			offsets.set(key, result.offset)
		}
	}

	/**
	 * Classifies a send that failed after the producer's retries (KS `RecordCollectorImpl.recordSendError`). Returns
	 * to drop the record; throws a run-wide failure, which the next flush reports.
	 */
	private sendFailed(task: StreamTask, input: Input, kind: SendKind, record: CollectedRecord, error: unknown): void {
		const { eos } = this.options
		if (isFatalSendError(error)) {
			throw new StreamsError(`sending to '${record.topic}' failed: no handler can make this send succeed`, {
				cause: error,
				taskId: task.id,
			})
		}
		if (isFencedError(error)) {
			throw new TaskMigratedError(`the producer was fenced while sending to '${record.topic}'`, { cause: error })
		}
		if (error instanceof TransactionAbortedError) {
			throw error
		}
		if (isRetriableError(error)) {
			if (eos) {
				throw new TaskMigratedError(`sending to '${record.topic}' failed under exactly-once`, { cause: error })
			}
			task.markCorrupted()
			return
		}
		if (eos) {
			// A failed send poisons the transaction: no handler can let it commit.
			throw new FailedProcessingError(`sending to '${record.topic}' failed under exactly-once`, {
				cause: error,
				taskId: task.id,
			})
		}
		applyHandler(
			() => {
				const ctx = handlerContext(task, input, record)
				const response = this.options.handlers.production(ctx, record, error, 'send')
				// A dropped changelog record would make the store diverge from its changelog.
				return kind === 'changelog' && response === 'continue' ? 'fail' : response
			},
			error,
			`sending to '${record.topic}' failed`,
			task.id
		)
		task.recordDropped('production', 'producer', {
			topic: record.topic,
			origin: 'send',
			error: errorMessage(error),
		})
	}
}

/** The input record of the send (null from init() and punctuators) and the record at the sink. */
function handlerContext(task: StreamTask, input: Input, record: CollectedRecord): ErrorHandlerContext {
	return {
		topic: input?.topic ?? null,
		partition: input?.partition ?? -1,
		offset: input?.offset ?? -1n,
		headers: record.headers ?? {},
		timestamp: record.timestamp ?? -1,
		processorNodeId: null,
		taskId: task.id,
		sourceRawKey: input?.key ?? null,
		sourceRawValue: input?.value ?? null,
	}
}
