/**
 * The output of one producer incarnation (KS `RecordCollector`): sends of every task, deferred acknowledgements,
 * per-task failure attribution and changelog positions, and the open transaction under exactly-once.
 */
import type { ConsumerGroupMetadata, Logger, SendResult } from '@kafkats/client'
import { TransactionAbortedError } from '@kafkats/client'
import { FailedProcessingError, IllegalStateError, StreamsError, TaskMigratedError } from '@/api/errors.js'
import type { ErrorHandlerContext } from '@/api/handlers.js'
import { applyHandler } from '@/processor/node.js'
import type { CollectedRecord, SendKind, TaskOutput } from '@/processor/output.js'
import type { PartitionCountCache, ResolvedHandlers } from '@/runtime/config.js'
import { errorMessage, isFatalSendError, isFencedError, isRetriableError } from '@/runtime/errors.js'
import type { OffsetAndMetadata, ProducedMessage, ProducerPort, TransactionPort } from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StreamTask } from '@/runtime/task.js'

/** The input record a task was processing when it sent. */
type Input = StreamTask['input']

export interface RecordCollectorOptions {
	readonly eos: boolean
	readonly handlers: ResolvedHandlers
	readonly counts: PartitionCountCache
	readonly logger: Logger
}

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
	/** Exactly-once: the last transaction was aborted and none was opened since, so the work done meanwhile is void. */
	private aborted = false
	/** Last acked offset per changelog partition, per task, since the last take. */
	private readonly changelogOffsets = new Map<StreamTask, Map<string, bigint>>()
	/** Tasks whose sends failed with a retriable error under at-least-once since the last flush. */
	private corrupted = new Set<StreamTask>()

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
		}
	}

	/** Exactly-once: begins a transaction unless one is open; awaited before any slice that may send. */
	async ensureTransaction(signal: AbortSignal): Promise<void> {
		if (!this.options.eos || this.transaction !== undefined || this.isClosed) {
			return
		}
		const { generation } = this
		this.opening ??= this.producer.beginTransaction({ signal }).finally(() => {
			this.opening = undefined
		})
		const transaction = await this.opening
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
	 * Awaits every send made so far, including those made while flushing. Throws the first run-wide failure; returns
	 * the tasks whose sends failed with a task-scoped retriable error under at-least-once.
	 */
	async flush(signal: AbortSignal): Promise<{ corrupted: ReadonlySet<StreamTask> }> {
		const { generation } = this
		await raceSignal(generation.settled(), signal)
		if (generation.failure) {
			throw generation.failure.error
		}
		const { corrupted } = this
		this.corrupted = new Set()
		return { corrupted }
	}

	/** Last acked changelog offset per changelog partition written by `task` since the last take. */
	takeChangelogOffsets(task: StreamTask): Map<string, bigint> {
		const offsets = this.changelogOffsets.get(task) ?? new Map<string, bigint>()
		this.changelogOffsets.delete(task)
		return offsets
	}

	/** Exactly-once: sends the offsets in the transaction (opening one if none is) and commits it. */
	async commitTransaction(
		offsets: readonly OffsetAndMetadata[],
		group: ConsumerGroupMetadata,
		signal: AbortSignal
	): Promise<void> {
		if (this.transaction === undefined && offsets.length === 0) {
			return
		}
		await this.ensureTransaction(signal)
		const { transaction } = this
		if (transaction === undefined) {
			throw new IllegalStateError('the record collector is closed')
		}
		if (offsets.length > 0) {
			await raceSignal(transaction.sendOffsets({ consumerGroupMetadata: group, offsets }), signal)
		}
		await transaction.commit({ signal })
		this.transaction = undefined
	}

	/**
	 * Exactly-once: gives up the sends made so far, waits until each was answered (a request answered after the abort
	 * marker would land outside any transaction), then aborts. Never throws.
	 */
	async abortTransaction(signal: AbortSignal): Promise<void> {
		this.aborted = true
		const answered = this.giveUp()
		const { opening } = this
		let transaction = this.transaction
		this.transaction = undefined
		try {
			if (transaction === undefined && opening !== undefined) {
				transaction = await raceSignal(opening, signal)
			}
			await raceSignal(answered, signal)
			await transaction?.abort({ signal })
		} catch (error) {
			this.options.logger.warn('aborting the transaction failed', { error: errorMessage(error) })
		}
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

	/** Ends the current generation; resolves once its sends were answered. */
	private giveUp(): Promise<void> {
		const ended = this.generation
		this.generation = new Generation()
		this.changelogOffsets.clear()
		this.corrupted = new Set()
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
				if (kind === 'changelog' && generation === this.generation) {
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
			this.corrupted.add(task)
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
		this.options.logger.warn('dropped a record whose send failed', {
			task: task.id.key,
			topic: record.topic,
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
