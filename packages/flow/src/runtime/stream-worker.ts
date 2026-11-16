import type { BatchContext, ConsumeContext, KafkaClient, Logger, Message } from '@kafkats/client'
import type { AnyProcessor, SourceNode, WorkerContext } from '@/processors/index.js'
import type { FlowConfig } from '@/types.js'
import { PendingCheckpoints, type Checkpoints } from '@/runtime/checkpoints.js'
import { PendingSends } from '@/runtime/pending-sends.js'
import { recordContextStorage, type RecordContext } from '@/runtime/context.js'
import {
	RESTART_INITIAL_BACKOFF_MS,
	RESTART_MAX_BACKOFF_MS,
	isInfrastructureRecoverable,
	isRecoverable,
	toError,
} from '@/runtime/errors.js'
import type { Restoration } from '@/runtime/restoration.js'
import type { StartPositions } from '@/runtime/start-positions.js'
import type { AppStatus } from '@/runtime/status.js'
import type { StoreRegistry } from '@/runtime/store-registry.js'
import type { Topology } from '@/runtime/topology.js'
import { KeyedTurns } from '@/runtime/keyed-turns.js'
import { DEFAULT_COMMIT_INTERVAL_MS, WorkerTransactions } from '@/runtime/transactions.js'
import {
	type WorkerSlot,
	workerConsumerConfig,
	workerProducerConfig,
	workerRunOptions,
	workerStartTimeoutMs,
} from '@/runtime/worker-config.js'

export type WorkerDeps = {
	client: KafkaClient
	config: FlowConfig
	eosEnabled: boolean
	/** Internal instance identity used to keep derived transactional IDs unique across replicas. */
	processId: string
	topology: Topology
	stores: StoreRegistry
	restoration: Restoration
	startPositions: StartPositions
	checkpoints: Checkpoints
	status: AppStatus
	logger: Logger
}

/**
 * One stream worker: a consumer feeding the topology and a producer for its output.
 *
 * A worker that fails never continues past the failure (fail-stop): it stops, drops everything it
 * has not committed, and either restarts from the committed offsets or puts the app in ERROR.
 */
export class StreamWorker {
	readonly context: WorkerContext
	private readonly transactions: WorkerTransactions
	private readonly commitIntervalMs: number
	/** Under at-least-once: one batch at a time per partition number (see handleBatch). */
	private readonly statePartitionTurns = new KeyedTurns<number>()
	private readonly eosEnabled: boolean

	constructor(
		private readonly slot: WorkerSlot,
		private readonly deps: WorkerDeps
	) {
		const { client, config } = deps
		this.eosEnabled = deps.eosEnabled
		const producer = client.producer(this.producerConfig())
		const consumer = client.consumer(
			// Always goes through the transaction queue, even when no transaction is open yet: a
			// record whose processing was dispatched but has not opened its transaction is queued
			// ahead of this commit, so its transaction is committed too instead of staying open
			// across the revoke.
			workerConsumerConfig(config, this.eosEnabled, deps.topology.resolveOffsetReset(), slot, () => this.commit())
		)

		this.context = {
			id: slot.id,
			producer,
			consumer,
			activeTransaction: null,
			sourcesByTopic: new Map(),
			assignedPartitions: new Map(),
			pendingCheckpoints: new PendingCheckpoints(),
			sends: new PendingSends(),
			dirtyStores: new Set(),
			streamClocks: new Map(),
			failure: null,
			processedSinceRestart: false,
		}
		this.commitIntervalMs = config.commitIntervalMs ?? DEFAULT_COMMIT_INTERVAL_MS
		this.transactions = new WorkerTransactions(
			this.context,
			this.commitIntervalMs,
			deps.checkpoints,
			(err, recoverable) => this.fail(err, recoverable)
		)
		this.context.sourcesByTopic = this.buildSources()
		this.attachConsumerEvents()
	}

	get id(): number {
		return this.context.id
	}

	/** Resolves once the consumer has joined the group and runs; rejects if it fails or stops first. */
	whenRunning(): Promise<void> {
		const consumer = this.context.consumer
		return new Promise<void>((resolve, reject) => {
			const startTimeoutMs = workerStartTimeoutMs(this.deps.config)
			const timer = setTimeout(() => {
				cleanup()
				reject(new Error(`Consumer did not start within ${startTimeoutMs}ms`))
			}, startTimeoutMs)

			const onRunning = () => {
				cleanup()
				resolve()
			}
			const onError = (error: unknown) => {
				cleanup()
				reject(toError(error))
			}
			const onStopped = () => {
				cleanup()
				reject(new Error('Consumer stopped before starting'))
			}

			const cleanup = () => {
				clearTimeout(timer)
				consumer.off('running', onRunning)
				consumer.off('error', onError)
				consumer.off('stopped', onStopped)
			}

			consumer.once('running', onRunning)
			consumer.once('error', onError)
			consumer.once('stopped', onStopped)
		})
	}

	/**
	 * Run the consume loop until the app closes or the worker fails permanently.
	 *
	 * A worker that fails (commit failure, lost transaction, broker outage, ...) never continues
	 * past the failure: its transaction is aborted, buffered state is dropped and the consumer is
	 * stopped, so nothing after the last committed offsets is acknowledged. For recoverable errors
	 * the worker then rejoins the group and resumes from the last committed offsets.
	 */
	async run(topics: string[]): Promise<void> {
		const { status, logger, config, client } = this.deps
		const worker = this.context
		let attempt = 0
		for (;;) {
			let runError: unknown = null
			try {
				await worker.consumer.runBatch(
					topics,
					(batch, ctx) => this.handleBatch(batch, ctx),
					workerRunOptions(config, this.eosEnabled)
				)
			} catch (err) {
				if (!(err instanceof BatchInterrupted)) runError = err
			}

			const failure =
				worker.failure ??
				(runError ? { error: toError(runError), recoverable: isInfrastructureRecoverable(runError) } : null)
			await this.reset()

			if (status.closing) return
			if (!failure) {
				// The consumer stopped without an error and without close(): nothing left to run.
				return
			}

			status.lastError = failure.error
			if (!status.started) {
				// Failures during startup are reported by start() itself.
				status.failStartup(failure.error)
				return
			}
			if (!failure.recoverable || config.restartOnError === false) {
				logger.error('stream worker failed', { worker: worker.id, error: failure.error.message })
				status.state = 'ERROR'
				return
			}

			attempt = worker.processedSinceRestart ? 1 : attempt + 1
			worker.processedSinceRestart = false
			const backoffMs = Math.min(
				RESTART_MAX_BACKOFF_MS,
				RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(attempt - 1, 10)
			)
			logger.warn('stream worker failed; restarting', {
				worker: worker.id,
				attempt,
				backoffMs,
				error: failure.error.message,
			})
			status.state = 'REBALANCING'
			await status.sleepUnlessClosing(backoffMs)
			if (status.closing) return

			if (this.eosEnabled) {
				// A producer whose transaction failed may be in an undefined state; start clean.
				await worker.producer.disconnect().catch(() => {})
				worker.producer = client.producer(this.producerConfig())
			}
		}
	}

	/** Commit the open transaction, after any transactional work queued before. */
	commit(): Promise<void> {
		return this.transactions.enqueue(() => this.transactions.commit())
	}

	/** Stop the consumer (on shutdown). */
	stop(): void {
		this.transactions.cancelCommitTimer()
		try {
			this.context.consumer.stop()
		} catch {
			// not running
		}
	}

	/** Disconnect the producer (after the run loop has returned). Never rejects. */
	async disconnect(): Promise<void> {
		await this.context.producer.disconnect().catch(() => {})
	}

	ownsStatePartition(sourceTopics: Set<string>, partition: number): boolean {
		for (const tp of this.context.assignedPartitions.values()) {
			if (tp.partition === partition && sourceTopics.has(tp.topic)) return true
		}
		return false
	}

	/** Mark the worker as failed and stop its consumer. Idempotent; the first failure wins. */
	private fail(err: unknown, recoverable: boolean): void {
		const worker = this.context
		if (!worker.failure) {
			worker.failure = { error: toError(err), recoverable }
		}
		this.deps.status.lastError = worker.failure.error
		try {
			worker.consumer.stop()
		} catch {
			// already stopping
		}
	}

	/** Bring a stopped worker back to a clean slate (no transaction, no buffered state, no partitions). */
	private async reset(): Promise<void> {
		const worker = this.context
		// Let any queued transactional work finish before tearing the transaction down.
		await this.transactions.idle()
		await this.transactions.abort(worker.failure?.error ?? new Error('worker stopped'))
		const released = [...worker.assignedPartitions.values()]
		worker.assignedPartitions.clear()
		worker.streamClocks.clear()
		this.deps.restoration.release(released)
		worker.failure = null
	}

	/**
	 * Process one consumed batch (records of one partition, in order).
	 *
	 * At-least-once: the batch's offsets are committed by the consumer once this returns, so it
	 * returns only after everything the batch produced has been acknowledged.
	 * Exactly-once: every record joins the worker's transaction batch, which commits offsets itself.
	 */
	private async handleBatch(batch: Message<Buffer>[], ctx: BatchContext): Promise<void> {
		this.throwIfFailed()

		if (this.eosEnabled) {
			for (const message of batch) {
				// close() first seals message delivery, then commits everything already queued.
				// Records that were fetched but not started remain uncommitted for the next owner.
				if (this.deps.status.closing) return
				const process = this.processorFor(message, ctx)
				if (!process) continue
				await this.transactions.enqueue(() => this.transactions.process(message, ctx, process))
			}
			return
		}

		// State is kept per partition number, and topics that feed the same stateful operation share
		// it: with `consumerRun.partitionConcurrency` above 1, batches of a:0 and b:0 would read and
		// write the same state at the same time and lose each other's updates.
		return this.statePartitionTurns.run(batch[0]!.partition, () => this.processBatch(batch, ctx))
	}

	/** Process a batch under at-least-once, acknowledging its progress as it goes. */
	private async processBatch(batch: Message<Buffer>[], ctx: BatchContext): Promise<void> {
		const worker = this.context
		// The batch may have waited for its turn while the worker failed.
		this.throwIfFailed()

		// A batch can take long to work through (many records, a slow processor), so its progress is
		// acknowledged every commit interval: a failure then repeats at most that much.
		const acknowledge = async (offset: bigint): Promise<void> => {
			await worker.sends.flush()
			// Another partition's batch of this worker may have failed in the meantime.
			this.throwIfFailed()
			ctx.resolveOffset(offset)
			worker.processedSinceRestart = true
		}
		try {
			let acknowledgedAt = Date.now()
			let processed: bigint | undefined
			for (const message of batch) {
				this.throwIfFailed()
				await this.processorFor(message, ctx)?.()
				processed = message.offset
				if (Date.now() - acknowledgedAt >= this.commitIntervalMs) {
					await acknowledge(processed)
					// Processing may never wait for I/O (nothing produced, in-memory state). Let
					// timers and sockets run, or a long batch would starve heartbeats and close().
					await new Promise<void>(resolve => setImmediate(resolve))
					// Stopping: everything so far is acknowledged, the rest of the batch is left
					// to the next owner instead of holding up the shutdown.
					if (ctx.signal.aborted || this.deps.status.closing) throw new BatchInterrupted()
					acknowledgedAt = Date.now()
				}
			}
			if (processed !== undefined) await acknowledge(processed)
		} catch (err) {
			if (err instanceof BatchInterrupted) throw err
			void worker.sends.discard()
			this.fail(err, isRecoverable(err))
			throw err
		}
	}

	/**
	 * Fail-stop: once a worker has failed, nothing more may be processed (and thereby
	 * acknowledged) until it has restarted from the last committed offsets.
	 */
	private throwIfFailed(): void {
		const failure = this.context.failure
		if (failure) throw failure.error
	}

	/** Runs a record through its source nodes, in the record's context. Undefined if nothing consumes it. */
	private processorFor(message: Message<Buffer>, ctx: ConsumeContext): (() => Promise<void>) | undefined {
		const worker = this.context
		const sources = worker.sourcesByTopic.get(message.topic)
		if (!sources) return undefined
		const recordContext: RecordContext = { worker, topic: message.topic, partition: message.partition }
		return () =>
			recordContextStorage.run(recordContext, async () => {
				for (const source of sources) {
					await source.handleMessage(message, ctx)
				}
			})
	}

	private attachConsumerEvents(): void {
		const { consumer } = this.context
		const { status, eosEnabled } = this.deps
		const worker = this.context
		// Note: 'rebalance' is a synchronous notification; the actual EOS commit is wired through
		// onBeforeRebalance (passed to the consumer config) which is awaited by the rebalance protocol.
		consumer.on('rebalance', () => {
			if (status.state === 'RUNNING') {
				status.state = 'REBALANCING'
			}
		})
		consumer.on('partitionsAssigned', partitions => this.onPartitionsAssigned(partitions))
		consumer.on('partitionsRevoked', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
			this.deps.restoration.release(partitions)
		})
		consumer.on('partitionsLost', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
			if (eosEnabled && this.transactions.active) {
				// The open transaction contains work for partitions we no longer own; it can
				// never commit. Abort it, and restart the worker if it still holds partitions
				// whose uncommitted records must be re-read.
				const error = new Error('Partitions were lost while a transaction was open')
				if (worker.assignedPartitions.size > 0) {
					this.fail(error, true)
				} else {
					void this.transactions.enqueue(() => this.transactions.abort(error))
				}
			}
			this.deps.restoration.release(partitions)
		})
		consumer.on('error', error => {
			// Informational: the consumer reports transient errors here too. Fatal failures
			// surface through the worker's run loop, which decides between restart and ERROR.
			status.lastError = error
		})
	}

	private onPartitionsAssigned(partitions: Array<{ topic: string; partition: number }>): void {
		const { status, restoration, startPositions } = this.deps
		const worker = this.context
		const consumer = worker.consumer
		for (const tp of partitions) {
			worker.assignedPartitions.set(`${tp.topic}:${tp.partition}`, tp)
			// A (re)assigned partition restarts from its committed offset; so does its clock.
			const suffix = `|${tp.topic}:${tp.partition}`
			for (const key of worker.streamClocks.keys()) {
				if (key.endsWith(suffix)) worker.streamClocks.delete(key)
			}
		}

		if (this.needsAssignmentPreparation()) {
			// No record of these partitions may be processed until their state is restored
			// and internal-topic positions are settled.
			try {
				consumer.pause(partitions)
			} catch {
				// Ignore - pause is best effort and may fail if the consumer is stopping
			}

			// Where the consumer starts each partition (committed offset or reset policy), read
			// before anything can move it.
			const positions = new Map<string, bigint>()
			for (const tp of partitions) {
				const position = consumer.position(tp.topic, tp.partition)
				if (position !== undefined) positions.set(`${tp.topic}:${tp.partition}`, position)
			}

			// Whether a partition is still held under *this* assignment. A partition that was revoked
			// and assigned again in the meantime has its own preparation queued behind this one and
			// must stay paused until that one finishes: resuming it here would let records be
			// processed while the later restore still rewrites (and resets the buffers of) its state.
			const current = new Map(partitions.map(tp => [`${tp.topic}:${tp.partition}`, tp]))
			const heldSinceThisAssignment = (topic: string, partition: number) => {
				const key = `${topic}:${partition}` as const
				return worker.assignedPartitions.get(key) === current.get(key)
			}

			void restoration
				.enqueue(partitions)
				.then(() =>
					startPositions.settle(partitions, positions, {
						seek: (topic, partition, offset) => {
							if (heldSinceThisAssignment(topic, partition)) consumer.seek(topic, partition, offset)
						},
						commitStartPosition: held =>
							consumer.commitStartPosition(
								held.filter(tp => heldSinceThisAssignment(tp.topic, tp.partition))
							),
					})
				)
				.then(() => {
					const stillAssigned = partitions.filter(tp => heldSinceThisAssignment(tp.topic, tp.partition))
					if (stillAssigned.length === 0 || worker.failure || status.closing) return
					try {
						consumer.resume(stillAssigned)
					} catch {
						// Ignore - consumer may have been stopped while restoring
					}
				})
				.catch(err => {
					if (status.closing) return
					const error = toError(err)
					// start() reports the failure.
					status.failStartup(error)
					// Never process on unrestored state: stop, then retry from scratch unless the
					// error is one no restart can fix (e.g. not authorized to read the changelog).
					this.fail(error, isInfrastructureRecoverable(error))
				})
		}

		if (status.started && status.state === 'REBALANCING') {
			status.state = 'RUNNING'
		}
	}

	/** Whether assigned partitions need asynchronous preparation before records may flow. */
	private needsAssignmentPreparation(): boolean {
		const { status, topology, stores } = this.deps
		if (!status.clusterAvailable) return false
		if (topology.hasInternalTopics()) return true
		if ((topology.resolveOffsetReset() ?? 'latest') === 'latest') return true
		return stores.hasPartitionScopedStores()
	}

	/** Clone the topology for this worker: processors hold per-worker state. */
	private buildSources(): Map<string, SourceNode<unknown, unknown>[]> {
		const { topology, status } = this.deps
		const clonedByTopic = new Map<string, SourceNode<unknown, unknown>[]>()
		const cloneMap = new Map<AnyProcessor, AnyProcessor>()
		for (const [topic, sources] of topology.groupSources(status.clusterAvailable)) {
			clonedByTopic.set(
				topic,
				sources.map(source => source.cloneGraph(this.context, cloneMap) as SourceNode<unknown, unknown>)
			)
		}
		return clonedByTopic
	}

	private producerConfig() {
		const { config, eosEnabled, processId } = this.deps
		return workerProducerConfig(config, eosEnabled, processId, this.slot)
	}
}

/** Thrown out of a batch handler that stopped early because the worker is stopping. */
class BatchInterrupted extends Error {
	constructor() {
		super('batch interrupted: the worker is stopping')
	}
}
