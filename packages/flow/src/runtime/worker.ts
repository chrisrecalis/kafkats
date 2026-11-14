import type { ConsumeContext, KafkaClient, Logger, Message, ProducerConfig, RunEachOptions } from '@kafkats/client'
import type { AnyProcessor, SourceNode, WorkerContext } from '@/processors/index.js'
import type { FlowConfig } from '@/types.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
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
import { DEFAULT_COMMIT_INTERVAL_MS, WorkerTransactions } from '@/runtime/transactions.js'

/** Default transaction timeout for exactly_once producers (same as Kafka Streams). */
const DEFAULT_TRANSACTION_TIMEOUT_MS = 10_000

/**
 * Default time to wait for a worker to join the group. Joining can legitimately take as long as
 * the session timeout of a member that crashed (the group waits for it before rebalancing).
 */
const DEFAULT_START_TIMEOUT_MS = 120_000

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

/** The stream workers of the app (`numStreamThreads`), each one consumer + one producer. */
export class WorkerPool {
	private active: StreamWorker[] = []
	private runPromises: Promise<void>[] = []

	constructor(private readonly deps: WorkerDeps) {}

	/**
	 * Start the workers on the given topics. Resolves once every worker has joined the group and the
	 * state of the partitions assigned so far has been restored.
	 */
	async start(topics: string[]): Promise<void> {
		const threadCount = Math.max(1, this.deps.config.numStreamThreads ?? 1)
		this.active = []
		this.runPromises = []

		const workerReady: Promise<void>[] = []
		for (let id = 0; id < threadCount; id += 1) {
			const worker = new StreamWorker(id, threadCount, this.deps)
			this.active.push(worker)
			workerReady.push(worker.whenRunning())
			this.runPromises.push(worker.run(topics))
		}

		await Promise.all(workerReady)

		// Partitions assigned so far are paused until their state is restored (see
		// StreamWorker.onPartitionsAssigned). Wait for those restorations before reporting the app as started.
		await this.deps.restoration.drain()
	}

	/** Whether any worker owns state partition `partition` of a store fed by `sourceTopics`. */
	ownsStatePartition(sourceTopics: Set<string>, partition: number): boolean {
		return this.active.some(worker => worker.ownsStatePartition(sourceTopics, partition))
	}

	/**
	 * Commit every worker's open transaction (on close). A failure here loses nothing: the
	 * transaction aborts and the records are reprocessed after restart.
	 */
	async commitAll(): Promise<void> {
		for (const worker of this.active) {
			await worker.transactions
				.enqueue(() => worker.transactions.commit())
				.catch(err => {
					const error = toError(err)
					this.deps.status.recordError(error)
					this.deps.logger.warn('final transaction commit failed during close', {
						worker: worker.context.id,
						error: error.message,
					})
				})
		}
	}

	/** Stop every worker's consumer; their run loops then wind down. */
	stop(): void {
		for (const worker of this.active) worker.stop()
	}

	/** Settles once every worker's run loop has returned. */
	async settled(): Promise<void> {
		await Promise.allSettled(this.runPromises)
	}

	/** Disconnect the producers and forget the workers. */
	async disconnect(): Promise<void> {
		for (const worker of this.active) {
			await worker.context.producer.disconnect().catch(() => {})
		}
		this.active = []
		this.runPromises = []
	}
}

/**
 * One stream worker: a consumer feeding the topology and a producer for its output.
 *
 * A worker that fails never continues past the failure (fail-stop): it stops, drops everything it
 * has not committed, and either restarts from the committed offsets or puts the app in ERROR.
 */
class StreamWorker {
	readonly context: WorkerContext
	readonly transactions: WorkerTransactions
	private readonly eosEnabled: boolean

	constructor(
		id: number,
		private readonly threadCount: number,
		private readonly deps: WorkerDeps
	) {
		const { client, config } = deps
		this.eosEnabled = deps.eosEnabled
		const producer = client.producer(this.buildProducerConfig(id))

		const userHook = config.consumer?.onBeforeRebalance
		const consumer = client.consumer({
			groupId: config.applicationId,
			...config.consumer,
			autoOffsetReset: deps.topology.resolveOffsetReset(),
			groupInstanceId: this.buildGroupInstanceId(id),
			isolationLevel: this.eosEnabled ? 'read_committed' : config.consumer?.isolationLevel,
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
				this.eosEnabled || userHook
					? async () => {
							if (userHook) await userHook()
							if (!this.eosEnabled) return
							if (this.transactions.active) {
								await this.transactions.enqueue(() => this.transactions.commit())
							}
						}
					: undefined,
		})

		this.context = {
			id,
			producer,
			consumer,
			activeTransaction: null,
			activeTransactionPromise: null,
			offsetCommitContext: null,
			sourcesByTopic: new Map(),
			assignedPartitions: new Map(),
			pendingOffsets: new Map(),
			pendingChangelogOffsets: new Map(),
			dirtyStores: new Set(),
			streamClocks: new Map(),
			failure: null,
			eosQueue: Promise.resolve(),
			transactionActive: false,
			commitTimer: null,
			lastCommitTime: 0,
		}
		this.transactions = new WorkerTransactions(
			this.context,
			config.commitIntervalMs ?? DEFAULT_COMMIT_INTERVAL_MS,
			deps.checkpoints,
			(err, recoverable) => this.fail(err, recoverable)
		)
		this.context.sourcesByTopic = this.buildSources()
		this.attachConsumerEvents()
	}

	/** Resolves once the consumer has joined the group and runs; rejects if it fails or stops first. */
	whenRunning(): Promise<void> {
		const consumer = this.context.consumer
		return new Promise<void>((resolve, reject) => {
			const startTimeoutMs = this.deps.config.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS
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
				await worker.consumer.runEach(
					topics,
					(message, ctx) => this.handleMessage(message, ctx),
					this.buildRunEachOptions()
				)
			} catch (err) {
				runError = err
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

			status.recordError(failure.error)
			if (!status.started) {
				// Failures during startup are reported by start() itself.
				status.failStartup(failure.error)
				return
			}
			if (!failure.recoverable || config.restartOnError === false) {
				logger.error('stream worker failed', { worker: worker.id, error: failure.error.message })
				status.setState('ERROR')
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
			status.setState('REBALANCING')
			await status.sleepUnlessClosing(backoffMs)
			if (status.closing) return

			if (this.eosEnabled) {
				// A producer whose transaction failed may be in an undefined state; start clean.
				await worker.producer.disconnect().catch(() => {})
				worker.producer = client.producer(this.buildProducerConfig(worker.id))
			}
		}
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
		this.deps.status.recordError(worker.failure.error)
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

	private async handleMessage(message: Message<Buffer>, ctx: ConsumeContext): Promise<void> {
		const worker = this.context
		// Fail-stop: once a worker has failed, nothing more may be processed (and thereby
		// acknowledged) until it has restarted from the last committed offsets.
		if (worker.failure) throw worker.failure.error

		// close() first seals message delivery, then commits everything already queued.
		// Records that were fetched but not started remain uncommitted for the next owner.
		if (this.deps.status.closing && this.eosEnabled) return

		const sources = worker.sourcesByTopic.get(message.topic)
		if (!sources) return

		const recordContext: RecordContext = { worker, topic: message.topic, partition: message.partition }
		const handle = async () => {
			for (const source of sources) {
				await source.handleMessage(message, ctx)
			}
		}

		if (!this.eosEnabled) {
			try {
				await recordContextStorage.run(recordContext, handle)
				worker.processedSinceRestart = true
			} catch (err) {
				this.fail(err, isRecoverable(err))
				throw err
			}
			return
		}

		await this.transactions.enqueue(() =>
			recordContextStorage.run(recordContext, () => this.transactions.process(message, ctx, handle))
		)
	}

	private attachConsumerEvents(): void {
		const { consumer } = this.context
		const { status, eosEnabled } = this.deps
		const worker = this.context
		// Note: 'rebalance' is a synchronous notification; the actual EOS commit is wired through
		// onBeforeRebalance (passed to the consumer config) which is awaited by the rebalance protocol.
		consumer.on('rebalance', () => {
			if (status.state === 'RUNNING') {
				status.setState('REBALANCING')
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
			if (eosEnabled && worker.transactionActive) {
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
			status.recordError(error)
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

			void restoration
				.enqueue(partitions)
				.then(() => startPositions.settle(worker, partitions, positions))
				.then(() => {
					const stillAssigned = partitions.filter(tp =>
						worker.assignedPartitions.has(`${tp.topic}:${tp.partition}`)
					)
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
					// Never process on unrestored state: stop, then retry from scratch.
					this.fail(error, true)
				})
		}

		if (status.started && status.state === 'REBALANCING') {
			status.setState('RUNNING')
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

		const add = (topic: string, source: SourceNode<unknown, unknown>) => {
			const cloned = source.cloneGraph(this.context, cloneMap)
			const list = clonedByTopic.get(topic) ?? []
			list.push(cloned as SourceNode<unknown, unknown>)
			clonedByTopic.set(topic, list)
		}

		for (const [topic, sources] of topology.sourcesByTopic) {
			for (const source of sources) add(topic, source)
		}
		if (!status.clusterAvailable) {
			// Without a cluster (mocked clients) global tables are fed like regular sources.
			for (const table of topology.globalTables) add(table.topic, table.source)
		}

		return clonedByTopic
	}

	private buildRunEachOptions(): RunEachOptions | undefined {
		const config = this.deps.config
		if (!this.eosEnabled) {
			return config.runEach
		}

		const base = config.runEach ?? {}
		return {
			...base,
			autoCommit: false,
			commitOffsets: false,
			partitionConcurrency: 1,
		}
	}

	private buildProducerConfig(workerId: number): ProducerConfig | undefined {
		const config = this.deps.config
		if (!this.eosEnabled) {
			return config.producer
		}

		const base = config.producer ?? {}
		const transactionalId = base.transactionalId
			? this.threadCount > 1
				? `${base.transactionalId}-w${workerId}`
				: base.transactionalId
			: `${config.applicationId}-${this.deps.processId}-w${workerId}`
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

	private buildGroupInstanceId(workerId: number): string | undefined {
		const base = this.deps.config.consumer?.groupInstanceId
		if (!base) {
			return undefined
		}
		return this.threadCount > 1 ? `${base}-w${workerId}` : base
	}
}
