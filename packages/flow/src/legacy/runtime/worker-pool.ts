import { toError } from '@/legacy/runtime/errors.js'
import { StreamWorker, type WorkerDeps } from '@/legacy/runtime/stream-worker.js'

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
			const worker = new StreamWorker({ id, threadCount }, this.deps)
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
			await worker.commit().catch(err => {
				const error = toError(err)
				this.deps.status.lastError = error
				this.deps.logger.warn('final transaction commit failed during close', {
					worker: worker.id,
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
			await worker.disconnect()
		}
		this.active = []
		this.runPromises = []
	}
}
