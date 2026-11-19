/**
 * Restore pipelines (4.7.1): per task that is not RUNNING, initialize the output (exactly-once), finish the clears of
 * stores a prune left partly cleared, restore its stores and settle its start positions (the I/O part, never inside
 * exclusive), then complete it in an exclusive op of its own. Owned by the TaskManager
 * (SI2): suspend, revive and close stop a task's pipeline and wait until it settled, so a store is never restored and
 * released at once (runtime§6.31), and no exclusive op ever waits for another one.
 */
import type { StreamTask } from '@/runtime/task.js'
import { cap } from '@/runtime/signals.js'

export interface TaskRestorer {
	/** Resolves when the task's stores are caught up; AbortError when `signal` aborts (after the in-flight store write). */
	restore(task: StreamTask, signal: AbortSignal): Promise<void>
}

export interface StartPositionSettler {
	/** 'latest' pins and the start-position commit for partitions without a commit; returns the settled positions. */
	settle(task: StreamTask, signal: AbortSignal): Promise<ReadonlyMap<string, bigint>>
}

export interface RestorePipelinesDeps {
	/** Exactly-once: the producer is initialized before any changelog is read (RecordCollector.initialize). */
	readonly initializeOutput: (signal: AbortSignal) => Promise<void>
	/** StateDirectory.finishClears of the task's leases: a prune its lease stopped left the store partly cleared. */
	readonly finishClears: (task: StreamTask, signal: AbortSignal) => Promise<void>
	readonly restorer: TaskRestorer
	readonly startPositions: StartPositionSettler
	readonly exclusive: <T>(fn: () => Promise<T>) => Promise<T>
	/** Aborts every pipeline (the run's hard stop and the runtime's shutdown). */
	readonly signal: AbortSignal
	/**
	 * The completion op, inside exclusive; `signal` is the pipeline's (aborted once it was stopped), `positions` the
	 * settled start positions.
	 */
	readonly complete: (task: StreamTask, signal: AbortSignal, positions: ReadonlyMap<string, bigint>) => Promise<void>
	/** A failure of a pipeline that was not stopped meanwhile. */
	readonly onFailure: (error: unknown) => void
}

/** Start positions are best effort and must not hold a task back for long (CR8). */
const SETTLE_TIMEOUT_MS = 5000

interface Pipeline {
	readonly abort: AbortController
	/** Restore, then settle: the settled start positions. */
	readonly restored: Promise<ReadonlyMap<string, bigint>>
}

export class RestorePipelines {
	private readonly pipelines = new Map<StreamTask, Pipeline>()

	constructor(private readonly deps: RestorePipelinesDeps) {}

	running(task: StreamTask): boolean {
		return this.pipelines.has(task)
	}

	start(task: StreamTask): void {
		const abort = new AbortController()
		const signal = AbortSignal.any([abort.signal, this.deps.signal])
		const pipeline: Pipeline = { abort, restored: this.restoreAndSettle(task, signal) }
		this.pipelines.set(task, pipeline)
		pipeline.restored
			.then(positions => this.deps.exclusive(() => this.deps.complete(task, signal, positions)))
			.catch((error: unknown) => {
				// A stopped pipeline's task is suspending or closing, or the run is stopping: nothing failed.
				if (!signal.aborted) this.deps.onFailure(error)
			})
			.finally(() => {
				if (this.pipelines.get(task) === pipeline) this.pipelines.delete(task)
			})
	}

	/** Aborts the task's pipeline and resolves once its restore and settle settled. A queued completion op then does nothing. */
	async stop(task: StreamTask): Promise<void> {
		const pipeline = this.pipelines.get(task)
		if (pipeline === undefined) {
			return
		}
		this.pipelines.delete(task)
		pipeline.abort.abort()
		await pipeline.restored.catch(() => {})
	}

	private async restoreAndSettle(task: StreamTask, signal: AbortSignal): Promise<ReadonlyMap<string, bigint>> {
		await this.deps.initializeOutput(signal)
		await this.deps.finishClears(task, signal)
		await this.deps.restorer.restore(task, signal)
		return this.deps.startPositions.settle(task, cap(signal, SETTLE_TIMEOUT_MS))
	}
}
