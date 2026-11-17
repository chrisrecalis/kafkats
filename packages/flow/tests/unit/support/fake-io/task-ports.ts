/**
 * Fakes of what the TaskManager is given besides IO: a restorer and a start-position settler the test drives, and
 * the scheduler's exclusive section.
 */
import type { StartPositionSettler, TaskRestorer } from '@/runtime/task-manager.js'
import type { StreamTask } from '@/runtime/task.js'
import { deferred } from './steps.js'

/** One restore the test settles. Its abort rejects once the store write in flight (`write`), if any, is done. */
export class RestoreJob {
	/** A store write in flight: an abort waits for it. */
	write: Promise<void> | undefined
	private readonly result = deferred<{ storeTimes: ReadonlyMap<string, number> }>()

	constructor(
		readonly task: StreamTask,
		readonly signal: AbortSignal
	) {
		const abort = () => {
			void (this.write ?? Promise.resolve()).then(() =>
				this.result.reject(new DOMException('The restore was aborted', 'AbortError'))
			)
		}
		if (signal.aborted) abort()
		else signal.addEventListener('abort', abort, { once: true })
	}

	get promise(): Promise<{ storeTimes: ReadonlyMap<string, number> }> {
		return this.result.promise
	}

	finish(storeTimes: ReadonlyMap<string, number> = new Map()): void {
		this.result.resolve({ storeTimes })
	}

	fail(error: unknown): void {
		this.result.reject(error)
	}
}

export class FakeRestorer implements TaskRestorer {
	/** false: a restore resolves at once; true: it waits until the test settles its job. */
	manual = false
	/** Every task a restore was asked for, in order. */
	readonly restored: StreamTask[] = []
	readonly jobs: RestoreJob[] = []

	restore(task: StreamTask, signal: AbortSignal): Promise<{ storeTimes: ReadonlyMap<string, number> }> {
		this.restored.push(task)
		if (!this.manual) {
			return Promise.resolve({ storeTimes: new Map() })
		}
		const job = new RestoreJob(task, signal)
		this.jobs.push(job)
		return job.promise
	}

	/** The last restore job of `task`. */
	job(task: StreamTask): RestoreJob {
		const job = this.jobs.filter(j => j.task === task).at(-1)
		if (job === undefined) {
			throw new Error(`no restore job for task ${task.id.key}`)
		}
		return job
	}
}

/**
 * Like StartPositions.settle (4.7.4) without pins: "commits" the resolved position of every input partition without
 * a commit (recorded as `<tp>@<offset>`) and returns those positions.
 */
export class FakeStartPositions implements StartPositionSettler {
	readonly commits: string[] = []

	settle(task: StreamTask): Promise<ReadonlyMap<string, bigint>> {
		const positions = new Map<string, bigint>()
		for (const [key, start] of task.starts) {
			if (start.committed === null) {
				positions.set(key, start.resolved)
				this.commits.push(`${key}@${start.resolved}`)
			}
		}
		return Promise.resolve(positions)
	}
}

/** Scheduler.exclusive's contract: one op at a time, in FIFO order; a failed op fails only its caller. */
export class SerialExclusive {
	private tail: Promise<unknown> = Promise.resolve()

	readonly run = <T>(fn: () => Promise<T>): Promise<T> => {
		const result = this.tail.then(fn)
		this.tail = result.catch(() => {})
		return result
	}

	/** Resolves once every op queued so far finished. */
	async idle(): Promise<void> {
		await this.tail
	}
}
