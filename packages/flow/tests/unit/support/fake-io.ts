/**
 * Fake IO for runtime tests: the group consumer as the TaskManager uses it (pause, resume, seek recorded in order), a
 * restorer and a start-position settler the test drives, and the scheduler's exclusive section. Free of vitest.
 */
import type { TopicPartition } from '@kafkats/client'
import type { GroupConsumerPort } from '@/runtime/io.js'
import type { StartPositionSettler, TaskRestorer } from '@/runtime/task-manager.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StreamTask } from '@/runtime/task.js'
import { FakeConsumer } from './task-harness.js'

export interface Deferred<T> {
	readonly promise: Promise<T>
	resolve(value: T): void
	reject(error: unknown): void
}

export function deferred<T = void>(): Deferred<T> {
	let resolve!: (value: T) => void
	let reject!: (error: unknown) => void
	const promise = new Promise<T>((res, rej) => {
		resolve = res
		reject = rej
	})
	return { promise, resolve, reject }
}

/** The fake consumer of the task harness, plus pause, resume and seek ('pause a:0', 'resume a:0', 'seek a:0 5'). */
export class FakeGroupConsumer
	extends FakeConsumer
	implements Pick<GroupConsumerPort, 'pause' | 'resume' | 'seek' | 'groupMetadata' | 'nextOffset' | 'commit'>
{
	readonly calls: string[] = []
	readonly paused = new Set<string>()

	pause(tps: readonly TopicPartition[]): void {
		for (const tp of tps) {
			this.paused.add(tpKey(tp))
			this.calls.push(`pause ${tpKey(tp)}`)
		}
	}

	resume(tps: readonly TopicPartition[]): void {
		for (const tp of tps) {
			this.paused.delete(tpKey(tp))
			this.calls.push(`resume ${tpKey(tp)}`)
		}
	}

	/** The next delivery offset becomes `offset`. */
	seek(tp: TopicPartition, offset: bigint): void {
		this.positions.set(tpKey(tp), offset)
		this.calls.push(`seek ${tpKey(tp)} ${offset}`)
	}
}

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
