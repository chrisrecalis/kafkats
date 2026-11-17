/**
 * Restoration of tasks (4.7.1, D8): one restore job per task that is not RUNNING, with its own restore consumer over
 * every changelog partition of the task (restoreChangelogs). At most `maxConcurrentTasks` jobs run at once; the others
 * wait in FIFO order.
 */
import { OffsetOutOfRangeError, type Logger } from '@kafkats/client'
import { TaskCorruptedError } from '@/api/errors.js'
import { restoreChangelogs, restoreTarget, type RestoreTarget } from '@/changelog/restorer.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import type { ResolvedRestorationOptions } from '@/runtime/config.js'
import type { RuntimeIO } from '@/runtime/io.js'
import type { TaskRestorer } from '@/runtime/restore-pipelines.js'
import { abortError } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StreamTask } from '@/runtime/task.js'
import type { KeyValueStore, WindowedKey } from '@/state.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { RegisteredStore } from '@/state/state-manager.js'

export interface RestorationPoolDeps {
	readonly io: Pick<RuntimeIO, 'restoreConsumers' | 'admin'>
	readonly checkpoints: Checkpoints
	readonly options: ResolvedRestorationOptions
	readonly applicationId: string
	/** Restore progress is checkpointed at most this often (the app's commit interval). */
	readonly commitIntervalMs: number
	readonly logger: Logger
}

/** What a restore job needs of a task. */
export type RestoringTask = Pick<StreamTask, 'id' | 'stores'>

/** Restore consumers created by this process: `n` of their group ids `<applicationId>-restore-<taskId>-<n>` (DL14). */
let restoreConsumers = 0

export class RestorationPool implements TaskRestorer {
	private running = 0
	/** Jobs waiting for a slot, in FIFO order. */
	private readonly waiting: (() => void)[] = []

	constructor(private readonly deps: RestorationPoolDeps) {}

	/**
	 * Restores the task's logged stores that restore (raw stores, runtime§6.36) and resolves to the max key time
	 * applied to each window and session store. An aborted job rejects with an AbortError: a queued one at once, a
	 * running one once its store write in flight is done and its progress is checkpointed. A changelog that lost
	 * records the restore needed (C7) is a TaskCorruptedError: the task is revived with its state wiped (4.7.5).
	 */
	async restore(task: RestoringTask, signal: AbortSignal): Promise<{ storeTimes: ReadonlyMap<string, number> }> {
		const restoring = task.stores.flatMap(store => {
			const { changelog } = store
			return changelog === undefined || changelog.skipRestoration ? [] : [{ store, changelog }]
		})
		if (restoring.length === 0) {
			return { storeTimes: new Map() }
		}
		await this.slot(signal)
		const { io, checkpoints, options, applicationId, commitIntervalMs, logger } = this.deps
		try {
			const { maxKeyTime } = await restoreChangelogs(
				{
					consumer: () =>
						io.restoreConsumers(`${applicationId}-restore-${task.id.key}-${++restoreConsumers}`),
					admin: io.admin,
				},
				restoring.map(({ store, changelog }) => target(store, changelog)),
				checkpoints,
				{ ...options, commitIntervalMs, signal, logger: logger.child({ task: task.id.key }) }
			)
			const storeTimes = new Map<string, number>()
			for (const { store, changelog } of restoring) {
				const time = maxKeyTime.get(tpKey(changelog))
				if (time !== undefined) storeTimes.set(store.builder.name, time)
			}
			return { storeTimes }
		} catch (error) {
			if (error instanceof OffsetOutOfRangeError) {
				throw new TaskCorruptedError(
					`Task ${task.id.key} cannot restore its state: ${error.message}`,
					[task.id],
					{ cause: error }
				)
			}
			throw error
		} finally {
			this.release()
		}
	}

	/** Resolves once the job may run; rejects at once when `signal` aborts while it waits. */
	private slot(signal: AbortSignal): Promise<void> {
		if (signal.aborted) {
			return Promise.reject(abortError(signal))
		}
		if (this.running < this.deps.options.maxConcurrentTasks) {
			this.running++
			return Promise.resolve()
		}
		return new Promise((resolve, reject) => {
			const start = () => {
				signal.removeEventListener('abort', onAbort)
				this.running++
				resolve()
			}
			const onAbort = () => {
				this.waiting.splice(this.waiting.indexOf(start), 1)
				reject(abortError(signal))
			}
			signal.addEventListener('abort', onAbort, { once: true })
			this.waiting.push(start)
		})
	}

	private release(): void {
		this.running--
		this.waiting.shift()?.()
	}
}

/**
 * The restore target of a logged store: its raw store, written with the codecs of its changelog writer (window and
 * session stores: windowed keys); window stores report window starts, session stores session ends (6.2).
 */
function target(
	{ builder, raw }: RegisteredStore,
	changelog: { readonly topic: string; readonly partition: number }
): RestoreTarget {
	const codecs = { key: builder.keyCodec, value: builder.valueCodec }
	if (builder.kind === 'key-value') {
		return restoreTarget(changelog, raw as KeyValueStore<unknown, unknown>, codecs)
	}
	const store = raw as KeyValueStore<WindowedKey<unknown>, unknown>
	const keyTime = builder.kind === 'window' ? windowStart : windowEnd
	return restoreTarget(changelog, store, { ...codecs, key: windowedKeyCodec(builder.keyCodec) }, keyTime)
}

const windowStart = (key: WindowedKey<unknown>): number => key.windowStart
const windowEnd = (key: WindowedKey<unknown>): number => key.windowEnd
