/**
 * The runtime of one start() (3.11, 4.2, 4.6.2, 4.10, 4.11): what outlives a group membership. It connects, prepares
 * the topics, loads the global tables and runs the app's group membership (a Run), reacts to failures like Kafka
 * Streams, and shuts everything down within the close deadline.
 *
 * Failures (4.6.2): corrupted tasks are revived in place; a migration abandons the run and joins again with a new one
 * after a backoff (D10); a fatal failure goes to the uncaught exception handler, which halts the app (state ERROR)
 * unless it answers 'restart'. Until the first assignment is RUNNING, every failure but a corruption rejects start()
 * instead.
 */
import type { Logger } from '@kafkats/client'
import { StreamsError, type TaskCorruptedError } from '@/api/errors.js'
import type { UncaughtExceptionResponse } from '@/api/handlers.js'
import type { AppLifecycle } from '@/app/lifecycle.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import {
	classify,
	errorMessage,
	RESTART_INITIAL_BACKOFF_MS,
	RESTART_MAX_BACKOFF_MS,
	type FailureSite,
} from '@/runtime/errors.js'
import { GlobalStateManager } from '@/runtime/global-tables.js'
import type { Clock, RuntimeIO } from '@/runtime/io.js'
import { Run } from '@/runtime/run.js'
import { cap, delay, raceSignal } from '@/runtime/signals.js'
import { StartPositions } from '@/runtime/start-positions.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { prepareTopics, type InternalTopicOptions } from '@/topology/internal-topics.js'
import type { PartitionCounts, StoreBuilder } from '@/topology/nodes.js'

/** The run's network awaits are cut this long before the close deadline, so that the local steps still fit. */
const HARD_STOP_MARGIN_MS = 1000
/** Bound of the abandon of a failed run (a close cuts it shorter). */
const ABANDON_TIMEOUT_MS = 15_000
const DISCONNECT_TIMEOUT_MS = 3000
const GLOBAL_STOP_TIMEOUT_MS = 3000
/** The backoff doubles with each recovery after which no run committed, up to 2^10 times the initial one. */
const MAX_BACKOFF_DOUBLINGS = 10

/** The app's client: connected at start, disconnected at the end of the start if the app created it. */
export interface ClusterConnection {
	connect(): Promise<void>
	disconnect(): Promise<void>
}

export interface FlowRuntimeDeps {
	readonly topology: BuiltTopology
	/** The start's resolved config (with its process id). */
	readonly config: ResolvedFlowConfig
	readonly io: RuntimeIO
	readonly connection: ClusterConnection
	readonly lifecycle: Pick<AppLifecycle, 'setRunning' | 'setRebalancing' | 'halt' | 'recordError'>
	readonly stateDirectory: StateDirectory
	readonly checkpoints: Checkpoints
	/** FlowConfig.changelog: replication, configs and auto-creation of internal topics. */
	readonly topics: InternalTopicOptions
	readonly logger: Logger
}

export class FlowRuntime {
	/**
	 * Background waits of this start (connect, topics, global bootstrap and follow, the start wait, restore
	 * pipelines, the recovery backoff). Aborted at close start.
	 */
	readonly shutdown = new AbortController()
	private readonly globals: GlobalStateManager | undefined
	/** 'latest' pins of this start: every run of it settles start positions with them (4.7.4). */
	private readonly startPositions: StartPositions
	private counts: PartitionCounts | undefined
	/** The current run: the live one, or the failed one until its recovery replaced it. */
	private run: Run | undefined
	/**
	 * Global tables being loaded again (4.12 step 3): stream processing waits until the last of them is, so a count of
	 * pauses, not a flag (each table reloads on its own).
	 */
	private pauses = 0
	private connecting: Promise<void> | undefined
	/** The first fatal failure: the app halted. */
	private fatal: { readonly error: unknown } | undefined
	/** A migration in progress: the failed run is abandoned, then a new one joins after the backoff. */
	private recovering = false
	/** Aborted by a fatal failure: ends the backoff of a recovery, which then starts no run (CR5, runtime§6.49). */
	private readonly recoveryAbort = new AbortController()
	/** Recoveries since a run last committed: the backoff doubles with them (runtime§6.50). */
	private attempt = 0
	/**
	 * Aborted with the failure that fails a start in flight, or with the start timeout. Set until start() returns; a
	 * failure fails the start only until the first assignment is RUNNING (`run.tasks.started`).
	 */
	private starting: AbortController | undefined
	private stopping: Promise<void> | undefined

	constructor(private readonly deps: FlowRuntimeDeps) {
		const { topology, io, config, stateDirectory, logger } = deps
		this.startPositions = new StartPositions({
			policy: topic => topology.resetPolicy(topic, config.defaultResetPolicy),
			logger,
		})
		if (topology.globalStores.length > 0) {
			this.globals = new GlobalStateManager({
				io,
				defs: topology.globalStores,
				stateDirectory,
				config,
				onFatal: error => this.fail(error, 'global'),
				gate: { pause: () => this.pauseProcessing(), resume: () => this.resumeProcessing() },
				logger,
			})
		}
	}

	/**
	 * 4.2 steps 3-8: connects, resolves the partition counts and creates the internal topics, loads the global tables
	 * (then follows them), then joins the group and resolves once every task of the first assignment is RUNNING.
	 * Rejects with the first failure before that, or when no assignment arrived within `startTimeoutMs`; restoration
	 * and the global bootstrap are not bounded by it.
	 */
	async start(signal: AbortSignal): Promise<void> {
		const { topology, io, config, logger } = this.deps
		const failed = new AbortController()
		this.starting = failed
		const starting = AbortSignal.any([signal, failed.signal])
		let cancelTimeout = (): void => {}
		try {
			this.connecting = this.deps.connection.connect()
			await raceSignal(this.connecting, starting)
			const admin = { admin: io.admin, clock: io.clock, logger }
			this.counts = await prepareTopics(topology, admin, this.deps.topics, starting)
			this.deps.stateDirectory.register(storePartitions(topology, this.counts))
			if (this.globals !== undefined) {
				await this.globals.bootstrap(starting)
				this.globals.follow(this.shutdown.signal)
			}
			if (topology.groupTopics.size === 0) {
				return
			}
			const run = this.newRun()
			cancelTimeout = io.clock.setTimer(io.clock.now() + config.startTimeoutMs, () => {
				if (!run.assigned) {
					failed.abort(
						new StreamsError(`did not join the group within startTimeoutMs (${config.startTimeoutMs} ms)`)
					)
				}
			})
			run.start()
			await run.tasks.whenAllRunning(starting)
		} finally {
			cancelTimeout()
			this.starting = undefined
		}
	}

	/**
	 * Reacts to a failure of the current run, or outside any run (4.6.2), synchronously; never throws. A failed run
	 * processes and commits nothing more from here on (runtime§6.1). The first failure wins (runtime§6.2), except
	 * that a fatal one outranks a recovery in progress.
	 */
	fail(error: unknown, site: FailureSite, run?: Run): void {
		const { logger, lifecycle } = this.deps
		if (run !== undefined && run !== this.run) {
			logger.debug('a failure of an abandoned run is ignored', fields(site, error))
			return
		}
		if (this.stopping !== undefined) {
			// Also a corruption: the close must not commit what the corrupted tasks processed.
			this.run?.markFailed()
			lifecycle.recordError(error)
			logger.warn('failed while closing: the tasks close dirty', fields(site, error))
			return
		}
		const kind = classify(error, site)
		if (kind === 'corrupted') {
			// Reported by getError(): the revived tasks reprocess from their last commit.
			lifecycle.recordError(error)
			this.run?.revive(error as TaskCorruptedError, { rebuild: site === 'restore' })
			return
		}
		if (this.starting !== undefined && !this.run?.tasks.started) {
			// start() rejects with it (4.2 step 8); the start's stop closes the run dirty. Once the first assignment is
			// RUNNING, start() resolves whatever fails: a failure meanwhile is handled like any later one.
			this.run?.markFailed()
			this.starting.abort(error)
			return
		}
		if (kind === 'fatal' && this.fatal === undefined && this.uncaughtResponse(error, site) === 'shutdown') {
			this.fatal = { error }
			this.run?.markFailed()
			this.recoveryAbort.abort()
			void this.halt(error, site)
			return
		}
		if (this.fatal !== undefined || this.recovering || this.run === undefined) {
			logger.debug('a later failure is ignored: the first one is being handled', fields(site, error))
			return
		}
		this.recovering = true
		this.run.markFailed()
		void this.recover(this.run, error, site)
	}

	/**
	 * Graceful if the run has not failed (close commit, clean close), else dirty (4.11 steps 2-6); then the stores and
	 * the client. Bounded by `deadline` (clock time): the run's network awaits are cut at `deadline - 1 s`, every
	 * other await at `deadline`. Memoized: a failed start and close() share it (CR14).
	 */
	stop(deadline: number): Promise<void> {
		this.stopping ??= this.release(deadline)
		return this.stopping
	}

	/**
	 * The gate of a global table loaded again (4.12 step 3, CR10): the RUNNING tasks are committed, then no slice
	 * starts until every pause was matched by a resumeProcessing(); commits go on meanwhile.
	 */
	async pauseProcessing(): Promise<void> {
		const pause = (): void => {
			this.pauses++
		}
		if (this.run === undefined) {
			pause()
		} else {
			await this.run.commitAndPause(pause)
		}
	}

	resumeProcessing(): void {
		this.pauses--
		if (this.pauses === 0) {
			this.run?.scheduler.wake()
		}
	}

	get processingPaused(): boolean {
		return this.pauses > 0
	}

	// ---------------------------------------------------------------------------------------------------------

	/** A run of this start: rebalance hooks, failures and the processing gate go through this runtime. */
	private newRun(): Run {
		const run: Run = new Run({
			...this.deps,
			counts: this.counts!,
			startPositions: this.startPositions,
			globalStore: name => this.globals!.store(name),
			paused: () => this.processingPaused,
			onFailure: (error, site) => this.fail(error, site, run),
			shutdown: this.shutdown.signal,
		})
		this.run = run
		return run
	}

	/**
	 * Task migration (4.10.2, KS `handleTaskMigrated`): the failed run is abandoned (its tasks close dirty, its
	 * consumer leaves the group), then a new run joins after the backoff, from the committed offsets. The abandon
	 * stops waiting for a task close that takes long (a store being wiped, a processor's close): the new run starts
	 * only once every store was released, so that no store is leased, restored or wiped by two tasks at once (KS
	 * retries its state directory lock; runtime§6.31). A close or a fatal failure meanwhile ends the recovery there.
	 */
	private async recover(failed: Run, error: unknown, site: FailureSite): Promise<void> {
		const { lifecycle, io, stateDirectory, logger } = this.deps
		lifecycle.setRebalancing()
		lifecycle.recordError(error)
		this.attempt = failed.commits.committedOnce ? 1 : this.attempt + 1
		const backoff = Math.min(
			RESTART_MAX_BACKOFF_MS,
			RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(this.attempt - 1, MAX_BACKOFF_DOUBLINGS)
		)
		logger.warn(`the app joins the group again in ${backoff} ms after a failure`, fields(site, error))
		await failed.abandon(abandonSignal(failed))
		const interrupted = AbortSignal.any([this.shutdown.signal, this.recoveryAbort.signal])
		try {
			await raceSignal(stateDirectory.released(), interrupted)
			await delay(io.clock, backoff, interrupted)
		} catch {
			return
		}
		if (this.shutdown.signal.aborted || this.fatal !== undefined) {
			return
		}
		const run = this.newRun()
		this.recovering = false
		run.start()
	}

	/**
	 * 4.10.3: the app moves to ERROR, its run is abandoned and the global tables stop (W15). The stores stay with the
	 * provider until close() releases them; start() keeps answering with its resolved promise until then.
	 */
	private async halt(error: unknown, site: FailureSite): Promise<void> {
		const { lifecycle, logger } = this.deps
		lifecycle.halt(error)
		logger.error('the app stopped after a failure: close() releases it', fields(site, error))
		const { run } = this
		await run?.abandon(abandonSignal(run))
		await this.globals?.stop(AbortSignal.timeout(ABANDON_TIMEOUT_MS))
	}

	/** KS cannot replace its global thread: 'restart' after a global table failure shuts down (KS REPLACE_THREAD). */
	private uncaughtResponse(error: unknown, site: FailureSite): UncaughtExceptionResponse {
		const { config, logger } = this.deps
		let response: UncaughtExceptionResponse
		try {
			response = config.handlers.uncaught(error instanceof Error ? error : new Error(String(error)))
		} catch (handlerError) {
			logger.warn('the uncaught exception handler threw: the app stops', { error: errorMessage(handlerError) })
			return 'shutdown'
		}
		if (response === 'restart' && site === 'global') {
			logger.warn("a global table cannot be restarted: the uncaught exception handler's 'restart' stops the app")
			return 'shutdown'
		}
		return response === 'restart' ? 'restart' : 'shutdown'
	}

	private async release(deadline: number): Promise<void> {
		const { io, stateDirectory, checkpoints, connection, logger } = this.deps
		this.shutdown.abort(new StreamsError('the app is closing'))
		const close = deadlineSignal(io.clock, deadline)
		const { run } = this
		const cancelHardStop = run && io.clock.setTimer(deadline - HARD_STOP_MARGIN_MS, () => run.hardStop.abort())
		try {
			await run?.closeGracefully(close.signal)
			await this.globals?.stop(cap(close.signal, GLOBAL_STOP_TIMEOUT_MS))
			await stateDirectory.close()
			checkpoints.clearInProcess()
			// After a connect still in flight, so that the client does not stay connected.
			const disconnected = (this.connecting ?? Promise.resolve())
				.catch(() => {})
				.then(() => connection.disconnect())
			await raceSignal(disconnected, cap(close.signal, DISCONNECT_TIMEOUT_MS)).catch((error: unknown) => {
				logger.warn('disconnecting the client did not finish in time', { error: errorMessage(error) })
			})
		} finally {
			cancelHardStop?.()
			close.cancel()
		}
	}
}

/** Bounds the abandon of a failed run; a close cuts it at its hard stop. */
function abandonSignal(run: Run): AbortSignal {
	return cap(run.hardStop.signal, ABANDON_TIMEOUT_MS)
}

/** A signal aborted at `at` on `clock` (the close deadline). */
function deadlineSignal(clock: Clock, at: number): { signal: AbortSignal; cancel: () => void } {
	const controller = new AbortController()
	const cancel = clock.setTimer(at, () => controller.abort(new StreamsError('the close deadline passed')))
	return { signal: controller.signal, cancel }
}

function fields(site: FailureSite, error: unknown): { site: FailureSite; error: string } {
	return { site, error: errorMessage(error) }
}

/** Every store of the topology with the partition count of its changelog (none without one). */
function storePartitions(
	topology: BuiltTopology,
	counts: PartitionCounts
): { readonly builder: StoreBuilder; readonly partitions: number }[] {
	return topology.subtopologies.flatMap(subtopology =>
		subtopology.stores.map(builder => ({
			builder,
			partitions: builder.logging.enabled ? (counts.get(builder.logging.topic) ?? 0) : 0,
		}))
	)
}
