/**
 * The runtime of one start() (3.11, 4.2, 4.6.2, 4.11): what must outlive a group membership. It connects, prepares
 * the topics, loads the global tables, runs the Run of the app's group membership, reacts to failures and shuts
 * everything down within the close deadline.
 *
 * In this build any failure stops the app: a failure before start() resolved rejects start(); afterwards the app
 * halts (state ERROR, the run abandoned) until close(). Recovery by rejoining comes with a later build.
 */
import type { Logger } from '@kafkats/client'
import { StreamsError } from '@/api/errors.js'
import type { UncaughtExceptionResponse } from '@/api/handlers.js'
import type { AppLifecycle } from '@/app/lifecycle.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { classify, errorMessage, type FailureSite } from '@/runtime/errors.js'
import { GlobalStateManager } from '@/runtime/global-tables.js'
import type { RuntimeIO } from '@/runtime/io.js'
import { Run } from '@/runtime/run.js'
import { cap, raceSignal } from '@/runtime/signals.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { prepareTopics, type InternalTopicOptions } from '@/topology/internal-topics.js'

/** The run's network awaits are cut this long before the close deadline, so that the local steps still fit. */
const HARD_STOP_MARGIN_MS = 1000
/** Bound of the abandon of a halted run (close cuts it shorter). */
const ABANDON_TIMEOUT_MS = 15_000
const DISCONNECT_TIMEOUT_MS = 3000
const GLOBAL_STOP_TIMEOUT_MS = 3000

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
	 * pipelines). Aborted at close start.
	 */
	readonly shutdown = new AbortController()
	private readonly globals: GlobalStateManager | undefined
	private run: Run | undefined
	/** Stream processing waits for a global table loaded again (4.12 step 3). */
	private paused = false
	private connecting: Promise<void> | undefined
	/** The first failure: it stops the app; later ones are only logged. */
	private failure: { readonly error: unknown } | undefined
	/** Aborted with the failure that fails a start in flight, or with the start timeout. */
	private starting: AbortController | undefined
	private stopping: Promise<void> | undefined

	constructor(private readonly deps: FlowRuntimeDeps) {
		const { topology, io, config, stateDirectory, logger } = deps
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
			const counts = await prepareTopics(topology, admin, this.deps.topics, starting)
			if (this.globals !== undefined) {
				await this.globals.bootstrap(starting)
				this.globals.follow(this.shutdown.signal)
			}
			if (topology.groupTopics.size === 0) {
				return
			}
			const run: Run = new Run({
				...this.deps,
				counts,
				globalStore: name => this.globals!.store(name),
				paused: () => this.paused,
				onFailure: (error, site) => this.fail(error, site, run),
				shutdown: this.shutdown.signal,
			})
			this.run = run
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
	 * Reacts to a failure of the current run (4.6.2), synchronously: the first one wins and fails the run at once, so
	 * that nothing of it is processed or committed afterwards (runtime§6.1). It rejects a start in flight, else halts
	 * the app; during the stop the tasks just close dirty. Never throws.
	 */
	fail(error: unknown, site: FailureSite, run?: Run): void {
		const { logger, lifecycle } = this.deps
		if ((run !== undefined && run !== this.run) || this.failure !== undefined) {
			logger.debug('a later failure is ignored: the first one stops the app', {
				site,
				error: errorMessage(error),
			})
			return
		}
		this.failure = { error }
		this.run?.markFailed()
		if (this.starting !== undefined) {
			this.starting.abort(error)
		} else if (this.stopping !== undefined) {
			lifecycle.recordError(error)
			logger.warn('failed while closing: the tasks close dirty', { site, error: errorMessage(error) })
		} else {
			this.halt(error, site)
		}
	}

	/**
	 * Graceful if the run has not failed (close commit, clean close), else dirty (4.11 steps 2-6); then the stores and
	 * the client. Bounded by `deadline` (clock time): the run's network awaits are cut at `deadline - 1 s`. Memoized:
	 * a failed start and close() share it (CR14).
	 */
	stop(deadline: number): Promise<void> {
		this.stopping ??= this.release(deadline)
		return this.stopping
	}

	/**
	 * The gate of a global table loaded again (4.12 step 3, CR10): the RUNNING tasks are committed, then no slice
	 * starts until resumeProcessing(); commits go on meanwhile.
	 */
	async pauseProcessing(): Promise<void> {
		const pause = (): void => {
			this.paused = true
		}
		if (this.run === undefined) {
			pause()
		} else {
			await this.run.commitAndPause(pause)
		}
	}

	resumeProcessing(): void {
		this.paused = false
		this.run?.scheduler.wake()
	}

	get processingPaused(): boolean {
		return this.paused
	}

	// ---------------------------------------------------------------------------------------------------------

	/** 4.10.3: the uncaught exception handler hears of a fatal failure; the app moves to ERROR and its run ends. */
	private halt(error: unknown, site: FailureSite): void {
		const { lifecycle, logger } = this.deps
		if (classify(error, site) === 'fatal' && this.uncaughtResponse(error) === 'restart') {
			logger.warn(
				"the uncaught exception handler answered 'restart', which this build does not do: the app stops"
			)
		}
		lifecycle.halt(error)
		logger.error('the app stopped after a failure: close() releases it', { site, error: errorMessage(error) })
		const { run } = this
		void run?.abandon(AbortSignal.any([run.hardStop.signal, AbortSignal.timeout(ABANDON_TIMEOUT_MS)]))
		void this.globals?.stop(AbortSignal.timeout(ABANDON_TIMEOUT_MS))
	}

	private uncaughtResponse(error: unknown): UncaughtExceptionResponse {
		try {
			return this.deps.config.handlers.uncaught(error instanceof Error ? error : new Error(String(error)))
		} catch (handlerError) {
			this.deps.logger.warn('the uncaught exception handler threw', { error: errorMessage(handlerError) })
			return 'shutdown'
		}
	}

	private async release(deadline: number): Promise<void> {
		const { io, stateDirectory, checkpoints, connection, logger } = this.deps
		this.shutdown.abort(new StreamsError('the app is closing'))
		const closeSignal = AbortSignal.timeout(Math.max(0, deadline - io.clock.now()))
		const run = this.run
		if (run !== undefined) {
			const cancelHardStop = io.clock.setTimer(deadline - HARD_STOP_MARGIN_MS, () => run.hardStop.abort())
			try {
				await run.closeGracefully(closeSignal)
			} finally {
				cancelHardStop()
			}
		}
		await this.globals?.stop(cap(closeSignal, GLOBAL_STOP_TIMEOUT_MS))
		await stateDirectory.close()
		checkpoints.clearInProcess()
		// After a connect still in flight, so that the client does not stay connected.
		const disconnected = (this.connecting ?? Promise.resolve()).catch(() => {}).then(() => connection.disconnect())
		const remaining = Math.min(DISCONNECT_TIMEOUT_MS, Math.max(0, deadline - io.clock.now()))
		await raceSignal(disconnected, AbortSignal.timeout(remaining)).catch((error: unknown) => {
			logger.warn('disconnecting the client did not finish in time', { error: errorMessage(error) })
		})
	}
}
