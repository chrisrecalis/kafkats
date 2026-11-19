/**
 * Global tables (3.13, 4.12, D12, runtime§6.58): every instance reads each global topic in full, with a consumer of
 * its own outside the group (manual assignment, read_committed, no commits). A table is wiped and loaded up to the
 * end its topic has when the load starts, before any record is processed (the bootstrap), and followed afterwards.
 * A position out of range (records deleted before they were read) loads the table again in place while stream
 * processing waits behind the gate, which commits first. A row that cannot be applied fails the bootstrap, and after
 * it stops the app; a failure of a consumer, or of a load after the bootstrap, is retried with backoff.
 */
import type { Logger, TopicPartition } from '@kafkats/client'
import { IllegalStateError, StreamsError } from '@/api/errors.js'
import { SourceTopicNotFoundError } from '@/changelog/errors.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { classify, errorMessage, RESTART_INITIAL_BACKOFF_MS, RESTART_MAX_BACKOFF_MS } from '@/runtime/errors.js'
import { GlobalReader, type ReaderEnd } from '@/runtime/global-reader.js'
import { GlobalStore } from '@/runtime/global-store.js'
import type { RuntimeIO } from '@/runtime/io.js'
import { cap, delay, raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StateStore } from '@/state.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { GlobalStoreDef } from '@/topology/nodes.js'

/** How long stopping a replaced consumer may take. */
const READER_STOP_TIMEOUT_MS = 5000

export interface GlobalStateManagerDeps {
	readonly io: Pick<RuntimeIO, 'globalConsumers' | 'admin' | 'clock'>
	readonly defs: readonly GlobalStoreDef[]
	readonly stateDirectory: StateDirectory
	readonly config: Pick<
		ResolvedFlowConfig,
		| 'applicationId'
		| 'processId'
		| 'eos'
		| 'emitIntervalMs'
		| 'defaultTimestampExtractor'
		| 'handlers'
		| 'restoration'
	>
	/** A failure after the bootstrap that no retry fixes (a row failure): the app stops (4.10.3). */
	readonly onFatal: (error: Error) => void
	/** Stream processing: pause() commits, then pauses it; resume() lets it go on (FlowRuntime, CR10). */
	readonly gate: { pause(): Promise<void>; resume(): void }
	readonly logger: Logger
}

export class GlobalStateManager {
	private readonly tables: readonly GlobalTable[]
	/** Aborted by stop(): every load, wait and backoff of the tables. */
	private readonly stopped = new AbortController()
	private stopping: Promise<void> | undefined

	constructor(deps: GlobalStateManagerDeps) {
		this.tables = deps.defs.map(def => new GlobalTable(def, deps))
	}

	/**
	 * Loads every table, in parallel (4.12 step 1): wiped, then read to the end offsets (the last stable offsets)
	 * captured now. Rejects with the first failure, an idle timeout, or the abort of `signal` or of stop().
	 */
	async bootstrap(signal: AbortSignal): Promise<void> {
		const loading = AbortSignal.any([signal, this.stopped.signal])
		await Promise.all(this.tables.map(table => table.bootstrap(loading)))
	}

	/** Follows every table from where its bootstrap got to until `signal` aborts or stop() (4.12 steps 2-3). */
	follow(signal: AbortSignal): void {
		const following = AbortSignal.any([signal, this.stopped.signal])
		for (const table of this.tables) table.follow(following)
	}

	/** The read-only view of a global store, which processors of tasks read. */
	store(name: string): StateStore {
		const table = this.tables.find(t => t.name === name)
		if (table === undefined) {
			throw new IllegalStateError(`no global store named '${name}'`)
		}
		return table.view
	}

	/**
	 * Stops loading and following and awaits the consumers; resolves once they stopped or `signal` aborted. Called by
	 * close and by halt (W15): the first call stops, later ones await it.
	 */
	stop(signal: AbortSignal): Promise<void> {
		if (this.stopping === undefined) {
			this.stopped.abort(new StreamsError('the global tables are stopping'))
			this.stopping = Promise.all(this.tables.map(table => table.stop(signal))).then(() => {})
		}
		return raceSignal(this.stopping, signal).catch(() => {})
	}
}

/** One global table: its store and the reader that loads and follows its topic. */
class GlobalTable {
	readonly name: string
	private store: GlobalStore | undefined
	/** The current reader, replaced after a consumer failure and by every load. */
	private reader: GlobalReader | undefined
	/** Failed consumers and loads since the start (the backoff grows with them). */
	private attempt = 0
	private following: Promise<void> | undefined

	constructor(
		private readonly def: GlobalStoreDef,
		private readonly deps: GlobalStateManagerDeps
	) {
		this.name = def.store.name
	}

	get view(): StateStore {
		if (this.store === undefined) {
			throw new IllegalStateError(`global store '${this.name}' is not loaded`)
		}
		return this.store.view
	}

	async bootstrap(signal: AbortSignal): Promise<void> {
		const { config, stateDirectory, io, logger } = this.deps
		this.store ??= await GlobalStore.open(this.def, { config, stateDirectory, clock: io.clock, logger })
		const end = await this.load(signal)
		if (end !== undefined) {
			throw failure(end)
		}
	}

	follow(signal: AbortSignal): void {
		this.following ??= this.maintain(signal)
	}

	/** Called after the manager's signal aborted: no reader starts any more. */
	async stop(signal: AbortSignal): Promise<void> {
		await this.reader?.stop(signal)
		await raceSignal(this.following ?? Promise.resolve(), signal).catch(() => {})
		await this.store?.close().catch((error: unknown) => {
			this.deps.logger.warn('closing a global store processor failed', { store: this.name, ...fields(error) })
		})
	}

	/**
	 * Wipes the store, then reads every partition from its earliest offset to its end offset captured now (the last
	 * stable offset); starts again when a position turns out of range meanwhile. Resolves with how the reader ended
	 * if it did before it caught up (its consumer is stopped then); rejects when listing the partitions or offsets
	 * fails, or with the abort of `signal`.
	 */
	private async load(signal: AbortSignal): Promise<ReaderEnd | undefined> {
		const { admin, clock } = this.deps.io
		const { idleTimeoutMs, checkIntervalMs } = this.deps.config.restoration
		for (;;) {
			await this.stopReader(signal)
			await this.store!.wipe()
			const partitions = await this.partitions(signal)
			const [earliest, end] = await Promise.all([
				admin.listOffsets(partitions, 'earliest', 'read_committed', { signal }),
				admin.listOffsets(partitions, 'latest', 'read_committed', { signal }),
			])
			const reader = this.startReader(partitions, earliest, signal)
			const outcome = await reader.caughtUp(end, { clock, idleTimeoutMs, checkIntervalMs, signal })
			if (outcome.kind === 'caught-up') {
				return undefined
			}
			if (outcome.kind !== 'out-of-range') {
				await this.stopReader(signal)
				return outcome
			}
			this.warnOutOfRange(outcome, 'is loaded again')
		}
	}

	/**
	 * After the bootstrap until `signal` aborts: a failed consumer is replaced after backoff, from the applied
	 * positions; an out-of-range position loads the table again; a row failure, or a failure no retry fixes, stops
	 * the app. An abort is no failure (CR10).
	 */
	private async maintain(signal: AbortSignal): Promise<void> {
		try {
			for (;;) {
				const end = await raceSignal(this.reader!.ended, signal)
				if (end.kind === 'stopped') {
					return
				}
				if (end.kind === 'out-of-range') {
					await this.reload(end, signal)
					continue
				}
				if (!retryable(end)) {
					throw failure(end)
				}
				await this.backoff(end.error, signal)
				this.startReader(this.reader!.partitions, this.reader!.applied, signal)
			}
		} catch (error) {
			if (!signal.aborted) {
				this.deps.logger.error('a global table cannot be kept up to date: the app stops', {
					store: this.name,
					...fields(error),
				})
				this.deps.onFatal(error instanceof Error ? error : new Error(String(error)))
			}
		}
	}

	/**
	 * 4.12 step 3: stream processing pauses (after a commit) while the table is wiped and loaded again; a failed
	 * load is retried with backoff, a row failure throws (the app stops; processing stays paused).
	 */
	private async reload(outOfRange: ReaderEnd & { kind: 'out-of-range' }, signal: AbortSignal): Promise<void> {
		this.warnOutOfRange(outOfRange, 'is loaded again while stream processing waits')
		await raceSignal(this.deps.gate.pause(), signal)
		for (;;) {
			const end = await this.load(signal).catch((error: unknown): ReaderEnd => ({ kind: 'failed', error }))
			if (end === undefined) {
				break
			}
			if (signal.aborted || !retryable(end)) {
				throw failure(end)
			}
			await this.backoff(end.error, signal)
		}
		this.deps.gate.resume()
	}

	/** The topic's partitions; SourceTopicNotFoundError if it has none. */
	private async partitions(signal: AbortSignal): Promise<TopicPartition[]> {
		const { topic } = this.def
		const count = (await this.deps.io.admin.describeTopics([topic], { signal })).get(topic)
		if (!count) {
			throw new SourceTopicNotFoundError(topic, { globalStore: this.name })
		}
		return Array.from({ length: count }, (_, partition) => ({ topic, partition }))
	}

	/** A new reader from `positions`; none once `signal` aborted (the manager stops the last one). */
	private startReader(
		partitions: readonly TopicPartition[],
		positions: ReadonlyMap<string, bigint>,
		signal: AbortSignal
	): GlobalReader {
		signal.throwIfAborted()
		const { applicationId, processId } = this.deps.config
		const consumer = this.deps.io.globalConsumers(`${applicationId}-global-${this.name}-${processId}`)
		const reader = new GlobalReader(consumer, this.store!, partitions, positions)
		this.reader = reader
		reader.start()
		return reader
	}

	private async stopReader(signal: AbortSignal): Promise<void> {
		await this.reader?.stop(cap(signal, READER_STOP_TIMEOUT_MS))
	}

	/** Backoff `min(30 s, 500 ms * 2^min(attempt, 10))`, the attempt counted first and never reset (runtime§6.58). */
	private async backoff(error: unknown, signal: AbortSignal): Promise<void> {
		this.attempt++
		const ms = Math.min(RESTART_MAX_BACKOFF_MS, RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(this.attempt, 10))
		this.deps.logger.warn('a global table consumer failed: it starts again after a backoff', {
			store: this.name,
			attempt: this.attempt,
			backoffMs: ms,
			...fields(error),
		})
		await this.stopReader(signal)
		await delay(this.deps.io.clock, ms, signal)
	}

	private warnOutOfRange(end: { tp: TopicPartition; offset: bigint }, action: string): void {
		this.deps.logger.warn(`a global table's position is out of range: the table ${action}`, {
			store: this.name,
			partition: tpKey(end.tp),
			// A string: the client's logger writes JSON, which has no bigint.
			offset: end.offset.toString(),
		})
	}
}

/** A consumer or load failure that a new attempt may fix; not a row failure (the same rows would fail again). */
function retryable(end: ReaderEnd): end is ReaderEnd & { kind: 'failed' } {
	return end.kind === 'failed' && classify(end.error, 'global') !== 'fatal'
}

/** The error of a reader that ended without catching up. */
function failure(end: ReaderEnd): unknown {
	return 'error' in end ? end.error : new StreamsError(`the global table reader ended (${end.kind})`)
}

function fields(error: unknown): { error: string } {
	return { error: errorMessage(error) }
}
