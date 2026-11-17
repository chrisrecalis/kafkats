/**
 * One replay of changelog partitions (4.7.2 steps 5-9): a throwaway restore consumer reads each partition from its
 * start up to the end offset captured before the replay, and the records are applied to the partition's store batch
 * by batch. Ends when every partition is caught up; fails on a stall unless the replay waits out an open transaction;
 * an abort ends it once the batch being applied is written.
 */
import { OffsetOutOfRangeError, type Logger, type Message, type TopicPartition } from '@kafkats/client'
import type { AdminPort, RestoreConsumerPort } from '@/runtime/io.js'
import { abortError, cap } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { Checkpoints } from '@/state/checkpoints.js'

/** A changelog partition to replay, and how its records reach its store. */
export interface ReplayTarget {
	readonly topic: string
	readonly partition: number
	/** Applies a batch to the raw store (latest change per key, decoded, written); resolves to the max key time, -1 none. */
	readonly apply: (batch: readonly Message<Buffer, Buffer>[]) => Promise<number>
}

export interface ReplayOptions {
	readonly idleTimeoutMs: number
	readonly initialIdleTimeoutMs: number
	readonly checkIntervalMs: number
	/** Progress checkpoints are written at most this often. */
	readonly commitIntervalMs: number
	readonly signal: AbortSignal
	readonly logger: Logger
}

export interface ReplayResult {
	/** Records applied that are not tombstones (overwritten ones included). */
	readonly restored: number
	/** Max key time applied per changelog partition (`topic:partition`). */
	readonly maxKeyTime: ReadonlyMap<string, number>
}

/**
 * How often the consumer's positions are read. Polled because a partition whose last records are transaction markers
 * reaches its end without a batch; reaching it is also progress.
 */
const POSITION_POLL_MS = 50
/** Upper bound for waiting on a dangling transaction (brokers cap transaction timeouts at 15 minutes by default). */
const MAX_OPEN_TRANSACTION_WAIT_MS = 16 * 60_000
const LIST_OFFSETS_TIMEOUT_MS = 10_000
const STOP_TIMEOUT_MS = 5000

/** One partition of the replay. */
interface Partition {
	readonly tp: TopicPartition
	readonly target: ReplayTarget
	readonly end: bigint
	/** Every record below this offset is applied. */
	applied: bigint
	/** The last progress checkpoint (initially the start). */
	checkpointed: bigint
	/** The consumer's position when last read. */
	position: bigint | undefined
	/** A batch of this partition is being applied. */
	applying: boolean
	maxKeyTime: number
	done: boolean
	/** The last stable offset at which an open transaction held this partition at the last stall check. */
	heldAt: bigint | undefined
}

type Outcome = { readonly failure?: unknown }

/** Why a replay makes no progress (see `openTransactions`). */
type Stall = 'held' | 'released' | 'stalled'

export class Replay {
	private readonly partitions = new Map<string, Partition>()
	private consumer: RestoreConsumerPort | undefined
	private readonly outcome: Promise<Outcome>
	private end!: (outcome: Outcome) => void
	private over = false
	/** The batch being applied: an abort waits for it. */
	private applying: Promise<void> = Promise.resolve()
	private restored = 0
	private startedAt = 0
	private lastProgressAt = 0
	private lastCheckpointAt = 0
	private madeProgress = false
	private stallCheckRunning = false
	private warnedOpenTransaction = false

	constructor(
		private readonly io: { readonly consumer: () => RestoreConsumerPort; readonly admin: AdminPort },
		partitions: readonly { readonly target: ReplayTarget; readonly start: bigint; readonly end: bigint }[],
		private readonly checkpoints: Checkpoints,
		private readonly options: ReplayOptions
	) {
		this.outcome = new Promise(resolve => {
			this.end = resolve
		})
		for (const { target, start, end } of partitions) {
			const tp = { topic: target.topic, partition: target.partition }
			this.partitions.set(tpKey(tp), {
				tp,
				target,
				end,
				applied: start,
				checkpointed: start,
				position: start,
				applying: false,
				maxKeyTime: -1,
				done: false,
				heldAt: undefined,
			})
		}
	}

	/** Rejects with the failure, or an AbortError, after the batch in flight and a progress checkpoint. */
	async run(): Promise<ReplayResult> {
		const { signal, checkIntervalMs } = this.options
		if (signal.aborted) {
			throw abortError(signal)
		}
		const consumer = this.io.consumer()
		this.consumer = consumer
		consumer.assign([...this.partitions.values()].map(p => ({ ...p.tp, offset: p.applied })))
		this.startedAt = this.lastProgressAt = this.lastCheckpointAt = Date.now()
		const onAbort = () => this.finish({ failure: abortError(signal) })
		signal.addEventListener('abort', onAbort, { once: true })
		const poll = setInterval(() => this.poll(), POSITION_POLL_MS)
		const stallCheck = setInterval(() => this.checkForStall(), checkIntervalMs)
		consumer
			.run(
				(tp, messages) => this.handle(tp, messages),
				(tp, offset) => this.finish({ failure: new OffsetOutOfRangeError(tp.topic, tp.partition, offset) })
			)
			.then(
				() => this.finish({ failure: new Error('The restore consumer stopped before the restore completed') }),
				(error: unknown) => this.finish({ failure: error })
			)

		const outcome = await this.outcome
		clearInterval(poll)
		clearInterval(stallCheck)
		signal.removeEventListener('abort', onAbort)
		this.stopInBackground(consumer)
		await this.applying
		if ('failure' in outcome) {
			await this.checkpointProgress()
			throw outcome.failure
		}
		const maxKeyTime = new Map<string, number>()
		for (const [key, p] of this.partitions) {
			if (p.maxKeyTime >= 0) maxKeyTime.set(key, p.maxKeyTime)
		}
		return { restored: this.restored, maxKeyTime }
	}

	/** The consumer's handler: one batch of one partition, in offset order; the next is not delivered before it settled. */
	private handle(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): Promise<void> {
		this.applying = this.apply(tp, messages)
		return this.applying
	}

	private async apply(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): Promise<void> {
		const p = this.partitions.get(tpKey(tp))
		const last = messages.at(-1)
		if (this.over || p === undefined || p.done || last === undefined) {
			return
		}
		p.applying = true
		try {
			// Records past the captured end belong to a concurrent writer: its owner has them in its own state.
			const records = messages.filter(message => message.offset < p.end)
			if (records.length > 0) {
				p.maxKeyTime = Math.max(p.maxKeyTime, await p.target.apply(records))
				this.restored += records.filter(record => record.key !== null && record.value !== null).length
			}
			p.applied = last.offset + 1n < p.end ? last.offset + 1n : p.end
			this.progressed()
			if (!this.over && Date.now() - this.lastCheckpointAt >= this.options.commitIntervalMs) {
				await this.checkpointProgress()
			}
		} catch (error) {
			this.finish({ failure: error })
			return
		} finally {
			p.applying = false
		}
		this.completeIfCaughtUp(p)
	}

	/**
	 * Progress checkpoints (KF4): every partition applied beyond its last checkpoint, then one flush, which makes the
	 * store writes before it durable (5.7). Never ahead of the store (runtime§6.14), within the log, so trusted later.
	 */
	private async checkpointProgress(): Promise<void> {
		this.lastCheckpointAt = Date.now()
		const progressed = [...this.partitions.values()].filter(p => p.applied > p.checkpointed)
		if (progressed.length === 0) {
			return
		}
		await Promise.all(
			progressed.map(p => {
				p.checkpointed = p.applied
				return this.checkpoints.set(p.tp.topic, p.tp.partition, p.applied)
			})
		)
		await this.checkpoints.flush()
	}

	/** Reads the position of every partition not caught up: a move is progress, the end completes it. */
	private poll(): void {
		for (const p of this.partitions.values()) {
			if (p.done) continue
			const position = this.consumer?.position(p.tp)
			if (position !== undefined && position !== p.position) {
				p.position = position
				this.progressed()
			}
			this.completeIfCaughtUp(p)
		}
	}

	/**
	 * A partition is caught up once the consumer's position (its next record to deliver) reached the end and no batch of
	 * it is being applied. The last record alone is not enough: on transactional changelogs the end sits past trailing
	 * commit markers.
	 */
	private completeIfCaughtUp(p: Partition): void {
		if (this.over || p.done || p.applying) {
			return
		}
		const position = this.consumer?.position(p.tp)
		if (position === undefined || position < p.end) {
			return
		}
		p.done = true
		this.consumer?.pause([p.tp])
		if ([...this.partitions.values()].every(other => other.done)) {
			this.finish({})
		}
	}

	private progressed(): void {
		this.lastProgressAt = Date.now()
		this.madeProgress = true
	}

	/** Every checkIntervalMs: fails the replay after too long without progress, unless an open transaction holds it. */
	private checkForStall(): void {
		const applying = [...this.partitions.values()].some(p => p.applying)
		// A slow store write is progress in the making, not a stall.
		if (this.over || this.stallCheckRunning || applying) {
			return
		}
		const timeoutMs = this.madeProgress ? this.options.idleTimeoutMs : this.options.initialIdleTimeoutMs
		if (Date.now() - this.lastProgressAt <= timeoutMs) {
			return
		}
		this.stallCheckRunning = true
		void this.openTransactions()
			.catch((): Stall => 'stalled')
			.then(stall => {
				this.stallCheckRunning = false
				if (this.over) return
				if (stall === 'held' && Date.now() - this.startedAt < MAX_OPEN_TRANSACTION_WAIT_MS) {
					this.waitForOpenTransaction()
					return
				}
				if (stall === 'released') {
					// The consumer's fetch of the records the transaction's end made stable may still be in flight.
					this.lastProgressAt = Date.now()
					return
				}
				const pending = this.pending().map(p => tpKey(p.tp))
				this.finish({
					failure: new Error(
						`Timed out restoring changelog partitions ${pending.join(', ')}: no progress for ${timeoutMs}ms`
					),
				})
			})
	}

	/**
	 * 'held': every partition not caught up consumed all stable records and waits behind an open transaction (LSO <
	 * end). 'released': not held, but the LSO of a partition held at the previous check moved since (its transaction
	 * ended in between). Else 'stalled'.
	 */
	private async openTransactions(): Promise<Stall> {
		const pending = this.pending()
		const stable = await this.io.admin.listOffsets(
			pending.map(p => p.tp),
			'latest',
			'read_committed',
			{ signal: cap(this.options.signal, LIST_OFFSETS_TIMEOUT_MS) }
		)
		let held = true
		let released = false
		for (const p of pending) {
			const lastStable = stable.get(tpKey(p.tp))
			const position = this.consumer?.position(p.tp)
			released ||= p.heldAt !== undefined && lastStable !== undefined && lastStable > p.heldAt
			const waiting =
				lastStable !== undefined && position !== undefined && lastStable < p.end && position >= lastStable
			p.heldAt = waiting ? lastStable : undefined
			held &&= waiting
		}
		return held ? 'held' : released ? 'released' : 'stalled'
	}

	/** Not a stall: the broker aborts the dangling transaction at its timeout, then the remaining records arrive. */
	private waitForOpenTransaction(): void {
		this.lastProgressAt = Date.now()
		if (this.warnedOpenTransaction) return
		this.warnedOpenTransaction = true
		this.options.logger.warn('a changelog restore is waiting for an open transaction to complete', {
			partitions: this.pending().map(p => tpKey(p.tp)),
		})
	}

	private pending(): Partition[] {
		return [...this.partitions.values()].filter(p => !p.done)
	}

	private finish(outcome: Outcome): void {
		if (this.over) return
		this.over = true
		this.end(outcome)
	}

	/** Bounded and caught, never awaited: nothing waits for a consumer to leave (CR2). */
	private stopInBackground(consumer: RestoreConsumerPort): void {
		consumer.stop({ signal: AbortSignal.timeout(STOP_TIMEOUT_MS) }).catch((error: unknown) => {
			this.options.logger.debug('a restore consumer did not stop in time', {
				error: error instanceof Error ? error.message : String(error),
			})
		})
	}
}
