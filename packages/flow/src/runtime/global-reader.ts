/**
 * One consumer of a global topic (4.12): it reads every partition from given positions and applies each batch to the
 * global store, one batch at a time. The global state manager waits on it to catch up (the load) and watches how it
 * ends (the follow).
 */
import { TimeoutError, type Message, type TopicPartition } from '@kafkats/client'
import { StreamsError } from '@/api/errors.js'
import type { GlobalStore } from '@/runtime/global-store.js'
import type { Clock, RestoreConsumerPort } from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'

/** Why a reader stopped applying records; it applies nothing after that. */
export type ReaderEnd =
	/** A position is out of range (records deleted before they were read, or a truncated topic). */
	| { readonly kind: 'out-of-range'; readonly tp: TopicPartition; readonly offset: bigint }
	/** Applying a batch failed: the same rows would fail again. */
	| { readonly kind: 'row-failure'; readonly error: unknown }
	/** The consumer's run failed or ended, or the reader made no progress in time while catching up. */
	| { readonly kind: 'failed'; readonly error: unknown }
	| { readonly kind: 'stopped' }

export interface CatchUpOptions {
	readonly clock: Clock
	readonly idleTimeoutMs: number
	readonly checkIntervalMs: number
	/** Aborting it rejects the wait with its reason. */
	readonly signal: AbortSignal
}

export class GlobalReader {
	/** The next offset to apply per partition, by `topic:partition`: where a successor starts. */
	readonly applied: Map<string, bigint>
	/** Settles with the first reason the reader stopped applying records. */
	readonly ended: Promise<ReaderEnd>
	private end!: (end: ReaderEnd) => void
	private isEnded = false
	/** A batch is being applied: positions may be ahead of what is applied. */
	private applying = false
	private batches = 0
	/** Called after each applied batch while a catch-up waits. */
	private onApplied: (() => void) | undefined

	constructor(
		private readonly consumer: RestoreConsumerPort,
		private readonly store: GlobalStore,
		readonly partitions: readonly TopicPartition[],
		positions: ReadonlyMap<string, bigint>
	) {
		this.applied = new Map(positions)
		this.ended = new Promise(resolve => {
			this.end = end => {
				if (this.isEnded) return
				this.isEnded = true
				resolve(end)
			}
		})
	}

	/** Starts the consumer from the positions. */
	start(): void {
		const { consumer } = this
		consumer.assign(this.partitions.map(tp => ({ ...tp, offset: this.applied.get(tpKey(tp))! })))
		consumer
			.run(
				(tp, messages) => this.apply(tp, messages),
				(tp, offset) => this.end({ kind: 'out-of-range', tp, offset })
			)
			.then(
				() => this.end({ kind: 'failed', error: new StreamsError('the global table consumer stopped') }),
				(error: unknown) => this.end({ kind: 'failed', error })
			)
	}

	/**
	 * Resolves with 'caught-up' once every partition's position reached its end offset in `end` (checked after each
	 * applied batch and every `checkIntervalMs`, never while a batch is being applied), else with how the reader
	 * ended; a reader that neither moved a position nor applied a batch for `idleTimeoutMs` ends as failed with a
	 * TimeoutError. Rejects with the signal's reason once it aborts.
	 */
	caughtUp(
		end: ReadonlyMap<string, bigint>,
		options: CatchUpOptions
	): Promise<{ readonly kind: 'caught-up' } | ReaderEnd> {
		const { clock, idleTimeoutMs, checkIntervalMs, signal } = options
		const pending = new Map(this.partitions.map(tp => [tpKey(tp), { tp, end: end.get(tpKey(tp)) ?? 0n }]))
		return new Promise((resolve, reject) => {
			let progress = this.progress()
			let progressAt = clock.now()
			let cancelTimer = (): void => {}
			let settled = false
			const settle = (finish: () => void): void => {
				if (settled) return
				settled = true
				cancelTimer()
				this.onApplied = undefined
				signal.removeEventListener('abort', onAbort)
				finish()
			}
			const check = (): void => {
				if (this.applying) return
				for (const [key, { tp, end }] of pending) {
					if ((this.consumer.position(tp) ?? this.applied.get(key)!) >= end) pending.delete(key)
				}
				if (pending.size === 0) settle(() => resolve({ kind: 'caught-up' }))
			}
			const tick = (): void => {
				check()
				if (settled) return
				const now = clock.now()
				const current = this.progress()
				if (current !== progress) {
					progress = current
					progressAt = now
				} else if (now - progressAt >= idleTimeoutMs) {
					const error = new TimeoutError(
						`loading global store '${this.store.def.store.name}' made no progress for ${idleTimeoutMs} ms`,
						idleTimeoutMs
					)
					settle(() => resolve({ kind: 'failed', error }))
					return
				}
				cancelTimer = clock.setTimer(now + checkIntervalMs, tick)
			}
			const onAbort = (): void => settle(() => reject(signal.reason))
			if (signal.aborted) {
				reject(signal.reason)
				return
			}
			signal.addEventListener('abort', onAbort, { once: true })
			void this.ended.then(ended => settle(() => resolve(ended)))
			this.onApplied = check
			check()
			if (!settled) cancelTimer = clock.setTimer(clock.now() + checkIntervalMs, tick)
		})
	}

	/** Ends the reader and stops its consumer; resolves once it stopped or `signal` aborted. */
	async stop(signal: AbortSignal): Promise<void> {
		this.end({ kind: 'stopped' })
		await this.consumer.stop({ signal }).catch(() => {})
	}

	/** A batch of one partition, in offset order; ignored once the reader ended (the table is read again anyway). */
	private async apply(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): Promise<void> {
		if (this.isEnded) return
		this.applying = true
		try {
			await this.store.apply(messages)
		} catch (error) {
			this.end({ kind: 'row-failure', error })
			throw error
		} finally {
			this.applying = false
		}
		this.applied.set(tpKey(tp), messages.at(-1)!.offset + 1n)
		this.batches++
		this.onApplied?.()
	}

	/** Changes whenever a position moves or a batch is applied. */
	private progress(): string {
		const positions = this.partitions.map(tp => this.consumer.position(tp) ?? this.applied.get(tpKey(tp)))
		return `${this.batches}@${positions.join(',')}`
	}
}
