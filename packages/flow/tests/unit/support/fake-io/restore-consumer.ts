import type { Message, TopicPartition } from '@kafkats/client'
import type { RestoreConsumerPort } from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { FakeCluster } from './cluster.js'
import { deferred, Steps } from './steps.js'

type BatchHandler = (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => Promise<void>

/**
 * A restore or global-table consumer (3.6) over the cluster's logs: manual assignment, read_committed, no commits.
 * Its run hands the committed records of the partitions, in turn, to the handler in batches of at most `batchSize`
 * (each awaited; the position moves when a batch is fetched, before the handler runs), then waits for the logs to
 * change. A position outside its log (before the log start or past the end) is reported out of range and its
 * partition paused (reset policy 'none' with the C7 signal).
 */
export class FakeRestoreConsumer implements RestoreConsumerPort {
	/** Every call in order: 'assign a:0@5', 'run', 'pause a:0', 'stop'. */
	readonly calls: string[] = []
	readonly steps = { stop: new Steps() }
	batchSize = 100
	/** Assigned partitions and their fetch positions, by `topic:partition`. */
	private readonly assigned = new Map<string, { tp: TopicPartition; position: bigint }>()
	private readonly paused = new Set<string>()
	private loop: Promise<void> | undefined
	private stopping = false
	private failure: { error: unknown } | undefined
	private wake = deferred()
	/** Where the next fetch starts looking, so that partitions take turns. */
	private turn = 0

	constructor(
		readonly cluster: FakeCluster,
		readonly groupId: string
	) {}

	assign(partitions: readonly { topic: string; partition: number; offset: bigint }[]): void {
		this.calls.push(`assign ${partitions.map(p => `${tpKey(p)}@${p.offset}`).join(' ')}`)
		for (const { topic, partition, offset } of partitions) {
			this.assigned.set(tpKey({ topic, partition }), { tp: { topic, partition }, position: offset })
		}
		this.poke()
	}

	run(onBatch: BatchHandler, onOutOfRange: (tp: TopicPartition, offset: bigint) => void): Promise<void> {
		if (this.loop) {
			throw new Error('the fake restore consumer runs only once')
		}
		this.calls.push('run')
		this.loop = this.fetchLoop(onBatch, onOutOfRange)
		return this.loop
	}

	pause(tps: readonly TopicPartition[]): void {
		for (const tp of tps) {
			this.calls.push(`pause ${tpKey(tp)}`)
			this.paused.add(tpKey(tp))
		}
	}

	position(tp: TopicPartition): bigint | undefined {
		return this.assigned.get(tpKey(tp))?.position
	}

	/** The run ends after the batch in flight; then the step's outcome. */
	async stop(options: { signal: AbortSignal }): Promise<void> {
		this.calls.push('stop')
		this.stopping = true
		this.poke()
		await raceSignal(this.loop?.catch(() => {}) ?? Promise.resolve(), options.signal)
		await this.steps.stop.perform(() => {}, options.signal)
	}

	/** The run fails with `error` (after the batch in flight), as a broker failure the client did not ride out. */
	fail(error: unknown): void {
		this.failure = { error }
		this.poke()
	}

	private async fetchLoop(
		onBatch: BatchHandler,
		onOutOfRange: (tp: TopicPartition, offset: bigint) => void
	): Promise<void> {
		for (;;) {
			if (this.failure) throw this.failure.error
			if (this.stopping) return
			const batch = this.fetch(onOutOfRange)
			if (batch) await onBatch(batch.tp, batch.messages)
			else await Promise.race([this.cluster.changed(), this.wake.promise])
		}
	}

	private fetch(
		onOutOfRange: (tp: TopicPartition, offset: bigint) => void
	): { tp: TopicPartition; messages: Message<Buffer, Buffer>[] } | undefined {
		const partitions = [...this.assigned.values()]
		for (let i = 0; i < partitions.length; i++) {
			const index = (this.turn + i) % partitions.length
			const assigned = partitions[index]!
			const key = tpKey(assigned.tp)
			if (this.paused.has(key)) continue
			const log = this.cluster.log(assigned.tp.topic, assigned.tp.partition)
			if (assigned.position < log.start || assigned.position > log.end) {
				this.paused.add(key)
				onOutOfRange(assigned.tp, assigned.position)
				continue
			}
			const { messages, next } = log.read(assigned.position, 'read_committed', this.batchSize)
			assigned.position = next
			if (messages.length > 0) {
				this.turn = index + 1
				return { tp: assigned.tp, messages }
			}
		}
		return undefined
	}

	private poke(): void {
		const wake = this.wake
		this.wake = deferred()
		wake.resolve()
	}
}
