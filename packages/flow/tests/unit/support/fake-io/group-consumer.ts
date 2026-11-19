import type { ConsumerGroupMetadata, Message, TopicPartition } from '@kafkats/client'
import type {
	AssignedPartition,
	CommittedOffset,
	GroupConsumerHooks,
	GroupConsumerPort,
	OffsetAndMetadata,
} from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import { FakeCluster } from './cluster.js'
import { message, type TestRecord } from './records.js'
import { deferred, Steps, type Deferred } from './steps.js'

/** The events of the group consumer port and what their listeners receive. */
interface ConsumerEvents {
	rebalancePending: []
	rebalanceComplete: []
	offsetOutOfRange: [{ topic: string; partition: number; offset: bigint }]
	offsetReset: [
		{ topic: string; partition: number; from: bigint | null; to: bigint; reason: 'no-commit' | 'out-of-range' },
	]
	error: [Error]
}

/** The consumer's view of one partition it has. */
export interface FakePartition extends TopicPartition {
	/** Fetch position: after the last record fetched, or where the assignment or a seek put it. */
	position: bigint
	/** The partition's end as of the last fetch (LSO under read_committed, else HW); unknown before the first. */
	end: bigint | undefined
	paused: boolean
	/** Assigned while the assignment hook runs (C4): not fetched, like a paused partition. */
	held: boolean
	/** Fetched batches not handed to the handler yet (the partition was paused or held, C5). */
	readonly pending: Message<Buffer, Buffer>[][]
}

type BatchHandler = (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => void

/**
 * A group member as the runtime sees it (3.6), driven by the test: `rebalance()` is an eager rebalance in the
 * client's order of hooks and signals (C4), `lose()` a session loss, `deliver()` a fetch response, `fail()` a run
 * that fails. Commits go to the group's offsets in the cluster, so a later member (the next Run's consumer) is
 * assigned them. Pause, resume and seek of a partition it does not have add it (as a manual assignment would).
 */
export class FakeGroupConsumer implements GroupConsumerPort {
	readonly cluster: FakeCluster
	readonly groupId: string
	readonly memberId: string
	/** Every port call in order: 'run a,b', 'pause a:0', 'resume a:0', 'seek a:0 5', 'commit a:0@5', 'committed a:0', 'stop'. */
	readonly calls: string[] = []
	readonly steps = { commit: new Steps(), committed: new Steps(), stop: new Steps() }
	/** The offsets of every commit that succeeded, in order. */
	readonly commits: OffsetAndMetadata[][] = []
	/** The partitions the consumer has, by `topic:partition`. */
	readonly partitions = new Map<string, FakePartition>()
	/** What groupMetadata() reports (C3): set by each join, undefined from a loss on. Tests may set it. */
	group: ConsumerGroupMetadata | undefined
	private generation = 0
	private pending = false
	/** A rebalance was asked for since the last join: the step in progress does not serve it. */
	private requested = false
	private handler: BatchHandler | undefined
	private hooks: GroupConsumerHooks | undefined
	/** Settles the promise run() returned. */
	private outcome: Deferred<void> | undefined
	private ended = false
	private stopping = false
	/** The rebalance steps run one after the other; this one settles when the last queued finished. */
	private step: Promise<void> = Promise.resolve()
	private queuedSteps = 0
	/** Loss hooks invoked since the last join: the next join waits for them. */
	private readonly losses: Promise<void>[] = []
	private readonly listeners = new Map<string, ((...args: never[]) => void)[]>()

	constructor(options: { cluster?: FakeCluster; groupId?: string; memberId?: string } = {}) {
		this.cluster = options.cluster ?? new FakeCluster()
		this.groupId = options.groupId ?? 'app'
		this.memberId = options.memberId ?? 'member'
	}

	// ------------------------------------------------------------------------------------------------------
	// The port
	// ------------------------------------------------------------------------------------------------------

	run(topics: readonly string[], onBatch: BatchHandler, hooks: GroupConsumerHooks): Promise<void> {
		if (this.outcome) {
			throw new Error('the fake consumer runs only once')
		}
		this.calls.push(`run ${topics.join(',')}`)
		this.handler = onBatch
		this.hooks = hooks
		this.outcome = deferred()
		for (const key of this.partitions.keys()) this.flush(key)
		return this.outcome.promise
	}

	/** Waits for the rebalance step in progress (its hook), like the client's run loop (4.3 deadlock rule). */
	async stop(options: { signal: AbortSignal }): Promise<void> {
		this.calls.push('stop')
		this.stopping = true
		await raceSignal(this.step, options.signal)
		await this.steps.stop.perform(() => this.end(), options.signal)
	}

	pause(tps: readonly TopicPartition[]): void {
		for (const tp of tps) {
			this.calls.push(`pause ${tpKey(tp)}`)
			this.partition(tp).paused = true
		}
	}

	/** Hands the partitions' waiting batches to the handler before it returns (unless held). */
	resume(tps: readonly TopicPartition[]): void {
		for (const tp of tps) {
			this.calls.push(`resume ${tpKey(tp)}`)
			this.partition(tp).paused = false
			this.flush(tpKey(tp))
		}
	}

	/** Drops the waiting batches of tp; the next fetch starts at `offset` (C5). */
	seek(tp: TopicPartition, offset: bigint): void {
		this.calls.push(`seek ${tpKey(tp)} ${offset}`)
		const partition = this.partition(tp)
		partition.position = offset
		partition.pending.length = 0
	}

	/** The first offset of the earliest batch not handed to the handler, else the fetch position (C6). */
	nextOffset(tp: TopicPartition): bigint | undefined {
		const partition = this.partitions.get(tpKey(tp))
		return partition && (partition.pending[0]?.[0]?.offset ?? partition.position)
	}

	/** `end - nextOffset`, at least 0; undefined before the end is known and while paused or held (C6). */
	currentLag(tp: TopicPartition): bigint | undefined {
		const partition = this.partitions.get(tpKey(tp))
		if (!partition || partition.paused || partition.held || partition.end === undefined) {
			return undefined
		}
		const lag = partition.end - this.nextOffset(tp)!
		return lag > 0n ? lag : 0n
	}

	commit(offsets: readonly OffsetAndMetadata[], options: { signal: AbortSignal }): Promise<void> {
		this.calls.push(`commit ${offsets.map(o => `${tpKey(o)}@${o.offset}`).join(' ')}`)
		return this.steps.commit.perform(() => {
			this.commits.push([...offsets])
			const committed = this.cluster.offsets(this.groupId)
			for (const { topic, partition, offset, metadata } of offsets) {
				committed.set(tpKey({ topic, partition }), { offset, metadata })
			}
		}, options.signal)
	}

	committed(
		tps: readonly TopicPartition[],
		options: { signal: AbortSignal }
	): Promise<Map<string, CommittedOffset | null>> {
		this.calls.push(`committed ${tps.map(tpKey).join(' ')}`)
		return this.steps.committed.perform(() => {
			const committed = this.cluster.offsets(this.groupId)
			return new Map(tps.map(tp => [tpKey(tp), committed.get(tpKey(tp)) ?? null]))
		}, options.signal)
	}

	groupMetadata(): ConsumerGroupMetadata | undefined {
		return this.group
	}

	/** From a rebalance request (`requestRebalance`, `lose`, a rebalance after the first join) until it completed. */
	get rebalancePending(): boolean {
		return this.pending
	}

	on(event: string, listener: (...args: never[]) => void): void {
		const listeners = this.listeners.get(event) ?? []
		listeners.push(listener)
		this.listeners.set(event, listeners)
	}

	// ------------------------------------------------------------------------------------------------------
	// What the test drives
	// ------------------------------------------------------------------------------------------------------

	/** The partition's state; a partition the consumer does not have is added (at position 0, end unknown). */
	partition(tp: TopicPartition | string): FakePartition {
		const { topic, partition } = typeof tp === 'string' ? parseKey(tp) : tp
		const key = tpKey({ topic, partition })
		let state = this.partitions.get(key)
		if (!state) {
			state = fakePartition({ topic, partition }, 0n, false)
			this.partitions.set(key, state)
		}
		return state
	}

	/**
	 * A fetch response for tp: `records` become one batch at the fetch position (dense offsets unless a record has
	 * one), handed to the handler at once unless tp is paused or held (then it waits, C5), and the end moves to
	 * `end` (default: at least the position after the batch). Without records only the end moves. Returns the batch.
	 */
	deliver(tp: TopicPartition | string, records: readonly TestRecord[] = [], end?: bigint): Message<Buffer, Buffer>[] {
		const partition = this.partition(tp)
		let next = partition.position
		const batch = records.map(record => {
			const offset = record.offset ?? next
			next = offset + 1n
			return message(partition.topic, partition.partition, offset, record)
		})
		partition.position = next
		partition.end = end ?? (partition.end !== undefined && partition.end > next ? partition.end : next)
		if (batch.length > 0) {
			partition.pending.push(batch)
			this.flush(tpKey(partition))
		}
		return batch
	}

	/** The group asks for a rebalance (a heartbeat said so): `rebalancePending` until a rebalance completed it. */
	requestRebalance(): void {
		this.requested = true
		if (!this.pending) {
			this.pending = true
			this.emit('rebalancePending')
		}
	}

	/**
	 * An eager rebalance in the client's order (C4): `rebalancePending`; the revocation hook for every partition the
	 * consumer has (awaited, then they are removed); the loss hooks invoked since the last join awaited; a new
	 * generation; the assignment added held, the assignment hook awaited, the partitions released (those paused stay
	 * paused; those lost meanwhile are not released); `rebalanceComplete`. The first join signals nothing and revokes
	 * nothing. A partition given by its key starts at the group's committed offset, else at its log start. Resolves
	 * once the step finished; a hook that rejects fails the run (`run()` rejects), not this promise.
	 */
	rebalance(...assignment: (string | AssignedPartition)[]): Promise<void> {
		if (!this.hooks || this.stopping) {
			throw new Error('rebalance() needs a running consumer')
		}
		if (this.generation > 0) {
			this.requestRebalance()
		}
		this.queuedSteps++
		const step = this.step.then(() => this.rebalanceStep(assignment))
		this.step = step
		return step
	}

	/**
	 * A session loss (C3, C4): the group metadata goes; every partition is removed with its waiting batches; the
	 * loss hook is invoked synchronously (its synchronous part ran when this returns); then the group asks for a
	 * rebalance. The next rebalance awaits the hook before it joins and revokes none of these partitions. Returns the
	 * hook's promise.
	 */
	lose(): Promise<void> {
		const lost = this.owned()
		this.group = undefined
		this.partitions.clear()
		let handled = Promise.resolve()
		if (lost.length > 0 && this.hooks) {
			handled = this.hooks.onPartitionsLost(lost)
			handled.catch(() => {})
			this.losses.push(handled)
		}
		this.requestRebalance()
		return handled
	}

	/** The run fails (FencedInstanceId, NoOffsetForPartitionError, ...): `run()` rejects with `error`. */
	fail(error: unknown): void {
		this.end(error)
	}

	/** Emits an event to the listeners registered with `on()`. */
	emit<E extends keyof ConsumerEvents>(event: E, ...args: ConsumerEvents[E]): void {
		for (const listener of this.listeners.get(event) ?? []) {
			;(listener as (...payload: ConsumerEvents[E]) => void)(...args)
		}
	}

	// ------------------------------------------------------------------------------------------------------

	private async rebalanceStep(assignment: readonly (string | AssignedPartition)[]): Promise<void> {
		const hooks = this.hooks!
		try {
			if (this.ended) return
			const revoked = this.owned()
			if (revoked.length > 0) {
				await hooks.onPartitionsRevoked(revoked)
				for (const tp of revoked) this.partitions.delete(tpKey(tp))
			}
			await Promise.all(this.losses.splice(0))
			this.generation++
			this.requested = false
			this.group = { groupId: this.groupId, generationId: this.generation, memberId: this.memberId }
			const assigned = assignment.map(p => (typeof p === 'string' ? this.assigned(p) : p))
			const added = assigned.map(tp => {
				const state = fakePartition(tp, tp.offset, true)
				this.partitions.set(tpKey(state), state)
				return state
			})
			await hooks.onPartitionsAssigned(assigned)
			for (const state of added) {
				if (this.partitions.get(tpKey(state)) !== state) continue
				state.held = false
				this.flush(tpKey(state))
			}
		} catch (error) {
			this.end(error)
		} finally {
			this.queuedSteps--
		}
		if (this.pending && !this.requested && this.queuedSteps === 0 && !this.ended) {
			this.pending = false
			this.emit('rebalanceComplete')
		}
	}

	private owned(): TopicPartition[] {
		return [...this.partitions.values()].map(({ topic, partition }) => ({ topic, partition }))
	}

	/** How the client assigns a partition: from the group's commit, else from the log start ('earliest'). */
	private assigned(key: string): AssignedPartition {
		const { topic, partition } = parseKey(key)
		const committed = this.cluster.offsets(this.groupId).get(key) ?? null
		const start = this.cluster.topics.get(topic)?.partitions[partition]?.start ?? 0n
		return { topic, partition, offset: committed?.offset ?? start, committed }
	}

	private flush(key: string): void {
		const partition = this.partitions.get(key)
		while (partition && !partition.paused && !partition.held && this.handler && !this.stopping && !this.ended) {
			const batch = partition.pending.shift()
			if (!batch) return
			this.handler({ topic: partition.topic, partition: partition.partition }, batch)
		}
	}

	private end(error?: unknown): void {
		if (this.ended) return
		this.ended = true
		if (error === undefined) this.outcome?.resolve()
		else this.outcome?.reject(error)
	}
}

function fakePartition({ topic, partition }: TopicPartition, position: bigint, held: boolean): FakePartition {
	return { topic, partition, position, end: undefined, paused: false, held, pending: [] }
}

function parseKey(key: string): TopicPartition {
	const at = key.lastIndexOf(':')
	return { topic: key.slice(0, at), partition: Number(key.slice(at + 1)) }
}
