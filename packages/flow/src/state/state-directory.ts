import type { Logger } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { TaskId } from '@/processor/api.js'
import { abortError } from '@/runtime/signals.js'
import type { KeyValueStore, StateStore, StateStoreProvider } from '@/state.js'
import { clearStore } from '@/state/bulk.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/** A provider store, initialized once. */
interface Opened {
	readonly store: StateStore
	/** The store's init(); unset until first needed and again after a failure, so that the next use retries it. */
	ready?: Promise<void>
}

/** The provider store of one store of the topology for one partition. */
interface Instance extends Opened {
	readonly id: string
	readonly builder: StoreBuilder
	readonly partition: number
	/** When its last lease ended (its creation before that): how long no task of this start owned it. */
	idleSince: number
	/** A clear began and did not finish (a lease or close stopped it, or it failed): the store must not be used as is. */
	partlyCleared: boolean
}

/** A store partition of the topology whose durable state an earlier start may have left behind. */
interface Unopened {
	readonly builder: StoreBuilder
	readonly partition: number
}

/** A wipe in progress: interruptible, so that a lease or close never waits for a whole store to be cleared. */
interface Wipe {
	readonly abort: AbortController
	/** Settles once the wipe stopped; never rejects. */
	readonly settled: Promise<void>
}

/** An instance no task holds, and when it may be dropped. */
export interface UnleasedInstance {
	readonly store: string
	readonly partition: number
	/** In-memory state: when its last lease ended; durable state: that time plus the cleanup delay. */
	readonly prunableAt: number
}

/** How long durable state no task owns is kept, and the clock that measures it (the runtime's). */
export interface StateCleanup {
	/** FlowConfig.stateCleanupDelayMs (Kafka Streams' `state.cleanup.delay.ms`). */
	readonly delayMs: number
	readonly now: () => number
}

/** Kafka Streams' default `state.cleanup.delay.ms`. */
export const DEFAULT_STATE_CLEANUP_DELAY_MS = 600_000

const instanceId = (store: string, partition: number): string => `${store}\u0000${partition}`

/**
 * The provider stores of one start(), across task lifetimes and recoveries (Kafka Streams' state directory).
 *
 * A store instance is lent to one task at a time; it stays open when the task closes, so that a task coming back
 * (every eager rebalance revokes before it assigns) finds its state and catches up from its checkpoint instead of
 * rebuilding it. The provider owns the stores: none is closed before the provider at close().
 *
 * State no task owns is dropped after a while (prune(), timed by the run's PruneTimer): in-memory state soon, durable
 * state once no task held it for the cleanup delay, like Kafka Streams' state directory cleaner deletes the task
 * directories nobody locked for `state.cleanup.delay.ms`, whatever stores they hold. A checkpoint of state kept longer
 * than that could be trusted although the changelog was compacted meanwhile: a replay from it misses the tombstones
 * compaction removed; and the state of a store nothing restores (no changelog, or `skipRestoration`) would be used as
 * found.
 *
 * Clearing a store takes a delete per entry, seconds for millions of them, so nothing waits for it: a lease made while
 * a prune clears the instance stops the clear and gets it partly cleared, with its checkpoint invalidated; the task's
 * restore pipeline finishes the clear (finishClears(), outside the scheduler's exclusive section) before the restore
 * rebuilds it. close() stops the clears before it closes the provider. What survives a clear that a close or a crash
 * stopped is durable: the invalidated checkpoint of a logged store has its next restore rebuild it, and the clear of a
 * store nothing restores is marked in progress in the checkpoint store until it is done, so that the next start's lease
 * gets it partly cleared and its owner finishes the clear.
 */
export class StateDirectory {
	private readonly instances = new Map<string, Instance>()
	private readonly leases = new Map<string, TaskId>()
	private readonly globals = new Map<string, Opened>()
	/** Durable store partitions of the topology that no lease of this start opened yet (register()). */
	private readonly unopened = new Map<string, Unopened>()
	private registeredAt = 0
	/** Wipes in progress per instance: a prune's (unleased instances) or its owner's (finishClears). */
	private readonly wipes = new Map<string, Wipe>()
	/** Callers of released() waiting for the last lease to end. */
	private waiters: (() => void)[] = []

	constructor(
		private readonly applicationId: string,
		private readonly provider: StateStoreProvider,
		private readonly checkpoints: Checkpoints,
		private readonly logger: Logger,
		private readonly cleanup: StateCleanup = { delayMs: DEFAULT_STATE_CLEANUP_DELAY_MS, now: Date.now }
	) {}

	/**
	 * Name of a store's partition in the provider. Prefixed with the application id, so that applications sharing a
	 * provider (one LMDB directory) never read or wipe each other's state.
	 */
	providerName(store: string, partition: number): string {
		return `${this.applicationId}.${store}.p${partition}`
	}

	/**
	 * Lends the provider store of (store, partition) to `owner`, creating it and awaiting its init() on first use. An
	 * init() that failed is retried by the next lease. Rejects with IllegalStateError while another task holds it. A
	 * prune in progress is stopped (after its write in flight): the owner's finishClears() completes its clear.
	 */
	async lease(owner: TaskId, builder: StoreBuilder, partition: number): Promise<StateStore> {
		const id = instanceId(builder.name, partition)
		const holder = this.leases.get(id)
		if (holder !== undefined) {
			throw new IllegalStateError(
				`State store '${builder.name}' of partition ${partition} is already leased by task ${holder.key}`
			)
		}
		this.leases.set(id, owner)
		try {
			await this.stopWipe(id)
			this.unopened.delete(id)
			const instance = this.instances.get(id) ?? (await this.reopen(id, builder, partition))
			await this.init(instance)
			return instance.store
		} catch (error) {
			this.endLease(id)
			throw error
		}
	}

	/**
	 * Ends `owner`'s lease, also when `invalidate` fails. `invalidate`: first invalidates the store's changelog
	 * checkpoint, so that the next owner's restore clears the store and rebuilds it from the changelog (a write and a
	 * flush here: the clear is the restore's, which a stop interrupts).
	 */
	async release(owner: TaskId, store: string, partition: number, options?: { invalidate?: boolean }): Promise<void> {
		const id = instanceId(store, partition)
		const instance = this.instances.get(id)
		if (instance === undefined || this.leases.get(id) !== owner) {
			throw new IllegalStateError(
				`Task ${owner.key} holds no lease of state store '${store}' of partition ${partition}`
			)
		}
		try {
			if (options?.invalidate) await this.invalidate(instance)
		} finally {
			this.endLease(id)
		}
	}

	/**
	 * Resolves once no store is leased: every task that held one released it (a run that ended without waiting for
	 * its task closes, 4.10.2).
	 */
	released(): Promise<void> {
		return this.leases.size === 0 ? Promise.resolve() : new Promise(resolve => this.waiters.push(resolve))
	}

	/**
	 * The store partitions of the topology (each store with the partition count of its tasks), once the counts are
	 * known: with durable checkpoints, the state an earlier start left of those no task of this start leases is dropped
	 * like that of an instance released at this call (unleased()).
	 */
	register(stores: Iterable<{ readonly builder: StoreBuilder; readonly partitions: number }>): void {
		if (!this.checkpoints.durable) return
		this.registeredAt = this.cleanup.now()
		for (const { builder, partitions } of stores) {
			for (let partition = 0; partition < partitions; partition++) {
				const id = instanceId(builder.name, partition)
				if (!this.instances.has(id)) this.unopened.set(id, { builder, partition })
			}
		}
	}

	/**
	 * The instances no task holds and no prune is dropping, with the time from which they may be dropped: in-memory
	 * state from its release, durable state once no task held it for the cleanup delay (registered partitions no lease
	 * opened count from their registration).
	 */
	unleased(): readonly UnleasedInstance[] {
		const delay = this.checkpoints.durable ? this.cleanup.delayMs : 0
		const unleased: UnleasedInstance[] = []
		for (const instance of this.instances.values()) {
			if (this.leases.has(instance.id) || this.wipes.has(instance.id)) continue
			unleased.push({
				store: instance.builder.name,
				partition: instance.partition,
				prunableAt: instance.idleSince + delay,
			})
		}
		for (const [id, { builder, partition }] of this.unopened) {
			if (this.leases.has(id) || this.wipes.has(id)) continue
			unleased.push({ store: builder.name, partition, prunableAt: this.registeredAt + delay })
		}
		return unleased
	}

	/**
	 * Drops the state of an unleased instance (another instance of the application owns it by now): invalidates its
	 * checkpoint, clears the store and forgets it. Of a registered partition no lease opened, only invalidates the
	 * checkpoint when the restore of its next lease rebuilds it (opening the store just to clear it would cost a provider
	 * database); the state of a store nothing restores is opened and cleared. Does nothing if a task leased it meanwhile.
	 * Resolves when a lease or close stopped it (the instance stays partly cleared); rejects when it failed (the
	 * instance stays, to be pruned again or cleared by its next owner).
	 */
	async prune(store: string, partition: number, signal: AbortSignal): Promise<void> {
		signal.throwIfAborted()
		const id = instanceId(store, partition)
		if (this.leases.has(id) || this.wipes.has(id)) return
		const unopened = this.unopened.get(id)
		if (unopened !== undefined && rebuiltByRestore(unopened.builder)) {
			await this.track(id, () => this.invalidateUnopened(unopened))
			this.unopened.delete(id)
			return
		}
		let instance = this.instances.get(id)
		if (instance === undefined) {
			if (unopened === undefined) return
			this.unopened.delete(id)
			instance = this.create(id, unopened.builder, partition, this.registeredAt)
		}
		const opened = instance
		if (await this.track(id, stop => this.wipe(opened, stop))) this.instances.delete(id)
	}

	/**
	 * Finishes the clears of the stores `owner` leased partly cleared (their prune was stopped by the lease, or failed):
	 * the checkpoint invalidated again, the store cleared. The task's restore pipeline runs it before the restore,
	 * outside the scheduler's exclusive section; rejects with an AbortError once `signal` aborts or close() stops it
	 * (after the write in flight).
	 */
	async finishClears(owner: TaskId, signal: AbortSignal): Promise<void> {
		const partly = [...this.leases]
			.filter(([id, holder]) => holder === owner && this.instances.get(id)?.partlyCleared === true)
			.map(([id]) => this.instances.get(id)!)
		for (const instance of partly) {
			const finished = await this.track(instance.id, stop => this.wipe(instance, AbortSignal.any([stop, signal])))
			if (!finished) throw abortError(signal)
		}
	}

	/**
	 * The store of a global table (`<applicationId>.<store>`, one for all partitions), created and initialized on first
	 * use. Not leased: the global state manager is its only writer.
	 */
	async globalStore(builder: StoreBuilder): Promise<StateStore> {
		let opened = this.globals.get(builder.name)
		if (opened === undefined) {
			opened = { store: builder.create(this.provider, `${this.applicationId}.${builder.name}`) }
			this.globals.set(builder.name, opened)
		}
		await this.init(opened)
		return opened.store
	}

	/**
	 * Stops the wipes in progress (after their writes in flight), forgets every instance and lease and closes the
	 * provider (a failure is logged). The next start re-creates them. A store left partly cleared is finished by its next
	 * owner: a logged store's checkpoint is invalidated, so its restore rebuilds it; a store nothing restores has its
	 * clear marked in progress, so its next lease gets it partly cleared (or a prune of the next start clears it).
	 */
	async close(): Promise<void> {
		const wipes = [...this.wipes.values()]
		for (const wipe of wipes) wipe.abort.abort()
		await Promise.all(wipes.map(wipe => wipe.settled))
		this.instances.clear()
		this.leases.clear()
		this.globals.clear()
		this.unopened.clear()
		this.notifyReleased()
		try {
			await this.provider.close()
		} catch (error) {
			this.logger.error('closing the state store provider failed', {
				error: error instanceof Error ? error.message : String(error),
			})
		}
	}

	/**
	 * The instance of a store partition this start has not opened yet: partly cleared when a clear an earlier start began
	 * did not finish (its marker, for a store nothing restores; a logged one's invalidated checkpoint has its restore
	 * rebuild it).
	 */
	private async reopen(id: string, builder: StoreBuilder, partition: number): Promise<Instance> {
		const partlyCleared =
			marksClears(builder, this.checkpoints) &&
			(await this.checkpoints.clearing(...this.marker(builder, partition)))
		const instance = this.create(id, builder, partition)
		instance.partlyCleared = partlyCleared
		return instance
	}

	private create(id: string, builder: StoreBuilder, partition: number, idleSince = this.cleanup.now()): Instance {
		const instance: Instance = {
			id,
			builder,
			partition,
			store: builder.create(this.provider, this.providerName(builder.name, partition)),
			idleSince,
			partlyCleared: false,
		}
		this.instances.set(id, instance)
		return instance
	}

	private endLease(id: string): void {
		this.leases.delete(id)
		const instance = this.instances.get(id)
		if (instance !== undefined) instance.idleSince = this.cleanup.now()
		this.notifyReleased()
	}

	private notifyReleased(): void {
		if (this.leases.size > 0) return
		const waiters = this.waiters
		this.waiters = []
		for (const resolve of waiters) resolve()
	}

	private init(opened: Opened): Promise<void> {
		opened.ready ??= opened.store.init().catch((error: unknown) => {
			opened.ready = undefined
			throw error
		})
		return opened.ready
	}

	private async invalidate(instance: Instance): Promise<void> {
		const { logging } = instance.builder
		if (logging.enabled) await this.checkpoints.invalidate(logging.topic, instance.partition)
	}

	/**
	 * Runs `fn` as the wipe of `id`: a lease and close stop it with its signal and wait for it to settle. Resolves to
	 * whether it ran to its end, false when stopped; rejects with its failure.
	 */
	private async track(id: string, fn: (signal: AbortSignal) => Promise<void>): Promise<boolean> {
		const abort = new AbortController()
		const done = fn(abort.signal)
		this.wipes.set(id, { abort, settled: done.catch(() => {}) })
		try {
			await done
			return true
		} catch (error) {
			if (abort.signal.aborted) return false
			throw error
		} finally {
			this.wipes.delete(id)
		}
	}

	/** Stops the wipe of `id` in progress, if any, and waits until it settled. */
	private async stopWipe(id: string): Promise<void> {
		const wipe = this.wipes.get(id)
		wipe?.abort.abort()
		await wipe?.settled
	}

	/**
	 * Invalidates the checkpoint (and marks the clear of a store nothing restores in progress), then clears the store
	 * until `signal` aborts: partly cleared until it is done.
	 */
	private async wipe(instance: Instance, signal: AbortSignal): Promise<void> {
		const marked = marksClears(instance.builder, this.checkpoints)
		const marker = this.marker(instance.builder, instance.partition)
		instance.partlyCleared = true
		await this.invalidate(instance)
		if (marked) await this.checkpoints.beginClear(...marker)
		await this.init(instance)
		await clearStore(instance.store as KeyValueStore<unknown, unknown>, signal)
		if (marked) await this.checkpoints.endClear(...marker)
		instance.partlyCleared = false
	}

	/** Where the checkpoints mark a clear of the store's partition: `<applicationId>.<store>` and the partition. */
	private marker(builder: StoreBuilder, partition: number): [store: string, partition: number] {
		return [`${this.applicationId}.${builder.name}`, partition]
	}

	/** Invalidates the checkpoint an earlier start left, if it would be trusted (a write and a flush otherwise saved). */
	private async invalidateUnopened({ builder, partition }: Unopened): Promise<void> {
		const { logging } = builder
		if (!logging.enabled) return
		const checkpoint = await this.checkpoints.get(logging.topic, partition)
		if (checkpoint !== undefined && checkpoint >= 0n) await this.checkpoints.invalidate(logging.topic, partition)
	}
}

/** Whether the restore of a lease rebuilds the store from its changelog when its checkpoint is invalid. */
function rebuiltByRestore(builder: StoreBuilder): boolean {
	return builder.logging.enabled && !builder.logging.skipRestoration
}

/**
 * Whether a clear of the store is marked in the checkpoints: only a store nothing restores needs it (an invalidated
 * checkpoint has the restore rebuild the others), and only a durable mark outlives the start that began the clear.
 */
function marksClears(builder: StoreBuilder, checkpoints: Checkpoints): boolean {
	return checkpoints.durable && !rebuiltByRestore(builder)
}
