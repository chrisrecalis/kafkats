import type { Logger } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { TaskId } from '@/processor/api.js'
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
}

/** A store partition of the topology whose durable state an earlier start may have left behind. */
interface Unopened {
	readonly builder: StoreBuilder
	readonly partition: number
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
 * directories nobody locked for `state.cleanup.delay.ms`. A checkpoint of state kept longer than that could be
 * trusted although the changelog was compacted meanwhile: a replay from it misses the tombstones compaction removed.
 */
export class StateDirectory {
	private readonly instances = new Map<string, Instance>()
	private readonly leases = new Map<string, TaskId>()
	private readonly globals = new Map<string, Opened>()
	/** Durable store partitions of the topology that no lease of this start opened yet (register()). */
	private readonly unopened = new Map<string, Unopened>()
	private registeredAt = 0
	/** Prunes in progress: a lease waits for the instance's and then gets a new one. */
	private readonly pruning = new Map<string, Promise<void>>()
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
	 * init() that failed is retried by the next lease. Rejects with IllegalStateError while another task holds it.
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
			await this.pruning.get(id)
			this.unopened.delete(id)
			const instance = this.instances.get(id) ?? this.create(id, builder, partition)
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
	 * The store partitions of the topology (each logged store with its partition count), once the counts are known: with
	 * durable checkpoints, the state an earlier start left of those no task of this start leases is dropped like that of
	 * an instance released at this call (unleased()). State without a changelog has no checkpoint to invalidate.
	 */
	register(stores: Iterable<{ readonly builder: StoreBuilder; readonly partitions: number }>): void {
		if (!this.checkpoints.durable) return
		this.registeredAt = this.cleanup.now()
		for (const { builder, partitions } of stores) {
			if (!builder.logging.enabled) continue
			for (let partition = 0; partition < partitions; partition++) {
				const id = instanceId(builder.name, partition)
				if (!this.instances.has(id)) this.unopened.set(id, { builder, partition })
			}
		}
	}

	/**
	 * The instances no task holds, with the time from which they may be dropped: in-memory state from its release,
	 * durable state once no task held it for the cleanup delay (registered partitions no lease opened count from their
	 * registration).
	 */
	unleased(): readonly UnleasedInstance[] {
		const delay = this.checkpoints.durable ? this.cleanup.delayMs : 0
		const unleased: UnleasedInstance[] = []
		for (const instance of this.instances.values()) {
			if (this.leases.has(instance.id)) continue
			unleased.push({
				store: instance.builder.name,
				partition: instance.partition,
				prunableAt: instance.idleSince + delay,
			})
		}
		for (const [id, { builder, partition }] of this.unopened) {
			if (this.leases.has(id)) continue
			unleased.push({ store: builder.name, partition, prunableAt: this.registeredAt + delay })
		}
		return unleased
	}

	/**
	 * Drops the state of an unleased instance (another instance of the application owns it by now): invalidates its
	 * checkpoint, clears the store and forgets it; of a registered partition no lease opened, only invalidates its
	 * checkpoint (opening the store just to clear it would cost a provider database), so that the next lease rebuilds
	 * it. Does nothing if a task leased it meanwhile; a lease made while it runs waits for it and gets a new instance.
	 */
	async prune(store: string, partition: number, signal: AbortSignal): Promise<void> {
		signal.throwIfAborted()
		const id = instanceId(store, partition)
		if (this.leases.has(id) || this.pruning.has(id)) return
		const instance = this.instances.get(id)
		const unopened = this.unopened.get(id)
		let pruned: Promise<void>
		if (instance !== undefined) {
			pruned = this.wipe(instance).then(() => {
				this.instances.delete(id)
			})
		} else if (unopened !== undefined) {
			pruned = this.invalidateUnopened(unopened).then(() => {
				this.unopened.delete(id)
			})
		} else {
			return
		}
		// A failed prune leaves the instance with an invalidated checkpoint, or untouched: either way consistent.
		this.pruning.set(
			id,
			pruned.catch(() => {})
		)
		try {
			await pruned
		} finally {
			this.pruning.delete(id)
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

	/** Forgets every instance and lease and closes the provider (a failure is logged). The next start re-creates them. */
	async close(): Promise<void> {
		this.instances.clear()
		this.leases.clear()
		this.globals.clear()
		this.unopened.clear()
		this.pruning.clear()
		this.notifyReleased()
		try {
			await this.provider.close()
		} catch (error) {
			this.logger.error('closing the state store provider failed', {
				error: error instanceof Error ? error.message : String(error),
			})
		}
	}

	private create(id: string, builder: StoreBuilder, partition: number): Instance {
		const instance = {
			id,
			builder,
			partition,
			store: builder.create(this.provider, this.providerName(builder.name, partition)),
			idleSince: this.cleanup.now(),
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

	private async wipe(instance: Instance): Promise<void> {
		await this.invalidate(instance)
		await clearStore(instance.store as KeyValueStore<unknown, unknown>)
	}

	/** Invalidates the checkpoint an earlier start left, if it would be trusted (a write and a flush otherwise saved). */
	private async invalidateUnopened({ builder, partition }: Unopened): Promise<void> {
		const { logging } = builder
		if (!logging.enabled) return
		const checkpoint = await this.checkpoints.get(logging.topic, partition)
		if (checkpoint !== undefined && checkpoint >= 0n) await this.checkpoints.invalidate(logging.topic, partition)
	}
}
