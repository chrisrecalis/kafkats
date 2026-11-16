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
	/** Set while the instance is pruned: a lease waits for it and then gets a new instance. */
	pruning?: Promise<void>
}

const instanceId = (store: string, partition: number): string => `${store}\u0000${partition}`

/**
 * The provider stores of one start(), across task lifetimes and recoveries (Kafka Streams' state directory).
 *
 * A store instance is lent to one task at a time; it stays open when the task closes, so that a task coming back
 * (every eager rebalance revokes before it assigns) finds its state and catches up from its checkpoint instead of
 * rebuilding it. The provider owns the stores: none is closed before the provider at close().
 */
export class StateDirectory {
	private readonly instances = new Map<string, Instance>()
	private readonly leases = new Map<string, TaskId>()
	private readonly globals = new Map<string, Opened>()

	constructor(
		private readonly applicationId: string,
		private readonly provider: StateStoreProvider,
		private readonly checkpoints: Checkpoints,
		private readonly logger: Logger
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
			await this.instances.get(id)?.pruning
			const instance = this.instances.get(id) ?? this.create(id, builder, partition)
			await this.init(instance)
			return instance.store
		} catch (error) {
			this.leases.delete(id)
			throw error
		}
	}

	/**
	 * Ends `owner`'s lease. `wipe`: first invalidates the store's changelog checkpoint, then clears the store, so that
	 * its state is rebuilt from the changelog by the next owner.
	 */
	async release(owner: TaskId, store: string, partition: number, options?: { wipe?: boolean }): Promise<void> {
		const id = instanceId(store, partition)
		const instance = this.instances.get(id)
		if (instance === undefined || this.leases.get(id) !== owner) {
			throw new IllegalStateError(
				`Task ${owner.key} holds no lease of state store '${store}' of partition ${partition}`
			)
		}
		try {
			if (options?.wipe) await this.wipe(instance)
		} finally {
			this.leases.delete(id)
		}
	}

	/**
	 * The instances no task holds, if checkpoints are kept in process. Durable state is kept, like Kafka Streams' task
	 * directories.
	 */
	unleased(): readonly { store: string; partition: number }[] {
		if (this.checkpoints.durable) return []
		return [...this.instances.values()]
			.filter(instance => !this.leases.has(instance.id))
			.map(({ builder, partition }) => ({ store: builder.name, partition }))
	}

	/**
	 * Drops an unleased in-memory instance (another instance of the application owns its state by now): invalidates its
	 * checkpoint, clears it and forgets it. Does nothing if a task leased it meanwhile; a lease made while it runs waits
	 * for it and gets a new, empty instance.
	 */
	async prune(store: string, partition: number, signal: AbortSignal): Promise<void> {
		signal.throwIfAborted()
		const id = instanceId(store, partition)
		const instance = this.instances.get(id)
		if (instance === undefined || instance.pruning !== undefined || this.leases.has(id)) return
		const pruned = this.wipe(instance).then(() => {
			this.instances.delete(id)
		})
		// A failed prune leaves the instance with an invalidated checkpoint: its next owner rebuilds it.
		instance.pruning = pruned.catch(() => {})
		try {
			await pruned
		} finally {
			instance.pruning = undefined
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
		}
		this.instances.set(id, instance)
		return instance
	}

	private init(opened: Opened): Promise<void> {
		opened.ready ??= opened.store.init().catch((error: unknown) => {
			opened.ready = undefined
			throw error
		})
		return opened.ready
	}

	private async wipe(instance: Instance): Promise<void> {
		const { logging } = instance.builder
		if (logging.enabled) await this.checkpoints.invalidate(logging.topic, instance.partition)
		await clearStore(instance.store as KeyValueStore<unknown, unknown>)
	}
}
