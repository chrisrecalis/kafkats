/**
 * The TestDriver's global stores (4.14, 6.19): a global topic's records are applied one by one through the global
 * store's processor (its KTableSource) into `<applicationId>.<store>`; tasks read the store through a read-only view.
 */
import type { Logger, Message } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { GlobalStore } from '@/runtime/global-store.js'
import type { Clock } from '@/runtime/io.js'
import type { StateStore } from '@/state.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { GlobalStoreDef } from '@/topology/nodes.js'

export interface GlobalStoresDeps {
	readonly settings: ResolvedFlowConfig
	readonly stateDirectory: StateDirectory
	readonly clock: Clock
	readonly logger: Logger
}

export class GlobalStores {
	private constructor(
		private readonly byTopic: ReadonlyMap<string, GlobalStore>,
		private readonly byName: ReadonlyMap<string, GlobalStore>
	) {}

	/** Opens every global store and initializes its processor. */
	static async create(defs: readonly GlobalStoreDef[], deps: GlobalStoresDeps): Promise<GlobalStores> {
		const { settings, ...rest } = deps
		const stores = await Promise.all(defs.map(def => GlobalStore.open(def, { config: settings, ...rest })))
		return new GlobalStores(
			new Map(stores.map(store => [store.def.topic, store])),
			new Map(stores.map(store => [store.def.store.name, store]))
		)
	}

	has(topic: string): boolean {
		return this.byTopic.has(topic)
	}

	/** The store of a global table by name (what tests read). */
	store(name: string): StateStore | undefined {
		return this.byName.get(name)?.store
	}

	/** What processors of tasks get: reads only. */
	view(name: string): StateStore {
		const store = this.byName.get(name)
		if (store === undefined) {
			throw new IllegalStateError(`no global store named '${name}'`)
		}
		return store.view
	}

	/** Applies one record of a global topic (GlobalStore.apply). */
	apply(message: Message<Buffer, Buffer>): Promise<void> {
		return this.byTopic.get(message.topic)!.apply([message])
	}

	async close(): Promise<void> {
		for (const store of this.byName.values()) {
			await store.close()
		}
	}
}
