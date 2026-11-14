import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, StateStoreProvider, WindowStore } from '@/state.js'
import {
	type ChangelogConfig,
	type ChangelogTopicSpec,
	ChangelogWriter,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
	windowedKeyCodec,
} from '@/changelog.js'
import {
	ChangelogBackedKeyValueStore,
	ChangelogBackedSessionStore,
	ChangelogBackedWindowStore,
} from '@/state/changelog.js'
import { BufferedKeyValueStore, BufferedSessionStore, BufferedWindowStore } from '@/state/buffered.js'
import {
	PartitionedKeyValueStore,
	PartitionedSessionStore,
	PartitionedWindowStore,
	StorePartitions,
	partitionStoreName,
	type StoreStack,
} from '@/state/partitioned.js'
import type { SessionStoreOptions, StoreFactory, StoreOptions, WindowStoreOptions } from '@/streams/topology-builder.js'
import type { FlowConfig } from '@/types.js'
import { recordContextStorage } from '@/runtime/context.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
import type { GlobalTableDef, Topology } from '@/runtime/topology.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyStore = KeyValueStore<any, any>

type StoreScope = 'partition' | 'app'

type StoreKind = 'key-value' | 'window' | 'session'

/** A logical state store and how its state is laid out. */
export type StoreDef = {
	name: string
	kind: StoreKind
	/**
	 * 'partition': one slice of state per source partition, restored from the changelog partition
	 * of the same index (safe across instances).
	 * 'app': a single app-wide store, for stores without source topics to be co-partitioned with.
	 */
	scope: StoreScope
	partitions: StorePartitions<AnyStore, AnyStore>
	changelog: ChangelogTopicSpec | null
	sourceTopics: Set<string>
}

/** What getOrCreate() needs to know about one kind of store (key-value, window, session). */
type StoreLayout<S extends AnyStore, KK, V> = {
	kind: StoreKind
	options: StoreOptions<unknown, V>
	/** Codec of changelog record keys (the windowed key for window and session stores). */
	changelogKeyCodec: Codec<KK>
	/** Retention of window and session stores; their changelogs expire records after it. */
	retentionMs?: number
	/** Build one partition's stack on top of the provider store called `providerName`. */
	createStack: (providerName: string, writer: ChangelogWriter<KK, V> | null) => StoreStack<S, S>
	createFacade: (partitions: StorePartitions<S, S>) => S
}

/**
 * The application's state stores.
 *
 * Every logical store is a facade over per-partition stacks: the provider's store, a transactional
 * write buffer on top of it, and changelog logging on top of that.
 */
export class StoreRegistry implements StoreFactory {
	/** Every store by name, as handed to processors (global tables' stores included). */
	readonly stateStores = new Map<string, KeyValueStore<unknown, unknown>>()
	private readonly defs = new Map<string, StoreDef>()
	private readonly specs = new Map<string, ChangelogTopicSpec>()
	/** True once the provider has been closed: stores created at build time must be re-created. */
	private globalStoresStale = false

	constructor(
		private readonly config: FlowConfig,
		private readonly provider: StateStoreProvider,
		private readonly topology: Topology,
		private readonly checkpoints: Checkpoints
	) {}

	getOrCreateKeyValueStore<K, V>(name: string, options: StoreOptions<K, V>): KeyValueStore<K, V> {
		const { keyCodec, valueCodec } = options
		return this.getOrCreate(name, {
			kind: 'key-value',
			options,
			changelogKeyCodec: keyCodec,
			createStack: (providerName, writer) => {
				const raw = this.provider.createKeyValueStore<K, V>(providerName, { keyCodec, valueCodec })
				const buffered = new BufferedKeyValueStore<K, V>(raw, keyCodec, valueCodec)
				const store = writer ? new ChangelogBackedKeyValueStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			createFacade: partitions => new PartitionedKeyValueStore<K, V>(partitions),
		})
	}

	getOrCreateWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		const { keyCodec, valueCodec, retentionMs, windowSizeMs } = options
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		return this.getOrCreate(name, {
			kind: 'window',
			options,
			changelogKeyCodec: storeKeyCodec,
			retentionMs,
			createStack: (providerName, writer) => {
				const raw = this.provider.createWindowStore<K, V>(providerName, {
					keyCodec,
					valueCodec,
					retentionMs,
					windowSizeMs,
				})
				const buffered = new BufferedWindowStore<K, V>(raw, storeKeyCodec, valueCodec, keyCodec, retentionMs)
				const store = writer ? new ChangelogBackedWindowStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			createFacade: partitions => new PartitionedWindowStore<K, V>(partitions),
		})
	}

	getOrCreateSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		const { keyCodec, valueCodec, retentionMs } = options
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		return this.getOrCreate(name, {
			kind: 'session',
			options,
			changelogKeyCodec: storeKeyCodec,
			retentionMs,
			createStack: (providerName, writer) => {
				const raw = this.provider.createSessionStore<K, V>(providerName, { keyCodec, valueCodec, retentionMs })
				const buffered = new BufferedSessionStore<K, V>(raw, storeKeyCodec, valueCodec, keyCodec, retentionMs)
				const store = writer ? new ChangelogBackedSessionStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			createFacade: partitions => new PartitionedSessionStore<K, V>(partitions),
		})
	}

	/** Create the store of a global table: a single provider store, without changelog or buffering. */
	createGlobalStore<K, V>(storeName: string, keyCodec: Codec<K>, valueCodec: Codec<V>): KeyValueStore<K, V> {
		const store = this.provider.createKeyValueStore<K, V>(this.providerStoreName(storeName), {
			keyCodec,
			valueCodec,
		})
		this.stateStores.set(storeName, store as KeyValueStore<unknown, unknown>)
		return store
	}

	/** The live store of a global table, re-created if a previous close() closed the provider. */
	openGlobalStore(table: GlobalTableDef): KeyValueStore<unknown, unknown> {
		if (this.globalStoresStale) {
			const store = this.provider.createKeyValueStore(this.providerStoreName(table.storeName), {
				keyCodec: table.storeKeyCodec,
				valueCodec: table.storeValueCodec,
			})
			table.storeRef.store = store
			this.stateStores.set(table.storeName, store)
		}
		return table.storeRef.store!
	}

	get definitions(): Iterable<StoreDef> {
		return this.defs.values()
	}

	get changelogSpecs(): ReadonlyMap<string, ChangelogTopicSpec> {
		return this.specs
	}

	hasPartitionScopedStores(): boolean {
		for (const def of this.defs.values()) {
			if (def.scope === 'partition') return true
		}
		return false
	}

	/**
	 * Drop all in-process store instances and close the provider, which owns the underlying stores.
	 * Stores are re-created on demand afterwards.
	 */
	async close(): Promise<void> {
		for (const def of this.defs.values()) {
			for (const partition of def.partitions.materializedPartitions()) {
				def.partitions.forget(partition)
			}
		}
		this.globalStoresStale = true
		await this.provider.close()
	}

	/**
	 * Name under which a store (or one partition of it) is kept by the state store provider.
	 * Prefixed with the application id so applications sharing one provider (e.g. one LMDB
	 * directory) can never read or wipe each other's state.
	 */
	private providerStoreName(storeName: string, partition?: number): string {
		const scoped = `${this.config.applicationId}.${storeName}`
		return partition === undefined ? scoped : partitionStoreName(scoped, partition)
	}

	private getOrCreate<S extends AnyStore, KK, V>(storeName: string, layout: StoreLayout<S, KK, V>): S {
		const existing = this.stateStores.get(storeName)
		if (existing) {
			// A global table's store has no definition; it is a key-value store.
			const existingKind = this.defs.get(storeName)?.kind ?? 'key-value'
			if (existingKind !== layout.kind) {
				throw new Error(
					`State store '${storeName}' already exists as a ${existingKind} store and cannot be used as a ` +
						`${layout.kind} store; give one of them a different storeName`
				)
			}
			return existing as unknown as S
		}

		// A store is partition-scoped when its keys are co-partitioned with known source topics.
		const { sourceTopics } = layout.options
		const scope: StoreScope = sourceTopics.size > 0 ? 'partition' : 'app'
		const changelogConfig = resolveChangelogConfig(layout.options.changelog)
		const changelog = changelogConfig.enabled ? this.changelogSpec(storeName, layout, changelogConfig) : null
		const writer = changelog ? this.changelogWriter(changelog, layout, scope) : null

		const partitions = new StorePartitions<S, S>(
			storeName,
			partition =>
				layout.createStack(
					this.providerStoreName(storeName, scope === 'partition' ? partition : undefined),
					writer
				),
			scope === 'app'
		)
		const facade = layout.createFacade(partitions)

		this.defs.set(storeName, {
			name: storeName,
			kind: layout.kind,
			scope,
			partitions: partitions as unknown as StorePartitions<AnyStore, AnyStore>,
			changelog,
			sourceTopics,
		})
		if (changelog) {
			this.specs.set(storeName, changelog)
		}
		this.stateStores.set(storeName, facade as unknown as KeyValueStore<unknown, unknown>)
		if (scope === 'partition') {
			this.topology.addStoreCopartitionGroup(sourceTopics)
		}
		return facade
	}

	/**
	 * The changelog topic of a store.
	 *
	 * Its key codec (de)serializes changelog record keys during restoration. For window and session
	 * stores it is `windowedKeyCodec(userKeyCodec)`, so the WindowedKey round-trips through the changelog.
	 */
	private changelogSpec<S extends AnyStore, KK, V>(
		storeName: string,
		layout: StoreLayout<S, KK, V>,
		changelogConfig: ChangelogConfig
	): ChangelogTopicSpec {
		// Window/session changelogs use delete+compact with a finite retention tied to the store's
		// retention, so the broker prunes records for expired windows. Without this the (KV) default
		// of compact + infinite retention would keep every window forever and resurrect expired
		// windows on restore. User-supplied topicConfigs still take precedence.
		const windowConfigOverrides: Record<string, string> =
			layout.retentionMs !== undefined
				? { 'cleanup.policy': 'delete,compact', 'retention.ms': String(layout.retentionMs) }
				: {}

		return {
			storeName,
			topicName: changelogConfig.topicName ?? buildChangelogTopicName(this.config.applicationId, storeName),
			replicationFactor: changelogConfig.replicationFactor,
			configs: getDefaultTopicConfigs({ ...windowConfigOverrides, ...changelogConfig.topicConfigs }),
			keyCodec: layout.changelogKeyCodec as Codec<unknown>,
			valueCodec: layout.options.valueCodec as Codec<unknown>,
			skipRestoration: changelogConfig.skipRestoration ?? false,
			sourceTopics: layout.options.sourceTopics,
			validateOnly: this.config.changelog?.autoCreate === false,
		}
	}

	/** Logs the writes made while processing a record to the store's changelog. */
	private changelogWriter<S extends AnyStore, KK, V>(
		spec: ChangelogTopicSpec,
		layout: StoreLayout<S, KK, V>,
		scope: StoreScope
	): ChangelogWriter<KK, V> {
		return new ChangelogWriter<KK, V>(
			spec.topicName,
			layout.changelogKeyCodec,
			layout.options.valueCodec,
			() => {
				const ctx = recordContextStorage.getStore()
				if (!ctx) {
					throw new Error('Changelog write called outside of message processing context')
				}
				return ctx.worker.producer
			},
			() => recordContextStorage.getStore()?.worker.activeTransaction ?? null,
			async result => {
				// Mocked producers (tests) don't report offsets; nothing to checkpoint then.
				if (!result || typeof result.offset !== 'bigint') return
				const worker = recordContextStorage.getStore()?.worker
				if (worker?.activeTransaction) {
					// Checkpointed only once the transaction has committed (see WorkerTransactions.commit).
					worker.pendingCheckpoints.track(result.topic, result.partition, result.offset)
					return
				}
				await this.checkpoints.recordWrite(result.topic, result.partition, result.offset)
			},
			// State partition N is logged to changelog partition N (see ChangelogWriter).
			scope === 'partition' ? () => recordContextStorage.getStore()?.partition : undefined,
			// Acknowledgements are awaited at the worker's next commit point, not per write.
			ack => {
				const worker = recordContextStorage.getStore()?.worker
				worker?.sends.track(ack)
				return worker !== undefined
			}
		)
	}
}
