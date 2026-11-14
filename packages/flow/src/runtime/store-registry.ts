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
import type { FlowConfig } from '@/types.js'
import { recordContextStorage } from '@/runtime/context.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
import { trackChangelogOffset } from '@/runtime/transactions.js'
import type { GlobalTableDef, Topology } from '@/runtime/topology.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyStore = KeyValueStore<any, any>

type StoreScope = 'partition' | 'app'

/** A logical state store and how its state is laid out. */
export type StoreDef = {
	name: string
	/**
	 * 'partition': one slice of state per source partition, restored from the changelog partition
	 * of the same index (the default; safe across instances).
	 * 'app': a single app-wide store (legacy layout, only for stores whose keys are not
	 * co-partitioned with their input).
	 */
	scope: StoreScope
	partitions: StorePartitions<AnyStore, AnyStore>
	changelog: ChangelogTopicSpec | null
	sourceTopics: Set<string>
}

/** What getOrCreate() needs to know about one kind of store (key-value, window, session). */
type StoreLayout<S extends AnyStore, KK, V> = {
	changelog: boolean | ChangelogConfig | undefined
	sourceTopics: Set<string> | undefined
	restrictRestorationToSourcePartitions: boolean
	/** Codec of changelog record keys (the windowed key for window and session stores). */
	changelogKeyCodec: Codec<KK>
	valueCodec: Codec<V>
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
export class StoreRegistry {
	/** Every store by name, as handed to processors. */
	readonly stateStores = new Map<string, KeyValueStore<unknown, unknown>>()
	private readonly defs = new Map<string, StoreDef>()
	private readonly specs = new Map<string, ChangelogTopicSpec>()
	/** True once the provider has been closed: stores created at build time must be re-created. */
	private globalStoresStale = false

	constructor(
		private readonly config: FlowConfig,
		readonly provider: StateStoreProvider,
		private readonly topology: Topology,
		private readonly checkpoints: Checkpoints
	) {}

	getOrCreateStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): KeyValueStore<K, V> {
		return this.getOrCreate(name ?? `store-${this.topology.nextStoreId()}`, {
			changelog,
			sourceTopics,
			restrictRestorationToSourcePartitions,
			changelogKeyCodec: keyCodec,
			valueCodec,
			createStack: (providerName, writer) => {
				const raw = this.provider.createKeyValueStore<K, V>(providerName, { keyCodec, valueCodec })
				const buffered = new BufferedKeyValueStore<K, V>(raw, keyCodec, valueCodec)
				const store = writer ? new ChangelogBackedKeyValueStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			createFacade: partitions => new PartitionedKeyValueStore<K, V>(partitions),
		})
	}

	getOrCreateWindowStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		windowOptions: { retentionMs: number; windowSizeMs: number },
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): WindowStore<K, V> {
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		return this.getOrCreate(name ?? `window-store-${this.topology.nextStoreId()}`, {
			changelog,
			sourceTopics,
			restrictRestorationToSourcePartitions,
			changelogKeyCodec: storeKeyCodec,
			valueCodec,
			retentionMs: windowOptions.retentionMs,
			createStack: (providerName, writer) => {
				const raw = this.provider.createWindowStore<K, V>(providerName, {
					keyCodec,
					valueCodec,
					retentionMs: windowOptions.retentionMs,
					windowSizeMs: windowOptions.windowSizeMs,
				})
				const buffered = new BufferedWindowStore<K, V>(
					raw,
					storeKeyCodec,
					valueCodec,
					keyCodec,
					windowOptions.retentionMs
				)
				const store = writer ? new ChangelogBackedWindowStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			createFacade: partitions => new PartitionedWindowStore<K, V>(partitions),
		})
	}

	getOrCreateSessionStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		sessionOptions: { retentionMs: number },
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): SessionStore<K, V> {
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		return this.getOrCreate(name ?? `session-store-${this.topology.nextStoreId()}`, {
			changelog,
			sourceTopics,
			restrictRestorationToSourcePartitions,
			changelogKeyCodec: storeKeyCodec,
			valueCodec,
			retentionMs: sessionOptions.retentionMs,
			createStack: (providerName, writer) => {
				const raw = this.provider.createSessionStore<K, V>(providerName, {
					keyCodec,
					valueCodec,
					retentionMs: sessionOptions.retentionMs,
				})
				const buffered = new BufferedSessionStore<K, V>(
					raw,
					storeKeyCodec,
					valueCodec,
					keyCodec,
					sessionOptions.retentionMs
				)
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
			return existing as unknown as S
		}

		// A store is partition-scoped when its keys are co-partitioned with known source topics.
		const sourceTopics = layout.sourceTopics ?? new Set<string>()
		const scope: StoreScope =
			layout.restrictRestorationToSourcePartitions && sourceTopics.size > 0 ? 'partition' : 'app'
		const changelogConfig = resolveChangelogConfig(layout.changelog)
		const writer = changelogConfig.enabled
			? this.setupChangelog(
					storeName,
					layout.changelogKeyCodec,
					layout.valueCodec,
					changelogConfig,
					sourceTopics,
					scope,
					layout.retentionMs
				)
			: null

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
			scope,
			partitions: partitions as unknown as StorePartitions<AnyStore, AnyStore>,
			changelog: writer ? this.specs.get(storeName)! : null,
			sourceTopics,
		})
		this.stateStores.set(storeName, facade as unknown as KeyValueStore<unknown, unknown>)
		if (scope === 'partition') {
			this.topology.addStoreCopartitionGroup(sourceTopics)
		}
		return facade
	}

	/**
	 * Register a changelog topic spec and writer for a store.
	 *
	 * `changelogKeyCodec` is the codec used to (de)serialize changelog record keys during
	 * restoration. For windowed/session stores it is `windowedKeyCodec(userKeyCodec)` so the
	 * WindowedKey round-trips through the changelog.
	 */
	private setupChangelog<KK, VV>(
		storeName: string,
		changelogKeyCodec: Codec<KK>,
		valueCodec: Codec<VV>,
		changelogConfig: ChangelogConfig,
		sourceTopics: Set<string>,
		scope: StoreScope,
		windowRetentionMs?: number
	): ChangelogWriter<KK, VV> {
		const topicName = changelogConfig.topicName ?? buildChangelogTopicName(this.config.applicationId, storeName)

		// Window/session changelogs use delete+compact with a finite retention tied to the store's
		// retention, so the broker prunes records for expired windows. Without this the (KV) default
		// of compact + infinite retention would keep every window forever and resurrect expired
		// windows on restore. User-supplied topicConfigs still take precedence.
		const windowConfigOverrides: Record<string, string> =
			windowRetentionMs !== undefined
				? { 'cleanup.policy': 'delete,compact', 'retention.ms': String(windowRetentionMs) }
				: {}

		this.specs.set(storeName, {
			storeName,
			topicName,
			replicationFactor: changelogConfig.replicationFactor,
			configs: getDefaultTopicConfigs({ ...windowConfigOverrides, ...changelogConfig.topicConfigs }),
			keyCodec: changelogKeyCodec as Codec<unknown>,
			valueCodec: valueCodec as Codec<unknown>,
			skipRestoration: changelogConfig.skipRestoration ?? false,
			sourceTopics,
			restrictRestorationToSourcePartitions: scope === 'partition',
			validateOnly: this.config.changelog?.autoCreate === false,
		})

		return new ChangelogWriter<KK, VV>(
			topicName,
			changelogKeyCodec,
			valueCodec,
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
					// Checkpoint only once the transaction has committed (see WorkerTransactions.commit).
					trackChangelogOffset(worker, result.topic, result.partition, result.offset)
					return
				}
				await this.checkpoints.recordWrite(result.topic, result.partition, result.offset)
			},
			// State partition N is logged to changelog partition N (see ChangelogWriter).
			scope === 'partition' ? () => recordContextStorage.getStore()?.partition : undefined
		)
	}
}
