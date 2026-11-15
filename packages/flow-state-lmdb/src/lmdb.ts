import { open, type Database, type RootDatabase } from 'lmdb'
import {
	OrderedChangelogCheckpointStore,
	OrderedKeyValueStore,
	OrderedSessionStore,
	OrderedWindowStore,
	type ChangelogCheckpointStore,
	type KeyValueStore,
	type KeyValueStoreOptions,
	type OrderedByteStore,
	type SessionStore,
	type SessionStoreOptions,
	type StateStoreProvider,
	type WindowStore,
	type WindowStoreOptions,
} from '@kafkats/flow'

export { readSignedTime, writeSignedTime } from '@kafkats/flow'

/** An LMDB database as the ordered byte store flow's state stores are built on. */
function lmdbBytes(db: Database<Buffer, Buffer>): OrderedByteStore {
	return {
		get: key => db.get(key),
		put: async (key, value) => {
			await db.put(key, value)
		},
		remove: async key => {
			await db.remove(key)
		},
		range: bounds => db.getRange(bounds),
		approximateCount: () => db.getKeysCount(),
		// lmdb-js resolves put() at visibility; `flushed` resolves once those writes are synced.
		flush: async () => {
			await db.flushed
		},
	}
}

/**
 * LMDB implementation of KeyValueStore.
 *
 * Uses LMDB for persistent, memory-mapped storage with ACID transactions.
 * Suitable for production workloads with large state.
 */
export class LMDBKeyValueStore<K, V> extends OrderedKeyValueStore<K, V> {
	constructor(name: string, db: Database<Buffer, Buffer>, options: KeyValueStoreOptions<K, V>) {
		super(name, lmdbBytes(db), options)
	}
}

/** LMDB implementation of WindowStore. Keys are ordered by [windowStart, windowEnd, key]. */
export class LMDBWindowStore<K, V> extends OrderedWindowStore<K, V> {
	constructor(name: string, db: Database<Buffer, Buffer>, options: WindowStoreOptions<K, V>) {
		super(name, lmdbBytes(db), options)
	}
}

/** LMDB implementation of SessionStore. Keys are ordered by [key, windowStart, windowEnd]. */
export class LMDBSessionStore<K, V> extends OrderedSessionStore<K, V> {
	constructor(name: string, db: Database<Buffer, Buffer>, options: SessionStoreOptions<K, V>) {
		super(name, lmdbBytes(db), options)
	}
}

/**
 * Configuration options for LMDBStateStoreProvider.
 */
export interface LMDBProviderOptions {
	/** Directory for LMDB database files */
	stateDir: string
	/** Maximum database size in bytes (default: 1GB) */
	mapSize?: number
	/** Maximum number of named databases (default: 4096). One is used per state store partition. */
	maxDbs?: number
}

/**
 * LMDB state store provider.
 *
 * Creates persistent, memory-mapped stores backed by LMDB.
 * Suitable for production workloads with large state.
 */
export class LMDBStateStoreProvider implements StateStoreProvider {
	readonly name = 'lmdb'
	private rootDb: RootDatabase<Buffer, Buffer> | null = null
	private readonly stores = new Map<string, Database<Buffer, Buffer>>()
	private checkpointDb: OrderedByteStore | null = null
	private readonly checkpointStore: ChangelogCheckpointStore

	constructor(private readonly options: LMDBProviderOptions) {
		this.root()
		this.checkpointStore = new OrderedChangelogCheckpointStore(() => this.checkpoints())
	}

	/** The environment, (re)opened on demand so a provider can be used again after close(). */
	private root(): RootDatabase<Buffer, Buffer> {
		if (!this.rootDb) {
			this.rootDb = open({
				path: this.options.stateDir,
				mapSize: this.options.mapSize ?? 1024 * 1024 * 1024, // 1GB default
				maxDbs: this.options.maxDbs ?? 4096,
				keyEncoding: 'binary',
				encoding: 'binary',
			})
		}
		return this.rootDb
	}

	private checkpoints(): OrderedByteStore {
		if (!this.checkpointDb) {
			this.checkpointDb = lmdbBytes(
				this.root().openDB('__kafkats_flow_changelog_checkpoints__', {
					keyEncoding: 'binary',
					encoding: 'binary',
				})
			)
		}
		return this.checkpointDb
	}

	private getOrCreateDb(name: string): Database<Buffer, Buffer> {
		let db = this.stores.get(name)
		if (!db) {
			db = this.root().openDB(name, {
				keyEncoding: 'binary',
				encoding: 'binary',
			})
			this.stores.set(name, db)
		}
		return db
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBKeyValueStore(name, db, options)
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBWindowStore(name, db, options)
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBSessionStore(name, db, options)
	}

	getChangelogCheckpointStore(): ChangelogCheckpointStore {
		return this.checkpointStore
	}

	async close(): Promise<void> {
		const root = this.rootDb
		this.rootDb = null
		this.checkpointDb = null
		this.stores.clear()
		await root?.close()
	}
}

/**
 * Create an LMDB state store provider.
 */
export function lmdb(options: LMDBProviderOptions): StateStoreProvider {
	return new LMDBStateStoreProvider(options)
}
