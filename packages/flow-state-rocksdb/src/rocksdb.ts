import { RocksDatabase, type RocksDatabaseOptions } from '@harperfast/rocksdb-js'
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
import { Coalesced } from './coalesced.js'
import { CHECKPOINTS, prefixedBytes, storeKeyspace } from './keyspace.js'

/**
 * Configuration options for RocksDBStateStoreProvider.
 */
export interface RocksDBProviderOptions {
	/** Directory of the RocksDB database. One process at a time can have it open. */
	stateDir: string
	/**
	 * Options passed to RocksDB when the database is opened (memtable size, compression, ...).
	 * The provider fixes the key and value encoding and keeps the write-ahead log on: it is what
	 * makes flushed state and checkpoints durable.
	 */
	rocksdb?: Omit<
		RocksDatabaseOptions,
		'name' | 'keyEncoding' | 'encoding' | 'readOnly' | 'secondaryPath' | 'store' | 'disableWAL'
	>
}

/**
 * RocksDB state store provider.
 *
 * Creates persistent stores in one RocksDB database (a log-structured merge tree): writes are
 * appended to a write-ahead log and an in-memory table, so they do not wait for the disk, and
 * state can grow beyond memory.
 */
export class RocksDBStateStoreProvider implements StateStoreProvider {
	readonly name = 'rocksdb'
	private db: RocksDatabase | null = null
	/**
	 * Makes every write so far durable. All stores and the checkpoints share one write-ahead log,
	 * so one sync covers them all (a commit flushes every store partition and the checkpoints),
	 * and a crash never keeps a checkpoint without the store data written before it.
	 */
	private readonly walSync = new Coalesced(() => this.db?.syncWal() ?? Promise.resolve())
	private readonly checkpointStore: ChangelogCheckpointStore

	constructor(private readonly options: RocksDBProviderOptions) {
		this.database()
		this.checkpointStore = new OrderedChangelogCheckpointStore(this.bytes(CHECKPOINTS))
	}

	/** The database, (re)opened on demand so a provider can be used again after close(). */
	private database(): RocksDatabase {
		this.db ??= RocksDatabase.open(this.options.stateDir, {
			...this.options.rocksdb,
			keyEncoding: 'binary',
			encoding: 'binary',
			disableWAL: false,
		})
		return this.db
	}

	private bytes(prefix: Buffer): OrderedByteStore {
		return prefixedBytes(
			() => this.database(),
			prefix,
			// Nothing to make durable, and nothing to reopen, once the database is closed.
			() => (this.db ? this.walSync.request() : Promise.resolve())
		)
	}

	private storeBytes(name: string): OrderedByteStore {
		return this.bytes(storeKeyspace(this.database(), name))
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		return new OrderedKeyValueStore(name, this.storeBytes(name), options)
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		return new OrderedWindowStore(name, this.storeBytes(name), options)
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		return new OrderedSessionStore(name, this.storeBytes(name), options)
	}

	getChangelogCheckpointStore(): ChangelogCheckpointStore {
		return this.checkpointStore
	}

	async close(): Promise<void> {
		const db = this.db
		if (!db) return
		await this.walSync.request().catch(() => {})
		this.db = null
		db.close()
	}
}

/**
 * Create a RocksDB state store provider.
 */
export function rocksdb(options: RocksDBProviderOptions): StateStoreProvider {
	return new RocksDBStateStoreProvider(options)
}
