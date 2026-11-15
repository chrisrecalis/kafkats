import { RocksDatabase } from '@harperfast/rocksdb-js'
import type {
	ChangelogCheckpointStore,
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	StateStoreProvider,
	WindowStore,
	WindowStoreOptions,
} from '@kafkats/flow'
import {
	OrderedChangelogCheckpointStore,
	OrderedKeyValueStore,
	OrderedSessionStore,
	OrderedWindowStore,
	type OrderedByteStore,
} from '@kafkats/flow/state'
import { Coalesced } from './coalesced.js'
import { CHECKPOINTS, prefixedBytes, storeKeyspace } from './keyspace.js'

/**
 * Configuration options for RocksDBStateStoreProvider.
 */
export interface RocksDBProviderOptions {
	/** Directory of the RocksDB database. One process at a time can have it open. */
	stateDir: string
	/**
	 * How many bytes of writes are collected in memory before they are written to a file
	 * (default 16 MiB). Larger values batch more per file and use more memory.
	 */
	writeBufferSize?: number
	/** Compression of the data files (default `'lz4'`). */
	compression?: 'none' | 'lz4' | 'zstd' | 'snappy'
	/**
	 * How many data files RocksDB keeps open. By default it is derived from the process's open-file
	 * limit; raise that limit for very large state.
	 */
	maxOpenFiles?: number
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
		const { stateDir, writeBufferSize, compression, maxOpenFiles } = this.options
		this.db ??= RocksDatabase.open(stateDir, {
			...(writeBufferSize !== undefined && { writeBufferSize }),
			...(compression !== undefined && { compression }),
			...(maxOpenFiles !== undefined && { maxOpenFiles }),
			keyEncoding: 'binary',
			encoding: 'binary',
			// The write-ahead log is what makes flushed state and checkpoints durable.
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
