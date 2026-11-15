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

/**
 * Configuration options for RocksDBStateStoreProvider.
 */
export interface RocksDBProviderOptions {
	/** Directory of the RocksDB database. One process at a time can have it open. */
	stateDir: string
	/**
	 * Options passed to RocksDB when the database is opened (memtable size, compression, ...).
	 * Key and value encoding are fixed by the provider.
	 */
	rocksdb?: Omit<
		RocksDatabaseOptions,
		// The write-ahead log is what makes flushed state and checkpoints durable.
		'name' | 'keyEncoding' | 'encoding' | 'readOnly' | 'secondaryPath' | 'store' | 'disableWAL'
	>
}

/**
 * All stores live in one RocksDB keyspace. Every key starts with the 4-byte id of its keyspace:
 * 0 holds the provider's own records, every store gets the next free id when it is first created.
 * Creating a store is therefore a single write, however many stores and partitions there are.
 */
const KEYSPACE_BYTES = 4
const META_KEYSPACE = 0
/** In the meta keyspace: `n` + store name -> id of the store's keyspace. */
const STORE_ID_PREFIX = Buffer.from('n')
/** In the meta keyspace: the id the next new store gets. */
const NEXT_STORE_ID = Buffer.from('i')
/** In the meta keyspace: `c` + checkpoint key -> changelog checkpoint. */
const CHECKPOINT_PREFIX = Buffer.from('c')

const keyspace = (id: number): Buffer => {
	const prefix = Buffer.alloc(KEYSPACE_BYTES)
	prefix.writeUInt32BE(id)
	return prefix
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
	private syncRunning: Promise<void> | null = null
	private syncQueued: Promise<void> | null = null
	private readonly checkpointStore: ChangelogCheckpointStore

	constructor(private readonly options: RocksDBProviderOptions) {
		this.database()
		this.checkpointStore = new OrderedChangelogCheckpointStore(() =>
			this.bytes(Buffer.concat([keyspace(META_KEYSPACE), CHECKPOINT_PREFIX]))
		)
	}

	/** The database, (re)opened on demand so a provider can be used again after close(). */
	private database(): RocksDatabase {
		if (!this.db) {
			this.db = RocksDatabase.open(this.options.stateDir, {
				...this.options.rocksdb,
				keyEncoding: 'binary',
				encoding: 'binary',
				disableWAL: false,
			})
		}
		return this.db
	}

	/**
	 * The keyspace of a store, assigned when the store is first created. The assignment lives in
	 * the database and is made in a transaction, so providers that share a directory (several in
	 * one process, or in worker threads) never hand out the same id: of two concurrent assignments
	 * one fails to commit and is retried.
	 */
	private storeKeyspace(name: string): Buffer {
		const db = this.database()
		const meta = keyspace(META_KEYSPACE)
		const nameKey = Buffer.concat([meta, STORE_ID_PREFIX, Buffer.from(name, 'utf8')])
		const counterKey = Buffer.concat([meta, NEXT_STORE_ID])
		return db.transactionSync(txn => {
			const assigned = txn.getSync(nameKey) as Buffer | undefined
			if (assigned) return assigned
			const next = (txn.getSync(counterKey) as Buffer | undefined) ?? keyspace(META_KEYSPACE + 1)
			txn.putSync(counterKey, keyspace(next.readUInt32BE() + 1))
			txn.putSync(nameKey, next)
			return next
		}) as Buffer
	}

	/** The keys starting with `prefix`, as the ordered byte store flow's state stores are built on. */
	private bytes(prefix: Buffer): OrderedByteStore {
		const database = (): RocksDatabase => this.database()
		const prefixed = (key: Buffer): Buffer => Buffer.concat([prefix, key])
		const bounds = ({ start, end }: { start?: Buffer; end?: Buffer }) => ({
			start: start ? prefixed(start) : prefix,
			end: end ? prefixed(end) : after(prefix),
		})
		return {
			// Answers synchronously when the value is in memory, with a promise when it is read from disk.
			get: key => this.database().get(prefixed(key)) as Buffer | undefined | Promise<Buffer | undefined>,
			put: (key, value) => this.database().put(prefixed(key), value),
			remove: key => this.database().remove(prefixed(key)),
			*range(range) {
				for (const { key, value } of database().getRange(bounds(range))) {
					yield { key: (key as Buffer).subarray(prefix.length), value: value as Buffer }
				}
			},
			approximateCount: () => {
				const db = this.database()
				const all = bounds({})
				// The estimate comes from table statistics and can be 0 for a few present keys.
				let empty = true
				for (const _ of db.getKeys({ ...all, limit: 1 })) empty = false
				if (empty) return 0
				return Math.max(1, Math.round(db.estimateCount(all).count))
			},
			flush: () => this.sync(),
		}
	}

	/**
	 * Make every write so far durable. All stores and the checkpoints share one write-ahead log, so
	 * one sync covers them all, and a crash never keeps a checkpoint without the store data written
	 * before it.
	 *
	 * A commit flushes every store partition and the checkpoints. At most one sync runs at a time;
	 * everyone who asks while it runs shares the single sync that follows it.
	 */
	private sync(): Promise<void> {
		// Nothing to make durable (and nothing to reopen) once the database is closed.
		if (!this.db) return Promise.resolve()
		if (!this.syncRunning) return (this.syncRunning = this.runSync())
		this.syncQueued ??= this.syncRunning
			.catch(() => {})
			.then(() => {
				this.syncQueued = null
				return (this.syncRunning = this.runSync())
			})
		return this.syncQueued
	}

	private runSync(): Promise<void> {
		const done = (): void => {
			if (this.syncRunning === running) this.syncRunning = null
		}
		const running: Promise<void> = (this.db?.syncWal() ?? Promise.resolve()).finally(done)
		return running
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		return new OrderedKeyValueStore(name, this.bytes(this.storeKeyspace(name)), options)
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		return new OrderedWindowStore(name, this.bytes(this.storeKeyspace(name)), options)
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		return new OrderedSessionStore(name, this.bytes(this.storeKeyspace(name)), options)
	}

	getChangelogCheckpointStore(): ChangelogCheckpointStore {
		return this.checkpointStore
	}

	async close(): Promise<void> {
		const db = this.db
		if (!db) return
		await this.sync().catch(() => {})
		this.db = null
		db.close()
	}
}

/** The smallest key greater than every key starting with `prefix`. */
function after(prefix: Buffer): Buffer {
	const end = Buffer.from(prefix)
	for (let i = end.length - 1; i >= 0; i--) {
		if (end[i]! < 0xff) {
			end[i]! += 1
			return end.subarray(0, i + 1)
		}
	}
	throw new Error('prefix has no successor')
}

/**
 * Create a RocksDB state store provider.
 */
export function rocksdb(options: RocksDBProviderOptions): StateStoreProvider {
	return new RocksDBStateStoreProvider(options)
}
