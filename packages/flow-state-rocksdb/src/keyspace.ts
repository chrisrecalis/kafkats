import type { RocksDatabase } from '@harperfast/rocksdb-js'
import type { OrderedByteStore } from '@kafkats/flow/state'

/**
 * All stores of a provider live in one RocksDB keyspace, told apart by a prefix: every key starts
 * with the 4-byte id of its keyspace. Keyspace 0 is the provider's own; every store gets the next
 * free id when it is first created. Creating a store is therefore one small write, however many
 * stores and partitions there are (a column family per store was measured: 400 took 23 seconds).
 */
const keyspace = (id: number): Buffer => {
	const prefix = Buffer.alloc(4)
	prefix.writeUInt32BE(id)
	return prefix
}

const META = keyspace(0)
const FIRST_STORE = keyspace(1)
/** `n` + store name -> the store's keyspace. */
const storeKey = (name: string): Buffer => Buffer.concat([META, Buffer.from('n'), Buffer.from(name, 'utf8')])
/** The keyspace the next new store gets. */
const NEXT_STORE = Buffer.concat([META, Buffer.from('i')])

/** Where the provider keeps its changelog checkpoints. */
export const CHECKPOINTS = Buffer.concat([META, Buffer.from('c')])

/**
 * The keyspace of the store `name`, assigned when the store is first created.
 *
 * The assignment is made in a transaction on the database, so providers that share a directory
 * (several in one process, or in worker threads) never give two stores the same keyspace: of two
 * concurrent assignments one fails to commit and is retried.
 */
export function storeKeyspace(db: RocksDatabase, name: string): Buffer {
	return db.transactionSync(txn => {
		const assigned = txn.getSync(storeKey(name)) as Buffer | undefined
		if (assigned) return assigned
		const next = (txn.getSync(NEXT_STORE) as Buffer | undefined) ?? FIRST_STORE
		txn.putSync(NEXT_STORE, keyspace(next.readUInt32BE() + 1))
		txn.putSync(storeKey(name), next)
		return next
	}) as Buffer
}

/**
 * The keys starting with `prefix`, without the prefix, as the ordered byte store flow's state
 * stores are built on. `database` is asked on every operation, so the provider can reopen it.
 */
export function prefixedBytes(
	database: () => RocksDatabase,
	prefix: Buffer,
	flush: () => Promise<void>
): OrderedByteStore {
	const prefixed = (key: Buffer): Buffer => Buffer.concat([prefix, key])
	const everything = { start: prefix, end: after(prefix) }
	return {
		// RocksDB answers synchronously when the value is in memory, with a promise when it reads the disk.
		get: key => database().get(prefixed(key)) as Buffer | undefined | Promise<Buffer | undefined>,
		put: (key, value) => database().put(prefixed(key), value),
		remove: key => database().remove(prefixed(key)),
		*range({ start, end }) {
			const bounds = {
				start: start ? prefixed(start) : everything.start,
				end: end ? prefixed(end) : everything.end,
			}
			for (const { key, value } of database().getRange(bounds)) {
				yield { key: (key as Buffer).subarray(prefix.length), value: value as Buffer }
			}
		},
		approximateCount() {
			// RocksDB estimates from table statistics and can say 0 for a few keys, so "empty" is checked.
			for (const _ of database().getKeys({ ...everything, limit: 1 })) {
				return Math.max(1, Math.round(database().estimateCount(everything).count))
			}
			return 0
		},
		flush,
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
