/**
 * The processor metadata of a task and the commit metadata format (Kafka Streams' `ProcessorMetadata` and
 * `TopicPartitionMetadata`):
 *
 *     base64( [0x02] [int64BE partitionTime] [meta bytes] )
 *     meta bytes = '' if empty, else [int32BE n] then n x ([int32BE keyLen][utf8 key][int64BE value]), keys sorted
 *
 * Keys are store names: `<store>` (processor clock), `<store>.observed` (store clock), `<store>.emit` (emit progress).
 */
import type { ProcessorMetadata } from '@/processor/api.js'

/** The metadata key of a store's clock (6.2). */
export const observedKey = (store: string): string => `${store}.observed`

/** The metadata key of a store's emit-on-window-close progress: the close time of its last scan (6.8). */
export const emitKey = (store: string): string => `${store}.emit`

const MAGIC_PARTITION_TIME = 1
const MAGIC_WITH_METADATA = 2
const INT32 = 4
const INT64 = 8
const EMPTY = Buffer.alloc(0)

/** What one committed offset says about its partition and the task's processors. */
export interface CommitMetadata {
	/** Max timestamp seen on the partition, -1 unknown. */
	readonly partitionTime: number
	readonly metadata: ReadonlyMap<string, number>
	/** Set when the string could not be decoded: the result is then `{ -1, empty }`. */
	readonly invalid?: string
}

/**
 * The processor metadata of one task: one object per task, shared by its processor contexts, processor clocks and
 * store clocks, and replaced in place when the task is seeded from its commits. It also keeps the values of the last
 * commit, which a task restarted from that commit starts with.
 */
export class ProcessorMetadataImpl implements ProcessorMetadata {
	private readonly values = new Map<string, number>()
	/** Equal to `values` while nothing changed since the last commit or seed. */
	private committedValues: ReadonlyMap<string, number> = new Map()
	private changed = false
	private serialized: Buffer | undefined = EMPTY

	get(key: string): number | undefined {
		return this.values.get(key)
	}

	/** The value as of the last commit, or as seeded before the first. */
	committed(key: string): number | undefined {
		return this.committedValues.get(key)
	}

	/** Sets the value; the task then needs a commit of all its input partitions. */
	put(key: string, value: number): void {
		if (!Number.isSafeInteger(value)) {
			throw new TypeError(`processor metadata '${key}' must be a safe integer, got ${value}`)
		}
		this.values.set(key, value)
		this.changed = true
		this.serialized = undefined
	}

	/** Whether a put happened since the last commit. */
	get needsCommit(): boolean {
		return this.changed
	}

	/** After a successful commit: the values are the committed ones, and no change is pending. */
	markCommitted(): void {
		if (this.changed) {
			this.committedValues = new Map(this.values)
			this.changed = false
		}
	}

	/**
	 * Seeding: replaces every entry with the max-merge of the committed metadata of the task's input partitions,
	 * keeping only `keys`, so the keys of removed or renamed stores are dropped. Not a change to commit.
	 */
	replaceAll(committed: Iterable<ReadonlyMap<string, number>>, keys: ReadonlySet<string>): void {
		this.values.clear()
		for (const metadata of committed) {
			for (const [key, value] of metadata) {
				const current = this.values.get(key)
				if (keys.has(key) && (current === undefined || value > current)) {
					this.values.set(key, value)
				}
			}
		}
		this.committedValues = new Map(this.values)
		this.changed = false
		this.serialized = undefined
	}

	/** The meta bytes, computed once per change (every input partition of every commit reuses them). */
	serialize(): Buffer {
		this.serialized ??= serialize(this.values)
		return this.serialized
	}
}

function serialize(values: ReadonlyMap<string, number>): Buffer {
	if (values.size === 0) {
		return EMPTY
	}
	const entries = [...values]
		.sort(([a], [b]) => (a < b ? -1 : 1))
		.map(([key, value]) => [Buffer.from(key), value] as const)
	const out = Buffer.alloc(entries.reduce((size, [key]) => size + INT32 + key.length + INT64, INT32))
	let offset = out.writeInt32BE(entries.length)
	for (const [key, value] of entries) {
		offset = out.writeInt32BE(key.length, offset)
		offset += key.copy(out, offset)
		offset = out.writeBigInt64BE(BigInt(value), offset)
	}
	return out
}

export function encodeCommitMetadata(partitionTime: number, metadata: ProcessorMetadataImpl): string {
	const meta = metadata.serialize()
	const out = Buffer.alloc(1 + INT64 + meta.length)
	out.writeUInt8(MAGIC_WITH_METADATA)
	out.writeBigInt64BE(BigInt(partitionTime), 1)
	meta.copy(out, 1 + INT64)
	return out.toString('base64')
}

const UNKNOWN: CommitMetadata = { partitionTime: -1, metadata: new Map() }

/** Lenient (KS): a missing or empty string is `{ -1, empty }`; anything undecodable too, with `invalid` set. */
export function decodeCommitMetadata(encoded: string | null | undefined): CommitMetadata {
	if (!encoded) {
		return UNKNOWN
	}
	const bytes = Buffer.from(encoded, 'base64')
	if (bytes.toString('base64').replace(/=+$/, '') !== encoded.replace(/=+$/, '')) {
		return { ...UNKNOWN, invalid: 'not base64' }
	}
	const magic = bytes[0]
	if (magic !== MAGIC_PARTITION_TIME && magic !== MAGIC_WITH_METADATA) {
		return { ...UNKNOWN, invalid: `unsupported version ${String(magic)}` }
	}
	if (bytes.length < 1 + INT64) {
		return { ...UNKNOWN, invalid: 'truncated partition time' }
	}
	const partitionTime = Number(bytes.readBigInt64BE(1))
	if (magic === MAGIC_PARTITION_TIME || bytes.length === 1 + INT64) {
		return { partitionTime, metadata: new Map() }
	}
	const metadata = deserialize(bytes, 1 + INT64)
	return metadata ? { partitionTime, metadata } : { ...UNKNOWN, invalid: 'malformed processor metadata' }
}

/** Mirrors KS `ProcessorMetadata.deserialize`: bounds-checked, trailing bytes ignored. */
function deserialize(bytes: Buffer, start: number): Map<string, number> | undefined {
	let offset = start
	if (bytes.length - offset < INT32) {
		return undefined
	}
	const count = bytes.readInt32BE(offset)
	offset += INT32
	if (count < 0 || count > (bytes.length - offset) / (INT32 + INT64)) {
		return undefined
	}
	const metadata = new Map<string, number>()
	for (let i = 0; i < count; i++) {
		if (bytes.length - offset < INT32) {
			return undefined
		}
		const keyLength = bytes.readInt32BE(offset)
		offset += INT32
		if (keyLength < 0 || keyLength + INT64 > bytes.length - offset) {
			return undefined
		}
		const key = bytes.toString('utf8', offset, offset + keyLength)
		offset += keyLength
		metadata.set(key, Number(bytes.readBigInt64BE(offset)))
		offset += INT64
	}
	return metadata
}
