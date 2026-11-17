/**
 * Where the driver's producer puts a record sent without a partition (4.14): the producer's default partitioning, so
 * a re-keyed record reaches the task a Kafka run would give it.
 */

/** murmur2 of the key made positive (Java `Utils.toPositive`), modulo the partition count; 0 without a key. */
export function defaultPartition(key: Buffer | null, partitions: number): number {
	return key === null ? 0 : (murmur2(key) & 0x7fffffff) % partitions
}

/** Java-compatible murmur2 (seed 0x9747b28c): a copy of the default partitioner's hash of `@kafkats/client`. */
function murmur2(data: Buffer): number {
	const m = 0x5bd1e995
	let h = 0x9747b28c ^ data.length
	let offset = 0
	for (; offset + 4 <= data.length; offset += 4) {
		let k = data[offset]! | (data[offset + 1]! << 8) | (data[offset + 2]! << 16) | (data[offset + 3]! << 24)
		k = Math.imul(k, m)
		k ^= k >>> 24
		k = Math.imul(k, m)
		h = Math.imul(h, m) ^ k
	}
	switch (data.length - offset) {
		case 3:
			h ^= data[offset + 2]! << 16
		// falls through
		case 2:
			h ^= data[offset + 1]! << 8
		// falls through
		case 1:
			h ^= data[offset]!
			h = Math.imul(h, m)
	}
	h ^= h >>> 13
	h = Math.imul(h, m)
	return h ^ (h >>> 15)
}
