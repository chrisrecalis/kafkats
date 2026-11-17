/** Same shape as the client's message headers. Not named `Headers` (Fetch API clash). */
export type RecordHeaders = Record<string, Buffer>

function checkTimestamp(timestamp: number): number {
	if (!Number.isInteger(timestamp) || timestamp < 0) {
		throw new TypeError(`A record timestamp must be a non-negative integer, got ${timestamp}`)
	}
	return timestamp
}

/**
 * A record as processors see it (Kafka Streams' `Record`). Named StreamRecord so that importing it never
 * shadows TypeScript's `Record<K, V>`. Immutable: the `with*` methods return new instances.
 */
export class StreamRecord<K, V> {
	readonly timestamp: number

	constructor(
		readonly key: K,
		readonly value: V,
		timestamp: number,
		readonly headers: RecordHeaders = {}
	) {
		this.timestamp = checkTimestamp(timestamp)
	}

	withKey<K2>(key: K2): StreamRecord<K2, V> {
		return new StreamRecord(key, this.value, this.timestamp, this.headers)
	}

	withValue<V2>(value: V2): StreamRecord<K, V2> {
		return new StreamRecord(this.key, value, this.timestamp, this.headers)
	}

	withTimestamp(timestamp: number): StreamRecord<K, V> {
		return new StreamRecord(this.key, this.value, timestamp, this.headers)
	}

	withHeaders(headers: RecordHeaders): StreamRecord<K, V> {
		return new StreamRecord(this.key, this.value, this.timestamp, headers)
	}
}

/** A record whose key cannot be changed (`processValues`). Same validation as StreamRecord. */
export class FixedKeyRecord<K, V> {
	readonly timestamp: number

	constructor(
		readonly key: K,
		readonly value: V,
		timestamp: number,
		readonly headers: RecordHeaders = {}
	) {
		this.timestamp = checkTimestamp(timestamp)
	}

	withValue<V2>(value: V2): FixedKeyRecord<K, V2> {
		return new FixedKeyRecord(this.key, value, this.timestamp, this.headers)
	}

	withTimestamp(timestamp: number): FixedKeyRecord<K, V> {
		return new FixedKeyRecord(this.key, this.value, timestamp, this.headers)
	}

	withHeaders(headers: RecordHeaders): FixedKeyRecord<K, V> {
		return new FixedKeyRecord(this.key, this.value, this.timestamp, headers)
	}
}
