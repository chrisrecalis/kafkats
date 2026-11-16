import type { RecordHeaders } from '@/processor/record.js'

/** What a timestamp extractor sees of a consumed record: decoded key and value, record timestamp in ms. */
export interface ConsumedRecord<K, V> {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
	readonly key: K | null
	readonly value: V | null
	readonly timestamp: number
	readonly headers: RecordHeaders
}

/**
 * Picks the timestamp of a consumed record (Kafka Streams' `TimestampExtractor`). `partitionTime` is the
 * largest timestamp seen on the record's partition, -1 while unknown. A negative result drops the record.
 */
export type TimestampExtractor = (record: ConsumedRecord<unknown, unknown>, partitionTime: number) => number
