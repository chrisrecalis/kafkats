import { InvalidTimestampError } from '@/api/errors.js'
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

/** Where a record comes from, for error messages. */
interface RecordPosition {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
}

/**
 * The logic of a built-in extractor, which needs only the record timestamp: the record queue runs it on the raw
 * message without building a ConsumedRecord.
 */
export type TimestampRule = (timestamp: number, partitionTime: number, record: RecordPosition) => number

const rules = new Map<TimestampExtractor, TimestampRule>()

function builtIn(rule: TimestampRule): TimestampExtractor {
	const extractor: TimestampExtractor = (record, partitionTime) => rule(record.timestamp, partitionTime, record)
	rules.set(extractor, rule)
	return extractor
}

function describe(record: RecordPosition): string {
	return `${record.topic}-${record.partition}@${record.offset}`
}

/** Kafka Streams' built-in extractors. */
export const TimestampExtractors = {
	/** The record timestamp; a negative one stops the app (InvalidTimestampError). The default. */
	failOnInvalid: builtIn((timestamp, _partitionTime, record) => {
		if (timestamp < 0) {
			throw new InvalidTimestampError(
				`Input record ${describe(record)} has an invalid (negative) timestamp ${timestamp}: it was possibly ` +
					'written without one. Use another timestamp extractor to process this data.'
			)
		}
		return timestamp
	}),
	/** The record timestamp; a record with a negative one is dropped with a warning. */
	logAndSkipOnInvalid: builtIn(timestamp => timestamp),
	/** The record timestamp; a negative one is replaced by the partition time (InvalidTimestampError while unknown). */
	usePartitionTimeOnInvalid: builtIn((timestamp, partitionTime, record) => {
		if (timestamp >= 0) {
			return timestamp
		}
		if (partitionTime < 0) {
			throw new InvalidTimestampError(
				`Could not infer a timestamp for input record ${describe(record)} with the invalid timestamp ` +
					`${timestamp}: the partition time is unknown`
			)
		}
		return partitionTime
	}),
	/** The wall clock at extraction (processing time). */
	wallClock: builtIn(() => Date.now()),
} as const satisfies Record<string, TimestampExtractor>

/** The rule of a built-in extractor; undefined for any other extractor. */
export function builtInRule(extractor: TimestampExtractor): TimestampRule | undefined {
	return rules.get(extractor)
}
