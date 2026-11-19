import type { Message } from '@kafkats/client'

/** A record as a test writes it: strings for key and value. */
export interface TestRecord {
	readonly key?: string | null
	readonly value?: string | null
	readonly timestamp?: number
	readonly headers?: Record<string, Buffer>
	/** Default: the next offset of the partition. */
	readonly offset?: bigint
}

export function message(
	topic: string,
	partition: number,
	offset: bigint,
	record: TestRecord = {}
): Message<Buffer, Buffer> {
	return {
		topic,
		partition,
		offset,
		timestamp: BigInt(record.timestamp ?? 0),
		key: record.key === undefined || record.key === null ? null : Buffer.from(record.key),
		value: (record.value === undefined || record.value === null ? null : Buffer.from(record.value)) as Buffer,
		headers: record.headers ?? {},
	}
}
