/**
 * Decoding and timestamp extraction of a raw record (4.5.2 steps 1-2), shared by the record queues of tasks and the
 * global stores: the codecs of every source node of the topic (no codec: the raw Buffer; null stays null), the
 * deserialization handler on a failure, the timestamp extractor (built-ins on the raw message), and the skip rules.
 */
import type { Message } from '@kafkats/client'
import { InvalidTimestampError, StreamsError } from '@/api/errors.js'
import type { Codec } from '@/codec.js'
import type { DroppedReason, TaskId } from '@/processor/api.js'
import { applyHandler } from '@/processor/node.js'
import { builtInRule, type TimestampExtractor, type TimestampRule } from '@/processor/timestamps.js'
import type { ResolvedDeserializationHandler } from '@/runtime/config.js'
import type { SourceDef } from '@/topology/nodes.js'

/** Reports a dropped record: reason, source node, details for the log. */
export type DropListener = (reason: DroppedReason, source: string, detail: Readonly<Record<string, unknown>>) => void

/** A source node reading the topic: its name and codecs. */
export type DecodingSource = Pick<SourceDef, 'name' | 'keyCodec' | 'valueCodec'>

/** A record decoded for the first source node of its topic (and the others, rare), with its extracted timestamp. */
export interface DecodedRecord {
	readonly key: unknown
	readonly value: unknown
	readonly timestamp: number
	/** Decoded for the other source nodes of the topic, in registration order. */
	readonly more?: readonly { key: unknown; value: unknown }[]
}

export class RecordDecoder {
	private readonly rule: TimestampRule | undefined

	constructor(
		private readonly sources: readonly DecodingSource[],
		private readonly extractor: TimestampExtractor,
		private readonly handler: ResolvedDeserializationHandler,
		private readonly onDrop: DropListener,
		private readonly taskId: TaskId
	) {
		this.rule = builtInRule(extractor)
	}

	/**
	 * Undefined: the record is skipped (the deserialization handler answered 'continue', or the timestamp is
	 * negative) and reported to the drop listener. A 'fail' of the handler, a failing extractor or one that returns
	 * no integer throws.
	 */
	decode(message: Message<Buffer, Buffer>, partitionTime: number): DecodedRecord | undefined {
		const { sources } = this
		let source = sources[0]!
		let key: unknown
		let value: unknown
		let more: { key: unknown; value: unknown }[] | undefined
		try {
			key = decode(message.key, source.keyCodec)
			value = decode(message.value, source.valueCodec)
			if (sources.length > 1) {
				// Each source node its own objects: a processor may mutate what it gets.
				more = []
				for (let i = 1; i < sources.length; i++) {
					source = sources[i]!
					more.push({
						key: decode(message.key, source.keyCodec),
						value: decode(message.value, source.valueCodec),
					})
				}
			}
		} catch (error) {
			this.deserializationFailed(message, source, error)
			return undefined
		}
		const timestamp = this.extract(message, key, value, partitionTime)
		if (timestamp < 0) {
			this.onDrop('negative-timestamp', sources[0]!.name, { ...position(message), timestamp })
			return undefined
		}
		return more === undefined ? { key, value, timestamp } : { key, value, timestamp, more }
	}

	private deserializationFailed(message: Message<Buffer, Buffer>, source: DecodingSource, error: unknown): void {
		const ctx = {
			...position(message),
			headers: message.headers,
			timestamp: Number(message.timestamp),
			processorNodeId: source.name,
			taskId: this.taskId,
			sourceRawKey: message.key,
			sourceRawValue: message.value,
		}
		applyHandler(
			() => this.handler(ctx, message, error),
			error,
			`deserialization of ${describe(message)} failed in '${source.name}'`,
			this.taskId
		)
		this.onDrop('deserialization', source.name, {
			...position(message),
			error: error instanceof Error ? error.message : String(error),
		})
	}

	private extract(message: Message<Buffer, Buffer>, key: unknown, value: unknown, partitionTime: number): number {
		const recordTimestamp = Number(message.timestamp)
		let timestamp: number
		try {
			if (this.rule) {
				return this.rule(recordTimestamp, partitionTime, message)
			}
			const record = { ...position(message), key, value, timestamp: recordTimestamp, headers: message.headers }
			timestamp = this.extractor(record, partitionTime)
		} catch (error) {
			if (error instanceof StreamsError) {
				throw error
			}
			throw new StreamsError(`the timestamp extractor failed for ${describe(message)}`, {
				cause: error,
				taskId: this.taskId,
			})
		}
		if (!Number.isSafeInteger(timestamp)) {
			throw new InvalidTimestampError(
				`the timestamp extractor returned ${timestamp} for ${describe(message)}, not an integer`,
				{ taskId: this.taskId }
			)
		}
		return timestamp
	}
}

/** No codec: the raw Buffer; null stays null. */
function decode(data: Buffer | null, codec: Codec<unknown> | undefined): unknown {
	return data === null || codec === undefined ? data : codec.decode(data)
}

function position(message: Message<Buffer, Buffer>): { topic: string; partition: number; offset: bigint } {
	return { topic: message.topic, partition: message.partition, offset: message.offset }
}

function describe(message: Message<Buffer, Buffer>): string {
	return `${message.topic}-${message.partition}@${message.offset}`
}
