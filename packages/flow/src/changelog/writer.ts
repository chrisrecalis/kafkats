import type { Producer, SendResult } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { WindowedKey } from '@/state.js'

/** What the writer sends through: a Producer, or the worker's open transaction. */
interface TransactionLike {
	send(
		topic: string,
		messages: { key?: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer>; partition?: number }
	): Promise<SendResult>
}

/**
 * Writes state mutations to a changelog topic.
 *
 * Records are written to the changelog partition with the same index as the source partition of
 * the record being processed (`getPartition`). Restoration reads changelog partition N to rebuild
 * the state of source partition N, so the two must line up regardless of how the changelog key
 * hashes (windowed keys, custom input partitioners, differing key codecs, ...).
 */
export class ChangelogWriter<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly getProducer: () => Producer,
		private readonly getTransaction: () => TransactionLike | null,
		private readonly onWriteAck?: (result: SendResult) => void | Promise<void>,
		private readonly getPartition?: () => number | undefined,
		/**
		 * Takes over a write's acknowledgement: waiting for it, and calling `onAck` when it arrives
		 * (returns true if it did). Without it, every write waits for its own acknowledgement.
		 */
		private readonly defer?: (
			send: Promise<SendResult>,
			onAck: (result: SendResult) => void | Promise<void>
		) => boolean
	) {}

	async write(key: K, value: V): Promise<void> {
		await this.send(this.keyCodec.encode(key), this.valueCodec.encode(value))
	}

	async writeTombstone(key: K): Promise<void> {
		await this.send(this.keyCodec.encode(key), null)
	}

	private async send(key: Buffer, value: Buffer | null): Promise<void> {
		const partition = this.getPartition?.()
		const message = partition === undefined ? { key, value } : { key, value, partition }
		const target = this.getTransaction() ?? this.getProducer()
		const send = target.send(this.topicName, message)
		const onAck = (result: SendResult) => this.onWriteAck?.(result)
		if (!this.defer?.(send, onAck)) await onAck(await send)
	}
}

/**
 * Changelog record key of window and session stores: the user key followed by window start and
 * end as signed 64-bit big-endian integers. This is the on-topic format; changing it breaks
 * restoration from existing changelogs.
 */
export function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode(windowed: WindowedKey<K>): Buffer {
			const keyBuffer = keyCodec.encode(windowed.key)
			const windowBuffer = Buffer.alloc(16)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowStart), 0)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowEnd), 8)
			return Buffer.concat([keyBuffer, windowBuffer])
		},
		decode(buffer: Buffer): WindowedKey<K> {
			const keyBuffer = buffer.subarray(0, buffer.length - 16)
			const windowBuffer = buffer.subarray(buffer.length - 16)
			return {
				key: keyCodec.decode(keyBuffer),
				windowStart: Number(windowBuffer.readBigInt64BE(0)),
				windowEnd: Number(windowBuffer.readBigInt64BE(8)),
			}
		},
	}
}
