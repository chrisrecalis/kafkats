import type { Codec } from '@/codec.js'
import type { TaskOutput } from '@/processor/output.js'
import type { WindowedKey } from '@/state.js'

/**
 * Writes the mutations of one task's store to its changelog partition, through the task's output.
 *
 * The partition is the task's partition: restoration rebuilds the state of task partition N from changelog partition
 * N, so the two must line up however the changelog key hashes (windowed keys, custom input partitioners, differing key
 * codecs, ...). Sends are not awaited here: the task's commit flushes them, and only acknowledged writes become
 * checkpoints. Records carry no timestamp (broker time), so the topic's time retention follows write time.
 */
export class ChangelogWriter<K, V> {
	constructor(
		private readonly topic: string,
		private readonly partition: number,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly output: TaskOutput
	) {}

	write(key: K, value: V): void {
		this.send(this.keyCodec.encode(key), this.valueCodec.encode(value))
	}

	writeTombstone(key: K): void {
		this.send(this.keyCodec.encode(key), null)
	}

	private send(key: Buffer, value: Buffer | null): void {
		this.output.send('changelog', { topic: this.topic, partition: this.partition, key, value })
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
