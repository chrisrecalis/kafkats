/**
 * Incremental Kafka frame decoder (length-prefixed messages).
 *
 * Kafka protocol framing:
 * - 4-byte big-endian INT32 length prefix (payload length, excludes the prefix)
 * - payload bytes
 *
 * This decoder avoids Buffer.concat() on every incoming chunk by keeping a queue
 * of received buffers and only copying when a frame spans multiple chunks.
 */

/**
 * Default upper bound on a single response frame (1 GiB). A frame larger than this almost
 * certainly means a corrupt/desynced length prefix rather than a real Kafka response, and
 * buffering toward / allocating the attacker-supplied size (up to ~2 GiB for an int32) would be
 * a memory-exhaustion vector. The bound is intentionally generous: a consumer fetch requests
 * `maxBytesPerPartition * partitions` with no overall cap, so a high-partition fetch can legitimately
 * approach hundreds of MiB; 1 GiB stays well clear of that while still rejecting the int32-max
 * corruption. Tune it via the client `maxFrameSize` option for unusually large or constrained setups.
 */
export const DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1024

export class KafkaFrameDecoder {
	private readonly buffers: Buffer[] = []
	private bufferOffset = 0 // Offset within buffers[0]
	private availableBytes = 0 // Total bytes available across buffers (from bufferOffset)
	private expectedLength = 0 // 0 means "need length prefix"
	private readonly maxFrameSize: number

	constructor(maxFrameSize: number = DEFAULT_MAX_FRAME_SIZE) {
		this.maxFrameSize = maxFrameSize
	}

	/**
	 * Push a new chunk and return any complete payloads extracted.
	 *
	 * @throws if a frame's declared length exceeds maxFrameSize (corrupt/desynced stream)
	 */
	push(chunk: Buffer): Buffer[] {
		if (chunk.length === 0) return []

		this.buffers.push(chunk)
		this.availableBytes += chunk.length

		const messages: Buffer[] = []

		while (true) {
			// Need length prefix
			if (this.expectedLength === 0) {
				if (this.availableBytes < 4) {
					break
				}

				this.expectedLength = this.peekInt32BE()
				this.consumeBytes(4)

				// Reset on invalid lengths
				if (this.expectedLength <= 0) {
					this.expectedLength = 0
					continue
				}

				// Guard against a corrupt/desynced length prefix driving an unbounded allocation.
				if (this.expectedLength > this.maxFrameSize) {
					throw new Error(
						`Kafka frame length ${this.expectedLength} exceeds maximum ${this.maxFrameSize} (corrupt stream?)`
					)
				}
			}

			// Need full payload
			if (this.availableBytes < this.expectedLength) {
				break
			}

			const payload = this.consumeBytes(this.expectedLength)
			this.expectedLength = 0
			messages.push(payload)
		}

		return messages
	}

	reset(): void {
		this.buffers.length = 0
		this.bufferOffset = 0
		this.availableBytes = 0
		this.expectedLength = 0
	}

	private peekInt32BE(): number {
		const first = this.buffers[0]!
		const availableInFirst = first.length - this.bufferOffset
		if (availableInFirst >= 4) {
			return first.readInt32BE(this.bufferOffset)
		}

		const tmp = Buffer.allocUnsafe(4)
		let copied = 0

		for (let i = 0; copied < 4; i++) {
			const buf = this.buffers[i]!
			const start = i === 0 ? this.bufferOffset : 0
			const available = buf.length - start
			const toCopy = Math.min(4 - copied, available)
			buf.copy(tmp, copied, start, start + toCopy)
			copied += toCopy
		}

		return tmp.readInt32BE(0)
	}

	private consumeBytes(length: number): Buffer {
		if (length === 0) {
			return Buffer.alloc(0)
		}

		const first = this.buffers[0]!
		const availableInFirst = first.length - this.bufferOffset

		if (length <= availableInFirst) {
			const start = this.bufferOffset
			const end = start + length
			const slice = first.subarray(start, end)

			this.bufferOffset = end
			this.availableBytes -= length

			if (this.bufferOffset === first.length) {
				this.buffers.shift()
				this.bufferOffset = 0
			}

			return slice
		}

		const out = Buffer.allocUnsafe(length)
		let copied = 0

		while (copied < length) {
			const buf = this.buffers[0]!
			const start = this.bufferOffset
			const available = buf.length - start
			const toCopy = Math.min(length - copied, available)
			buf.copy(out, copied, start, start + toCopy)
			copied += toCopy

			this.bufferOffset += toCopy
			this.availableBytes -= toCopy

			if (this.bufferOffset === buf.length) {
				this.buffers.shift()
				this.bufferOffset = 0
			}
		}

		return out
	}
}
