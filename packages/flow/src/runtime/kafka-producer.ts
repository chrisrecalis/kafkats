/**
 * The producer port over @kafkats/client (3.6, C11): at-least-once sends, and exactly-once transactions through the
 * client's transaction handles, whose commit and abort take the port's signal.
 */
import type { Producer, SendResult, TransactionHandle } from '@kafkats/client'
import type { ProducedMessage, ProducerPort, TransactionPort } from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'

/** Bound of the client's disconnect (flush, end of an open transaction) and of the abort of an unwanted transaction. */
export const PRODUCER_CLOSE_TIMEOUT_MS = 5000

export class KafkaProducer implements ProducerPort {
	constructor(private readonly producer: Producer) {}

	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		return this.producer.send(topic, message)
	}

	/**
	 * The signal bounds the wait for the transaction to open, not its life: the client's own signal option would
	 * abort the open transaction later. A transaction that opens after the caller gave up is aborted, so it does not
	 * hold the producer's turn.
	 */
	async beginTransaction({ signal }: { signal: AbortSignal }): Promise<TransactionPort> {
		const opening = this.producer.beginTransaction()
		try {
			return transactionPort(await raceSignal(opening, signal))
		} catch (error) {
			opening.then(
				transaction => transaction.abort({ timeoutMs: PRODUCER_CLOSE_TIMEOUT_MS }).catch(() => {}),
				() => {}
			)
			throw error
		}
	}

	partitionCount(topic: string, { signal }: { signal: AbortSignal }): Promise<number> {
		return this.producer.partitionCount(topic, { signal })
	}

	/** The client bounds the disconnect by itself; past `signal` it goes on in the background. */
	close({ signal }: { signal: AbortSignal }): Promise<void> {
		return raceSignal(this.producer.disconnect({ timeoutMs: PRODUCER_CLOSE_TIMEOUT_MS }), signal)
	}
}

function transactionPort(transaction: TransactionHandle): TransactionPort {
	return {
		// One message in, one result out.
		send: (topic, message) => transaction.send(topic, message) as Promise<SendResult>,
		sendOffsets: ({ consumerGroupMetadata, offsets }) =>
			transaction.sendOffsets({ consumerGroupMetadata, offsets: [...offsets] }),
		commit: ({ signal }) => transaction.commit({ signal }),
		abort: ({ signal }) => transaction.abort({ signal }),
	}
}
