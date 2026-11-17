import { InvalidTxnStateError, type ConsumerGroupMetadata, type SendResult } from '@kafkats/client'
import type { OffsetAndMetadata, ProducedMessage, ProducerPort, TransactionPort } from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'
import { defaultPartition } from '@/testing/partitioner.js'
import type { FakeCluster, PartitionLog } from './cluster.js'
import { Steps } from './steps.js'

const TRANSACTIONAL_ID = 'fake'

export interface SentRecord {
	readonly topic: string
	readonly message: ProducedMessage
	readonly transaction?: FakeTransaction
}

/**
 * A producer whose every step ends as the test scripts it (`steps`; the transaction steps are shared by all its
 * transactions). Sends are recorded when made (`sent`) and acknowledged at the next offset of their partition: in
 * the cluster's log when the producer has a cluster (a transaction's records stay invisible to read_committed until
 * it commits), else from a counter per partition. Without a partition a record goes where the client's default
 * partitioner puts it.
 */
export class FakeProducer implements ProducerPort {
	readonly steps = {
		send: new Steps(),
		beginTransaction: new Steps(),
		sendOffsets: new Steps(),
		commit: new Steps(),
		abort: new Steps(),
		partitionCount: new Steps(),
		close: new Steps(),
	}
	/** Every send, in call order (a transaction's too). */
	readonly sent: SentRecord[] = []
	readonly transactions: FakeTransaction[] = []
	closed = false
	private readonly counters = new Map<string, bigint>()

	constructor(readonly cluster?: FakeCluster) {}

	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		return this.produce(topic, message)
	}

	/** One transaction at a time: beginning another while one is open is InvalidTxnStateError. */
	beginTransaction(options: { signal: AbortSignal }): Promise<TransactionPort> {
		return this.steps.beginTransaction.perform(() => {
			const current = this.transactions.at(-1)
			if (current?.state === 'open') {
				throw new InvalidTxnStateError(TRANSACTIONAL_ID, 'in_transaction', ['ready'])
			}
			const transaction = new FakeTransaction(this)
			this.transactions.push(transaction)
			return transaction
		}, options.signal)
	}

	/** The cluster's count (a topic nobody created has one partition), else 1. */
	partitionCount(topic: string, options: { signal: AbortSignal }): Promise<number> {
		return this.steps.partitionCount.perform(() => this.count(topic), options.signal)
	}

	close(options: { signal: AbortSignal }): Promise<void> {
		return this.steps.close.perform(() => {
			this.closed = true
		}, options.signal)
	}

	/** Records sent to `topic` outside transactions or in committed ones. */
	committedRecords(topic: string): SentRecord[] {
		return this.sent.filter(
			r => r.topic === topic && (r.transaction === undefined || r.transaction.state === 'committed')
		)
	}

	/** A send, in `transaction` if given. */
	produce(topic: string, message: ProducedMessage, transaction?: FakeTransaction): Promise<SendResult> {
		const record = { topic, message, transaction }
		this.sent.push(record)
		transaction?.sent.push(record)
		return this.steps.send.perform(() => this.ack(topic, message, transaction))
	}

	private ack(topic: string, message: ProducedMessage, transaction: FakeTransaction | undefined): SendResult {
		transaction?.expectOpen()
		const count = this.count(topic)
		const partition = message.partition ?? (count === 1 ? 0 : defaultPartition(message.key, count))
		const timestamp = message.timestamp ?? new Date(0)
		if (!this.cluster) {
			const key = tpKey({ topic, partition })
			const offset = this.counters.get(key) ?? 0n
			this.counters.set(key, offset + 1n)
			return { topic, partition, offset, timestamp }
		}
		const log = this.cluster.log(topic, partition)
		const offset = log.append(
			{
				key: message.key,
				value: message.value as Buffer,
				timestamp: BigInt(timestamp.getTime()),
				headers: message.headers ?? {},
			},
			transaction
		)
		transaction?.wrote(log)
		return { topic, partition, offset, timestamp }
	}

	private count(topic: string): number {
		return this.cluster?.topic(topic).partitions.length ?? 1
	}
}

/**
 * An open transaction (the client's handle, C11). Its effects follow the answered steps: offsets sent are kept
 * (`offsets`, `group`); a commit makes its records visible to read_committed and its offsets the group's committed
 * offsets in the cluster; an abort hides its records. Either ends it: a later call is InvalidTxnStateError.
 */
export class FakeTransaction implements TransactionPort {
	state: 'open' | 'committed' | 'aborted' = 'open'
	/** Every call in order: 'send', 'sendOffsets', 'commit', 'abort'. */
	readonly calls: string[] = []
	readonly sent: SentRecord[] = []
	/** The offsets sent, the last per partition. */
	offsets: OffsetAndMetadata[] = []
	group: ConsumerGroupMetadata | undefined
	private readonly logs = new Set<PartitionLog>()

	constructor(private readonly producer: FakeProducer) {}

	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		this.calls.push('send')
		return this.producer.produce(topic, message, this)
	}

	sendOffsets(params: {
		consumerGroupMetadata: ConsumerGroupMetadata
		offsets: readonly OffsetAndMetadata[]
	}): Promise<void> {
		this.calls.push('sendOffsets')
		return this.producer.steps.sendOffsets.perform(() => {
			this.expectOpen()
			this.group = params.consumerGroupMetadata
			const sent = new Set(params.offsets.map(tpKey))
			this.offsets = [...this.offsets.filter(o => !sent.has(tpKey(o))), ...params.offsets]
		})
	}

	commit(options: { signal: AbortSignal }): Promise<void> {
		this.calls.push('commit')
		return this.producer.steps.commit.perform(() => this.end('committed'), options.signal)
	}

	abort(options: { signal: AbortSignal }): Promise<void> {
		this.calls.push('abort')
		return this.producer.steps.abort.perform(() => this.end('aborted'), options.signal)
	}

	expectOpen(): void {
		if (this.state !== 'open') {
			throw new InvalidTxnStateError(TRANSACTIONAL_ID, this.state, ['open'])
		}
	}

	/** Its records went to `log`: the log gets its marker when it ends. */
	wrote(log: PartitionLog): void {
		this.logs.add(log)
	}

	private end(state: 'committed' | 'aborted'): void {
		this.expectOpen()
		this.state = state
		for (const log of this.logs) log.appendMarker(this)
		const cluster = this.producer.cluster
		if (state === 'committed' && cluster && this.group) {
			const committed = cluster.offsets(this.group.groupId)
			for (const { topic, partition, offset, metadata } of this.offsets) {
				committed.set(tpKey({ topic, partition }), { offset, metadata })
			}
		}
	}
}
