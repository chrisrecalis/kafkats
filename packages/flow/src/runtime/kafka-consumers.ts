/**
 * The consumer ports over @kafkats/client (3.6): the group consumer of a Run, and the restore and global-table
 * consumers (manual assignment, no commits). A port's signal is the client's `signal` option where the client takes
 * one, else it races the call (`raceSignal`).
 */
import type {
	Consumer,
	ConsumerConfig,
	ConsumerGroupMetadata,
	KafkaClient,
	Message,
	TopicPartition,
	TopicPartitionOffset,
} from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type {
	CommittedOffset,
	GroupConsumerHooks,
	GroupConsumerPort,
	OffsetAndMetadata,
	RestoreConsumerPort,
} from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'

/** The group consumer's config without the rebalance hooks, which the adapter routes to those of its run. */
export type GroupConsumerConfig = Omit<
	ConsumerConfig,
	'onBeforeRebalance' | 'onPartitionsAssigned' | 'onPartitionsRevoked' | 'onPartitionsLost'
>

type ConsumerEvent = 'rebalancePending' | 'rebalanceComplete' | 'offsetOutOfRange' | 'offsetReset' | 'error'

/** Flow commits offsets itself (D3); the handlers only enqueue (group) or are awaited batch by batch (restore). */
const RUN_OPTIONS = { autoCommit: false, commitOffsets: false, partitionConcurrency: 1 } as const

/** The group member of one Run: subscribed to the topology's source topics, partitions assigned by `range`. */
export class KafkaGroupConsumer implements GroupConsumerPort {
	private readonly consumer: Consumer
	private hooks: GroupConsumerHooks | undefined
	private running: Promise<void> | undefined

	constructor(client: Pick<KafkaClient, 'consumer'>, config: GroupConsumerConfig) {
		// The client takes its hooks with the config; they call those of the run (set before the client runs any).
		this.consumer = client.consumer({
			...config,
			onPartitionsRevoked: partitions => this.hooks!.onPartitionsRevoked(partitions),
			onPartitionsAssigned: partitions => this.hooks!.onPartitionsAssigned(partitions),
			// The client invokes it synchronously from the loss event, and so does this one the run's hook (C4).
			onPartitionsLost: partitions => this.hooks!.onPartitionsLost(partitions),
		})
	}

	run(
		topics: readonly string[],
		onBatch: (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => void,
		hooks: GroupConsumerHooks
	): Promise<void> {
		this.hooks = hooks
		this.running = this.consumer.runBatch(
			[...topics],
			async (messages, { topic, partition }) => onBatch({ topic, partition }, messages),
			RUN_OPTIONS
		)
		return this.running
	}

	/** Resolves once the run ended (the client left the group, bounded by itself), or rejects when `signal` aborts. */
	stop({ signal }: { signal: AbortSignal }): Promise<void> {
		this.consumer.stop()
		return raceSignal(ended(this.running), signal)
	}

	/** A consumer that is stopping or stopped has no partitions: ignored then, as resume() and seek() are. */
	pause(tps: readonly TopicPartition[]): void {
		if (this.consumer.isRunning) this.consumer.pause([...tps])
	}

	resume(tps: readonly TopicPartition[]): void {
		if (this.consumer.isRunning) this.consumer.resume([...tps])
	}

	seek(tp: TopicPartition, offset: bigint): void {
		if (this.consumer.isRunning) this.consumer.seek(tp.topic, tp.partition, offset)
	}

	nextOffset(tp: TopicPartition): bigint | undefined {
		return this.consumer.nextOffset(tp.topic, tp.partition)
	}

	currentLag(tp: TopicPartition): bigint | undefined {
		return this.consumer.currentLag(tp.topic, tp.partition)
	}

	commit(offsets: readonly OffsetAndMetadata[], { signal }: { signal: AbortSignal }): Promise<void> {
		return this.consumer.commit(offsets, { signal })
	}

	committed(
		tps: readonly TopicPartition[],
		{ signal }: { signal: AbortSignal }
	): Promise<Map<string, CommittedOffset | null>> {
		return this.consumer.committed(tps, { signal })
	}

	groupMetadata(): ConsumerGroupMetadata | undefined {
		return this.consumer.groupMetadata()
	}

	get rebalancePending(): boolean {
		return this.consumer.rebalancePending
	}

	/** The client's events of these names carry what the port's listeners take. */
	on(event: ConsumerEvent, listener: (...args: never[]) => void): void {
		this.consumer.on(event, listener)
	}
}

/**
 * A restore or global-table consumer: manual assignment from explicit offsets, read_committed, reset policy 'none'
 * (an out-of-range position pauses its partition and is reported, C7), no commits. One run.
 */
export class KafkaRestoreConsumer implements RestoreConsumerPort {
	private readonly consumer: Consumer
	private readonly assignment: TopicPartitionOffset[] = []
	private running: Promise<void> | undefined

	constructor(client: Pick<KafkaClient, 'consumer'>, config: ConsumerConfig) {
		this.consumer = client.consumer(config)
	}

	/** Before run(): the client takes a manual assignment when its run starts. */
	assign(partitions: readonly { topic: string; partition: number; offset: bigint }[]): void {
		if (this.running) {
			throw new IllegalStateError('a restore consumer is assigned its partitions before it runs')
		}
		for (const { topic, partition, offset } of partitions) this.assignment.push({ topic, partition, offset })
	}

	run(
		onBatch: (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => Promise<void>,
		onOutOfRange: (tp: TopicPartition, offset: bigint) => void
	): Promise<void> {
		this.consumer.on('offsetOutOfRange', ({ topic, partition, offset }) =>
			onOutOfRange({ topic, partition }, offset)
		)
		const topics = [...new Set(this.assignment.map(({ topic }) => topic))]
		this.running = this.consumer.runBatch(
			topics,
			(messages, { topic, partition }) => onBatch({ topic, partition }, messages),
			{ ...RUN_OPTIONS, assignment: this.assignment }
		)
		return this.running
	}

	pause(tps: readonly TopicPartition[]): void {
		if (this.consumer.isRunning) this.consumer.pause([...tps])
	}

	/** Java's position: the offset of the next record to be delivered (records fetched but not delivered excluded). */
	position(tp: TopicPartition): bigint | undefined {
		return this.consumer.nextOffset(tp.topic, tp.partition)
	}

	stop({ signal }: { signal: AbortSignal }): Promise<void> {
		this.consumer.stop()
		return raceSignal(ended(this.running), signal)
	}
}

/** Resolves once a run settled, however it ended (its owner handles its failure). */
function ended(run: Promise<void> | undefined): Promise<void> {
	return (
		run?.then(
			() => {},
			() => {}
		) ?? Promise.resolve()
	)
}
