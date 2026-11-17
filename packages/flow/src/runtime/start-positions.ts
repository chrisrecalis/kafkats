/**
 * Start positions of input partitions without a committed offset (4.7.4, runtime§6.40). The client put each one where
 * its topic's reset policy says (repartition and through topics: 'earliest', KF16); a task's restore pipeline settles
 * them once its stores are restored, outside exclusive (CR8) and before its partitions resume.
 *
 * A partition read from 'latest' stays at the position this process first started it at (its pin) until an offset is
 * committed: a rebalance or a recovery before the first commit would otherwise resolve the end of the log again and
 * skip what arrived meanwhile. The pinned position is committed too (best effort), so that a restart of the process
 * resumes from it as well. An 'earliest' position needs neither: resolving it again reads the same records.
 */
import type { Logger, TopicPartition } from '@kafkats/client'
import { encodeCommitMetadata, ProcessorMetadataImpl } from '@/processor/metadata.js'
import { errorMessage } from '@/runtime/errors.js'
import type { GroupConsumerPort, OffsetAndMetadata } from '@/runtime/io.js'
import type { StartPositionSettler } from '@/runtime/restore-pipelines.js'
import { tpKey } from '@/runtime/task-id.js'
import type { StreamTask } from '@/runtime/task.js'
import type { ResetPolicy } from '@/topology/nodes.js'

/** The commit metadata of a start position: no partition time, no processor metadata (it seeds nothing, 4.8.5). */
const START_METADATA = encodeCommitMetadata(-1, new ProcessorMetadataImpl())

export interface StartPositionsDeps {
	/** The reset policy of an input topic (the source's, else the app default; repartition and through: 'earliest'). */
	readonly policy: (topic: string) => ResetPolicy
	readonly logger: Logger
}

type Consumer = Pick<GroupConsumerPort, 'seek' | 'commit'>

/** One per start (runtime§6.59): the pins outlive group memberships, not the start. */
export class StartPositions {
	/** Per `topic:partition`: where this process first started a 'latest' partition that had no commit. */
	private readonly pins = new Map<string, bigint>()

	constructor(private readonly deps: StartPositionsDeps) {}

	/** The settler of one run: it seeks and commits through that run's consumer. */
	settler(consumer: Consumer): StartPositionSettler {
		return { settle: (task, signal) => this.settle(task, consumer, signal) }
	}

	/**
	 * The start positions of the task's partitions without a commit, which become their last committed offsets (CR13).
	 * A 'latest' partition goes back to its pin (the seek drops what the consumer fetched from elsewhere, C5), and its
	 * position is committed; a commit that fails or outlasts `signal` is logged, and the pin keeps the position here.
	 */
	async settle(
		task: Pick<StreamTask, 'inputPartitions' | 'starts'>,
		consumer: Consumer,
		signal: AbortSignal
	): Promise<ReadonlyMap<string, bigint>> {
		signal.throwIfAborted()
		const positions = new Map<string, bigint>()
		const pinned: OffsetAndMetadata[] = []
		for (const tp of task.inputPartitions) {
			const key = tpKey(tp)
			const start = task.starts.get(key)
			if (start === undefined) {
				continue
			}
			if (start.committed !== null) {
				// Committed: the group knows where the partition stands, the pin is done.
				this.pins.delete(key)
				continue
			}
			if (this.deps.policy(tp.topic) !== 'latest') {
				positions.set(key, start.resolved)
				continue
			}
			const position = this.pin(tp, start.resolved, consumer)
			positions.set(key, position)
			pinned.push({ topic: tp.topic, partition: tp.partition, offset: position, metadata: START_METADATA })
		}
		if (pinned.length > 0) {
			await consumer.commit(pinned, { signal }).catch((error: unknown) => {
				this.deps.logger.warn('committing start positions failed: they are kept in this process only', {
					partitions: pinned.map(tpKey),
					error: errorMessage(error),
				})
			})
		}
		return positions
	}

	/** The pin of the partition: the first position it got here; the consumer moves back to it if it resolved another. */
	private pin(tp: TopicPartition, resolved: bigint, consumer: Consumer): bigint {
		const key = tpKey(tp)
		const pin = this.pins.get(key)
		if (pin === undefined) {
			this.pins.set(key, resolved)
			return resolved
		}
		if (pin !== resolved) {
			consumer.seek(tp, pin)
		}
		return pin
	}
}
