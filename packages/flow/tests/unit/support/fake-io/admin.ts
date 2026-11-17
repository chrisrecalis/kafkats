import { ErrorCode, KafkaProtocolError, type TopicPartition } from '@kafkats/client'
import type { AdminPort } from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'
import type { InternalTopicSpec, PartitionCounts } from '@/topology/nodes.js'
import type { FakeCluster, PartitionLog } from './cluster.js'
import { Steps } from './steps.js'

/** The admin port over the cluster; every step ends as the test scripts it (`steps`). */
export class FakeAdmin implements AdminPort {
	/** Every call in order: 'describeTopics a,b', 'createTopics x', 'listOffsets latest read_committed a:0', 'deleteRecords a:0@5'. */
	readonly calls: string[] = []
	readonly steps = {
		describeTopics: new Steps(),
		createTopics: new Steps(),
		listOffsets: new Steps(),
		deleteRecords: new Steps(),
	}

	constructor(readonly cluster: FakeCluster) {}

	/** Partition counts of the topics that exist. */
	describeTopics(topics: readonly string[], options: { signal: AbortSignal }): Promise<Map<string, number>> {
		this.calls.push(`describeTopics ${topics.join(',')}`)
		return this.steps.describeTopics.perform(() => {
			const counts = new Map<string, number>()
			for (const name of topics) {
				const topic = this.cluster.topics.get(name)
				if (topic) counts.set(name, topic.partitions.length)
			}
			return counts
		}, options.signal)
	}

	/** Creates the topics that do not exist (an existing one counts as created), with their configs. */
	createTopics(
		specs: readonly InternalTopicSpec[],
		counts: PartitionCounts,
		options: { signal: AbortSignal }
	): Promise<void> {
		this.calls.push(`createTopics ${specs.map(spec => spec.name).join(',')}`)
		return this.steps.createTopics.perform(() => {
			for (const spec of specs) {
				const partitions = spec.partitions ?? counts.get(spec.name)
				if (partitions === undefined) {
					throw new Error(`no partition count for '${spec.name}'`)
				}
				this.cluster.createTopic(spec.name, partitions, spec.configs)
			}
		}, options.signal)
	}

	/** By `topic:partition`: the log start, or the end (the last stable offset under read_committed). */
	listOffsets(
		tps: readonly TopicPartition[],
		spec: 'earliest' | 'latest',
		isolation: 'read_committed' | 'read_uncommitted',
		options: { signal: AbortSignal }
	): Promise<Map<string, bigint>> {
		this.calls.push(`listOffsets ${spec} ${isolation} ${tps.map(tpKey).join(' ')}`)
		return this.steps.listOffsets.perform(() => {
			const offsets = new Map<string, bigint>()
			for (const tp of tps) {
				const log = this.existing(tp)
				offsets.set(
					tpKey(tp),
					spec === 'earliest' ? log.start : isolation === 'read_committed' ? log.lastStable : log.end
				)
			}
			return offsets
		}, options.signal)
	}

	/** Moves the log start of each partition to its offset (beyond the end: OffsetOutOfRange). */
	deleteRecords(
		offsets: readonly { topic: string; partition: number; offset: bigint }[],
		options: { signal: AbortSignal }
	): Promise<void> {
		this.calls.push(`deleteRecords ${offsets.map(o => `${tpKey(o)}@${o.offset}`).join(' ')}`)
		return this.steps.deleteRecords.perform(() => {
			for (const { offset, ...tp } of offsets) this.existing(tp).deleteBefore(offset)
		}, options.signal)
	}

	private existing(tp: TopicPartition): PartitionLog {
		const log = this.cluster.topics.get(tp.topic)?.partitions[tp.partition]
		if (!log) {
			throw new KafkaProtocolError(ErrorCode.UnknownTopicOrPartition)
		}
		return log
	}
}
