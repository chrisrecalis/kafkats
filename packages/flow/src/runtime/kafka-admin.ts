/** The admin port over @kafkats/client (3.6, C12). Calls the client takes no signal for are raced with it. */
import { ErrorCode, KafkaProtocolError, type Admin, type TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { AdminPort } from '@/runtime/io.js'
import { raceSignal } from '@/runtime/signals.js'
import { tpKey } from '@/runtime/task-id.js'
import type { InternalTopicSpec, PartitionCounts } from '@/topology/nodes.js'

/** Kafka Streams' `replication.factor` default: the broker's `default.replication.factor`. */
const BROKER_DEFAULT_REPLICATION = -1

export class KafkaAdmin implements AdminPort {
	constructor(private readonly admin: Admin) {}

	/** The partition count of each topic that exists (one without partitions does not). */
	async describeTopics(topics: readonly string[], { signal }: { signal: AbortSignal }): Promise<Map<string, number>> {
		if (topics.length === 0) {
			return new Map()
		}
		const described = await raceSignal(this.admin.describeTopics([...topics]), signal)
		return new Map(
			described
				.filter(({ partitions }) => partitions.length > 0)
				.map(({ name, partitions }) => [name, partitions.length])
		)
	}

	/** A topic that exists already counts as created (the caller checks its partition count). */
	async createTopics(
		specs: readonly InternalTopicSpec[],
		counts: PartitionCounts,
		{ signal }: { signal: AbortSignal }
	): Promise<void> {
		const topics = specs.map(spec => {
			const numPartitions = spec.partitions ?? counts.get(spec.name)
			if (numPartitions === undefined) {
				throw new IllegalStateError(`No partition count for internal topic '${spec.name}'`)
			}
			const replicationFactor = spec.replicationFactor ?? BROKER_DEFAULT_REPLICATION
			return { name: spec.name, numPartitions, replicationFactor, configs: spec.configs }
		})
		if (topics.length === 0) {
			return
		}
		const results = await raceSignal(this.admin.createTopics(topics), signal)
		const failed = results.find(
			({ errorCode }) => errorCode !== ErrorCode.None && errorCode !== ErrorCode.TopicAlreadyExists
		)
		if (failed) {
			throw new KafkaProtocolError(
				failed.errorCode,
				`creating topic '${failed.name}': ${failed.errorMessage ?? ''}`
			)
		}
	}

	/** By `topic:partition`: the log start, or the end (the last stable offset under read_committed). */
	async listOffsets(
		tps: readonly TopicPartition[],
		spec: 'earliest' | 'latest',
		isolation: 'read_committed' | 'read_uncommitted',
		{ signal }: { signal: AbortSignal }
	): Promise<Map<string, bigint>> {
		const byTopic = new Map<string, number[]>()
		for (const { topic, partition } of tps) {
			byTopic.set(topic, [...(byTopic.get(topic) ?? []), partition])
		}
		const listed = await raceSignal(
			Promise.all(
				[...byTopic].map(async ([topic, partitions]) => {
					const offsets = await this.admin.fetchTopicOffsets(topic, partitions, spec, {
						isolationLevel: isolation,
					})
					return [...offsets].map(([partition, offset]) => [tpKey({ topic, partition }), offset] as const)
				})
			),
			signal
		)
		return new Map(listed.flat())
	}

	/** Rejects with the first partition's failure (the client reports them per partition). */
	async deleteRecords(
		offsets: readonly { topic: string; partition: number; offset: bigint }[],
		{ signal }: { signal: AbortSignal }
	): Promise<void> {
		const results = await this.admin.deleteRecords(offsets, { signal })
		const failure = results.find(result => result.error !== undefined)?.error
		if (failure) {
			throw failure
		}
	}
}
