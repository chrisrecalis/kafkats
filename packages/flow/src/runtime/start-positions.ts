import { ErrorCode, KafkaError, type KafkaClient } from '@kafkats/client'
import type { WorkerContext } from '@/processors/index.js'
import type { AppStatus } from '@/runtime/status.js'
import type { TopicPartition } from '@/runtime/restoration.js'
import type { Topology } from '@/runtime/topology.js'

/**
 * Settles where newly assigned partitions without a committed offset start.
 *
 * Internal (repartition / through) topics are read from the beginning: with the consumer
 * default ('latest'), records written before this worker resolved its position would be lost.
 *
 * Source topics read from 'latest' are pinned to the position this process first started at.
 * Otherwise a worker that restarts (or gets the partition back) before its first commit would
 * re-resolve 'latest' and silently skip everything it had read but not committed.
 */
export class StartPositions {
	/**
	 * Where this process started reading source partitions that had no committed offset under the
	 * 'latest' reset policy. Until the first commit lands, a worker restart or rebalance resumes
	 * from here instead of jumping to the (new) end of the topic and skipping records.
	 * Shared by all workers: a partition can move between workers of this process.
	 */
	private readonly initialPositions = new Map<string, bigint>()

	constructor(
		private readonly client: KafkaClient,
		private readonly groupId: string,
		private readonly topology: Topology,
		private readonly status: AppStatus
	) {}

	async settle(
		worker: WorkerContext,
		partitions: TopicPartition[],
		startPositions: Map<string, bigint>
	): Promise<void> {
		const topology = this.topology
		const policy = topology.resolveOffsetReset() ?? 'latest'
		// With 'earliest' (and 'none') the start position of a source partition is already stable.
		const relevant = partitions.filter(tp => topology.isInternalTopic(tp.topic) || policy === 'latest')
		if (relevant.length === 0 || !this.status.clusterAvailable) return

		const byTopic = new Map<string, number[]>()
		for (const tp of relevant) {
			byTopic.set(tp.topic, [...(byTopic.get(tp.topic) ?? []), tp.partition])
		}

		const groupId = this.groupId
		const topics = [...byTopic].map(([name, parts]) => ({
			name,
			partitions: parts.map(partitionIndex => ({ partitionIndex })),
		}))
		// Offsets of an in-flight transaction (another instance committing) are reported as
		// unstable; wait for them rather than guessing.
		const deadline = Date.now() + 30_000
		let response
		for (;;) {
			try {
				const coordinator = await this.client.cluster.getCoordinator('GROUP', groupId)
				response = await coordinator.offsetFetch({ groupId, topics, requireStable: true })
				const code =
					response.errorCode !== ErrorCode.None
						? response.errorCode
						: response.topics.flatMap(t => t.partitions).find(p => p.errorCode !== ErrorCode.None)
								?.errorCode
				if (code === undefined) break
				throw new KafkaError(`OffsetFetch failed for group ${groupId}`, code)
			} catch (err) {
				const retriable = err instanceof KafkaError && err.retriable
				if (!retriable || Date.now() > deadline || this.status.closing) throw err
				this.client.cluster.invalidateCoordinator('GROUP', groupId)
				await new Promise(resolve => setTimeout(resolve, 200))
			}
		}

		const admin = this.client.admin()
		for (const topic of response.topics) {
			const uncommitted: number[] = []
			for (const partition of topic.partitions) {
				if (partition.committedOffset < 0n) uncommitted.push(partition.partitionIndex)
			}
			if (uncommitted.length === 0) continue
			if (topology.isInternalTopic(topic.name)) {
				const earliest = await admin.fetchTopicOffsets(topic.name, uncommitted, 'earliest')
				for (const partition of uncommitted) {
					if (!worker.assignedPartitions.has(`${topic.name}:${partition}`)) continue
					worker.consumer.seek(topic.name, partition, earliest.get(partition) ?? 0n)
				}
				continue
			}
			// Source topic read from 'latest': the first position this process started the partition
			// at stays its start position until an offset is committed.
			for (const partition of uncommitted) {
				const key = `${topic.name}:${partition}`
				if (!worker.assignedPartitions.has(`${topic.name}:${partition}`)) continue
				const pinned = this.initialPositions.get(key)
				const resolved = startPositions.get(key)
				if (pinned === undefined) {
					if (resolved !== undefined) this.initialPositions.set(key, resolved)
				} else if (pinned !== resolved) {
					worker.consumer.seek(topic.name, partition, pinned)
				}
			}
		}
	}

	/** Forget all pinned positions (on shutdown). */
	clear(): void {
		this.initialPositions.clear()
	}
}
