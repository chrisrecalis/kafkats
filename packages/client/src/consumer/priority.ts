export interface SchedulerPartition {
	topic: string
	partition: number
	/** Estimated bytes buffered but not yet delivered. */
	bufferedBytes: number
	busy: boolean
}

export interface SchedulerState {
	partitions: SchedulerPartition[]
	/**
	 * A poll stops starting partitions once it has used this many (estimated) bytes. Equal to the
	 * consumer's `maxBytesPerPartition`, which also caps each partition's batch.
	 */
	byteBudget: number
	bufferCapacityBytes: number
	bufferedBytes: number
}

export interface ScheduleDecision {
	/**
	 * Partitions to deliver from, in order, each capped at `maxBytes` (and at `byteBudget`). Every batch
	 * holds at least one record, so a zero cap does not withhold a partition. Buffered partitions omitted
	 * here are still delivered FIFO after the planned entries while poll budget remains, so use `fetch`
	 * (not `drain`) to hold a topic back.
	 */
	drain: Array<{ topic: string; partition: number; maxBytes: number }>
	/** Partitions the background fetcher may request from next. Omitted partitions are not fetched. */
	fetch: Array<{ topic: string; partition: number }>
}

declare const topicBrand: unique symbol

/**
 * A topic scheduling policy for consumer fetch and delivery.
 *
 * `schedule()` can be called more often than records are drained (the background fetcher
 * calls it too), and a drain decision can be applied only partially. Fairness must therefore
 * be derived from `state`, not from schedule call counts. Do not share a stateful strategy
 * instance between concurrently running consumers.
 *
 * Fetch decisions currently gate whole partitions. A future per-entry `maxBytes` can provide
 * finer control over the bytes requested for each partition.
 */
export interface PriorityStrategy<T extends string = string> {
	schedule(state: SchedulerState): ScheduleDecision
	readonly topics?: readonly string[]
	readonly [topicBrand]?: T
}

export function strict<const T extends string>(options: { order: readonly T[] }): PriorityStrategy<T> {
	if (options.order.length === 0) throw new Error('strict priority order must not be empty')
	if (new Set(options.order).size !== options.order.length) {
		throw new Error('strict priority order must not contain duplicates')
	}

	const topics = [...options.order]
	const ranks = new Map<string, number>(topics.map((topic, index) => [topic, index]))
	const fallbackRank = topics.length

	return {
		topics,
		schedule(state) {
			const ranked = state.partitions
				.map((partition, index) => ({ partition, index, rank: ranks.get(partition.topic) ?? fallbackRank }))
				.sort((a, b) => a.rank - b.rank || a.index - b.index)

			const drain = ranked
				.filter(({ partition }) => !partition.busy && partition.bufferedBytes > 0)
				.map(({ partition }) => ({
					topic: partition.topic,
					partition: partition.partition,
					maxBytes: state.byteBudget,
				}))

			const bufferedByRank = Array<number>(fallbackRank + 1).fill(0)
			for (const { partition, rank } of ranked) {
				if (!partition.busy) bufferedByRank[rank]! += partition.bufferedBytes
			}

			let allowedRank = 0
			let higherBuffered = bufferedByRank[0]!
			while (allowedRank < fallbackRank && higherBuffered < state.byteBudget) {
				allowedRank++
				higherBuffered += bufferedByRank[allowedRank]!
			}

			const fetch = ranked
				.filter(({ rank }) => rank <= allowedRank)
				.map(({ partition }) => ({ topic: partition.topic, partition: partition.partition }))
			return { drain, fetch }
		},
	}
}

export function weighted<const T extends string>(options: {
	shares: Readonly<Record<T, number>>
}): PriorityStrategy<T> {
	const topics = Object.keys(options.shares) as T[]
	if (topics.length === 0) throw new Error('weighted priority shares must not be empty')
	for (const topic of topics) {
		const share = options.shares[topic]
		if (!Number.isFinite(share) || share <= 0) {
			throw new Error(`weighted priority share for topic "${topic}" must be finite and greater than zero`)
		}
	}

	const configuredShares = new Map<string, number>(topics.map(topic => [topic, options.shares[topic]]))
	const shareOf = (topic: string) => configuredShares.get(topic) ?? 1

	return {
		topics,
		schedule(state) {
			const groups = groupByTopic(state.partitions)
			const topicStates = Array.from(groups, ([topic, partitions]) => ({
				topic,
				partitions,
				share: shareOf(topic),
				demand: partitions.reduce(
					(total, partition) => total + (partition.busy ? 0 : partition.bufferedBytes),
					0
				),
				allocation: 0,
			}))
			let remaining = state.byteBudget
			let unsatisfied = topicStates.filter(topic => topic.demand > 0)

			while (remaining > 0 && unsatisfied.length > 0) {
				const totalShare = unsatisfied.reduce((sum, topic) => sum + topic.share, 0)
				const satisfied = unsatisfied.filter(
					topic => topic.demand <= Math.floor((remaining * topic.share) / totalShare)
				)
				if (satisfied.length === 0) break
				for (const topic of satisfied) {
					topic.allocation = topic.demand
					remaining -= topic.demand
				}
				unsatisfied = unsatisfied.filter(topic => !satisfied.includes(topic))
			}

			const totalShare = unsatisfied.reduce((sum, topic) => sum + topic.share, 0)
			for (const topic of unsatisfied) {
				topic.allocation = Math.floor((remaining * topic.share) / totalShare)
			}

			const drain: ScheduleDecision['drain'] = []
			for (const topic of [...topicStates].sort((a, b) => b.share - a.share)) {
				let allocation = topic.allocation
				for (const partition of topic.partitions) {
					if (partition.busy || allocation === 0) continue
					const maxBytes = Math.min(allocation, partition.bufferedBytes)
					if (maxBytes > 0) drain.push({ topic: topic.topic, partition: partition.partition, maxBytes })
					allocation -= maxBytes
				}
			}

			const presentShares = topicStates.reduce((sum, topic) => sum + topic.share, 0)
			const fetch: ScheduleDecision['fetch'] = []
			for (const topic of topicStates) {
				const bufferedBytes = topic.partitions.reduce((sum, partition) => sum + partition.bufferedBytes, 0)
				const byteLimit = (state.bufferCapacityBytes * topic.share) / presentShares
				if (bufferedBytes < byteLimit) {
					for (const partition of topic.partitions) {
						fetch.push({ topic: topic.topic, partition: partition.partition })
					}
				}
			}
			return { drain, fetch }
		},
	}
}

export function validatePriorityStrategy(strategy: PriorityStrategy, topics: readonly string[]): void {
	if (typeof strategy.schedule !== 'function') throw new Error('priority strategy schedule must be a function')
	const subscriptionTopics = new Set(topics)
	for (const topic of strategy.topics ?? []) {
		if (!subscriptionTopics.has(topic)) {
			throw new Error(`priority strategy references topic "${topic}" that is not in the subscription`)
		}
	}
}

function groupByTopic(partitions: SchedulerPartition[]): Map<string, SchedulerPartition[]> {
	const groups = new Map<string, SchedulerPartition[]>()
	for (const partition of partitions) {
		const group = groups.get(partition.topic)
		if (group) group.push(partition)
		else groups.set(partition.topic, [partition])
	}
	return groups
}
