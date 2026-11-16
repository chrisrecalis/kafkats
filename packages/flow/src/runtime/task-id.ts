import type { TopicPartition } from '@kafkats/client'
import type { TaskId } from '@/processor/api.js'

/** `"<topic>:<partition>"`: the key of a topic-partition in maps. */
export function tpKey(tp: TopicPartition): string {
	return `${tp.topic}:${tp.partition}`
}

/**
 * The only place TaskIds are constructed: one frozen object per `(subtopology, partition)`, so ids compare with `===`
 * and work as Map keys. One registry per built topology.
 */
export class TaskIdRegistry {
	private readonly ids = new Map<string, TaskId>()

	get(subtopology: number, partition: number): TaskId {
		const key = `${subtopology}_${partition}`
		let id = this.ids.get(key)
		if (!id) {
			id = Object.freeze({ subtopology, partition, key })
			this.ids.set(key, id)
		}
		return id
	}

	/** By subtopology, then partition (Kafka Streams' TaskId order). */
	static compare(a: TaskId, b: TaskId): number {
		return a.subtopology - b.subtopology || a.partition - b.partition
	}
}
