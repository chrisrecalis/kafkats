/** The tasks an eager assignment asks for (4.1.4, 4.3.2 steps 2-3), verified against the topology (D9). */
import type { TopicPartition } from '@kafkats/client'
import { AssignmentError, TaskMigratedError } from '@/api/errors.js'
import { ChangelogPartitionMismatchError } from '@/changelog/errors.js'
import type { TaskId } from '@/processor/api.js'
import type { AssignedPartition } from '@/runtime/io.js'
import { TaskIdRegistry, tpKey } from '@/runtime/task-id.js'
import type { PartitionStart } from '@/runtime/task.js'
import type { BuiltTopology, Subtopology } from '@/topology/built-topology.js'
import type { PartitionCounts } from '@/topology/nodes.js'

export interface PlannedTask {
	readonly subtopology: Subtopology
	/** In the subtopology's source topic order (the order of the task's queues). */
	readonly partitions: readonly TopicPartition[]
	/** The assignment payload per partition (`tpKey`). */
	readonly starts: ReadonlyMap<string, PartitionStart>
}

/**
 * The assignment's tasks in TaskId order. Throws when no task of it may run: an assigned topic the topology does not
 * read (AssignmentError, fatal: the instances run different topologies), a source topic of a stateful task that grew
 * since start (ChangelogPartitionMismatchError, fatal, CR16), or a stateful task with only some of its partitions
 * (TaskMigratedError: leave, back off, rejoin). Stateless tasks accept partial sets (R9).
 */
export function planAssignment(
	assigned: readonly AssignedPartition[],
	topology: BuiltTopology,
	counts: PartitionCounts,
	ids: TaskIdRegistry
): Map<TaskId, PlannedTask> {
	const grouped = new Map<TaskId, { subtopology: Subtopology; partitions: Map<string, AssignedPartition> }>()
	for (const tp of assigned) {
		const subtopology = topology.subtopologyOf(tp.topic)
		if (subtopology === undefined) {
			throw new AssignmentError(
				`Assigned topic '${tp.topic}' is not read by this topology: the instances run different topologies`
			)
		}
		const id = ids.get(subtopology.id, tp.partition)
		const group = grouped.get(id) ?? { subtopology, partitions: new Map() }
		group.partitions.set(tpKey(tp), tp)
		grouped.set(id, group)
	}
	const plan = new Map<TaskId, PlannedTask>()
	for (const id of [...grouped.keys()].sort(TaskIdRegistry.compare)) {
		const { subtopology, partitions } = grouped.get(id)!
		plan.set(id, {
			subtopology,
			partitions: subtopology.stateful
				? statefulPartitions(id, subtopology, partitions, topology, counts)
				: [...subtopology.sourceTopics]
						.map(topic => ({ topic, partition: id.partition }))
						.filter(tp => partitions.has(tpKey(tp))),
			starts: new Map(
				[...partitions].map(([key, tp]) => [key, { committed: tp.committed, resolved: tp.offset }])
			),
		})
	}
	return plan
}

/** A stateful task runs only with every partition of its state, all within the partition counts of start. */
function statefulPartitions(
	id: TaskId,
	subtopology: Subtopology,
	assigned: ReadonlyMap<string, TopicPartition>,
	topology: BuiltTopology,
	counts: PartitionCounts
): TopicPartition[] {
	for (const tp of assigned.values()) {
		const count = counts.get(tp.topic)
		if (count !== undefined && tp.partition >= count) {
			const changelog = subtopology.changelogTopics.values().next().value ?? tp.topic
			throw new ChangelogPartitionMismatchError(changelog, tp.partition + 1, count, [tp.topic])
		}
	}
	const expected = topology.inputPartitions(subtopology, id.partition, counts)
	if (expected.length !== assigned.size || expected.some(tp => !assigned.has(tpKey(tp)))) {
		const keys = [...assigned.keys()].join(', ')
		throw new TaskMigratedError(`partial assignment of stateful task ${id.key}: only ${keys}`, { taskId: id })
	}
	return expected
}
