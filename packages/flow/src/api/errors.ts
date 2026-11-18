/**
 * Every error class flow throws. The changelog errors (ChangelogPartitionMismatchError,
 * SourceTopicNotFoundError, CoPartitioningError) live in changelog/errors.ts.
 */
import type { TaskId } from '@/processor/api.js'

/** Invalid or removed configuration, thrown by flow(). */
export class FlowConfigError extends Error {
	override name = 'FlowConfigError'
}

/**
 * An invalid topology: thrown by the DSL call that makes the mistake (a missing codec, an illegal or duplicate name),
 * or by describe() and start(), which build the topology, for the rules spanning all of it.
 */
export class TopologyError extends Error {
	override name = 'TopologyError'
}

/** Base of the errors raised while the app runs. */
export class StreamsError extends Error {
	override name = 'StreamsError'
	/** The task the error belongs to, if any. */
	readonly taskId?: TaskId

	constructor(message: string, options?: { cause?: unknown; taskId?: TaskId }) {
		super(message, { cause: options?.cause })
		this.taskId = options?.taskId
	}
}

/** start() while close() is in flight; or use of a closed context or store, or an invalid task transition: a bug. */
export class IllegalStateError extends StreamsError {
	override name = 'IllegalStateError'
}

/** Fencing, lost partitions, a failed commit or a partial stateful assignment: all tasks close dirty and the app rejoins. */
export class TaskMigratedError extends StreamsError {
	override name = 'TaskMigratedError'
}

/** The local state or output of these tasks is unusable: they are revived in place. */
export class TaskCorruptedError extends StreamsError {
	override name = 'TaskCorruptedError'

	constructor(
		message: string,
		readonly tasks: readonly TaskId[],
		options?: { cause?: unknown }
	) {
		super(message, options)
	}
}

/** An exception handler answered 'fail', or threw. */
export class FailedProcessingError extends StreamsError {
	override name = 'FailedProcessingError'
}

/** A timestamp extractor returned an invalid timestamp. */
export class InvalidTimestampError extends StreamsError {
	override name = 'InvalidTimestampError'
}

/** An assigned topic is not part of the topology: instances run different topologies (fatal). */
export class AssignmentError extends StreamsError {
	override name = 'AssignmentError'
}

/**
 * Reset policy 'none' and no committed offset (its cause is the client's NoOffsetForPartitionError), or an out-of-range
 * position (fatal).
 */
export class NoOffsetResetError extends StreamsError {
	override name = 'NoOffsetResetError'
}
