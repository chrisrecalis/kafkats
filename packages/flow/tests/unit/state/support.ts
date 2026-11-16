/** Fixtures of the state layer unit tests. */
import { noopLogger } from '@kafkats/client'
import type { ChangelogDefaults } from '@/changelog/config.js'
import { codec } from '@/codec.js'
import type { ProcessorMetadata, TaskId } from '@/processor/api.js'
import type { CollectedRecord, SendKind, TaskOutput } from '@/processor/output.js'
import type { ChangelogCheckpointStore, StateStoreProvider } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'

export const APP = 'app'
export const DEFAULTS: ChangelogDefaults = { applicationId: APP }
export const str = codec.string()

export function taskId(subtopology: number, partition: number): TaskId {
	return Object.freeze({ subtopology, partition, key: `${subtopology}_${partition}` })
}

/** A TaskOutput that records what is sent. */
export class RecordingOutput implements TaskOutput {
	readonly sent: { kind: SendKind; record: CollectedRecord }[] = []

	send(kind: SendKind, record: CollectedRecord): void {
		this.sent.push({ kind, record })
	}

	partitionCount(): number | undefined {
		return undefined
	}

	loadPartitionCount(): Promise<number> {
		return Promise.reject(new Error('not used by state tests'))
	}

	/** The changelog records sent, decoded with string codecs (`null` for a tombstone). */
	changelog(): { topic: string; partition?: number; key: string; value: string | null }[] {
		return this.sent
			.filter(sent => sent.kind === 'changelog')
			.map(({ record }) => ({
				topic: record.topic,
				partition: record.partition,
				key: record.key!.toString(),
				value: record.value === null ? null : record.value.toString(),
			}))
	}
}

export function metadata(): ProcessorMetadata {
	const values = new Map<string, number>()
	return { get: key => values.get(key), put: (key, value) => void values.set(key, value) }
}

/** A durable checkpoint store over a Map, with the flushes it saw. */
export function checkpointStore(): ChangelogCheckpointStore & { values: Map<string, bigint>; flushes: number } {
	const values = new Map<string, bigint>()
	const store = {
		values,
		flushes: 0,
		get: (topic: string, partition: number) => Promise.resolve(values.get(`${topic}:${partition}`)),
		set: (topic: string, partition: number, offset: bigint) => {
			values.set(`${topic}:${partition}`, offset)
			return Promise.resolve()
		},
		flush: () => {
			store.flushes += 1
			return Promise.resolve()
		},
	}
	return store
}

export function directory(options: { provider?: StateStoreProvider; checkpoints?: Checkpoints } = {}): {
	directory: StateDirectory
	provider: StateStoreProvider
	checkpoints: Checkpoints
} {
	const provider = options.provider ?? new InMemoryStateStoreProvider()
	const checkpoints = options.checkpoints ?? new Checkpoints(undefined, () => {})
	return { directory: new StateDirectory(APP, provider, checkpoints, noopLogger), provider, checkpoints }
}

export async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
	const out: T[] = []
	for await (const item of iterable) out.push(item)
	return out
}
