import { IllegalStateError } from '@/api/errors.js'
import { ChangelogWriter, windowedKeyCodec } from '@/changelog/writer.js'
import type { Codec } from '@/codec.js'
import type { ProcessorMetadata, TaskId } from '@/processor/api.js'
import type { TaskOutput } from '@/processor/output.js'
import type { KeyValueStore, SessionStore, StateStore, WindowedKey, WindowStore } from '@/state.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import { storeHandle } from '@/state/handles.js'
import {
	ChangelogBackedKeyValueStore,
	ChangelogBackedSessionStore,
	ChangelogBackedWindowStore,
} from '@/state/logging.js'
import type { StateDirectory } from '@/state/state-directory.js'
import {
	TransactionalKeyValueStore,
	TransactionalSessionStore,
	TransactionalWindowStore,
	type TransactionalLayer,
} from '@/state/transactional.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/** One store of a task: its stack, from the provider store up to what processors get. */
export interface RegisteredStore {
	readonly builder: StoreBuilder
	/** The provider store, below every layer: what restoration writes. */
	readonly raw: StateStore
	/** What processors get. */
	readonly top: StateStore
	/** The exactly-once write buffer. */
	readonly buffer?: TransactionalLayer
	readonly changelog?: { readonly topic: string; readonly partition: number; readonly skipRestoration: boolean }
}

export interface TaskStateManagerDeps {
	readonly stateDirectory: StateDirectory
	readonly output: TaskOutput
	readonly eos: boolean
	readonly checkpoints: Checkpoints
	/** The task's processor metadata (one object per task, updated in place): store clocks persist into it. */
	readonly metadata: ProcessorMetadata
}

type KeyValue = KeyValueStore<unknown, unknown>
type Window = WindowStore<unknown, unknown>
type Session = SessionStore<unknown, unknown>
type Writer = ChangelogWriter<unknown, unknown>
type WindowedWriter = ChangelogWriter<WindowedKey<unknown>, unknown>

/** How the layers of each kind of store are made. */
interface Layers {
	transactional(raw: StateStore, builder: StoreBuilder): StateStore & TransactionalLayer
	logged(inner: StateStore, writer: Writer): StateStore
	/** The key codec of changelog records. */
	changelogKey(builder: StoreBuilder): Codec<unknown>
}

const LAYERS: Record<StoreBuilder['kind'], Layers> = {
	'key-value': {
		transactional: (raw, builder) =>
			new TransactionalKeyValueStore(raw as KeyValue, builder.keyCodec, builder.valueCodec),
		logged: (inner, writer) => new ChangelogBackedKeyValueStore(inner as KeyValue, writer),
		changelogKey: builder => builder.keyCodec,
	},
	window: {
		transactional: (raw, builder) =>
			new TransactionalWindowStore(raw as Window, builder.keyCodec, builder.valueCodec, retention(builder)),
		logged: (inner, writer) => new ChangelogBackedWindowStore(inner as Window, writer as WindowedWriter),
		changelogKey: builder => windowedKeyCodec(builder.keyCodec) as Codec<unknown>,
	},
	session: {
		transactional: (raw, builder) =>
			new TransactionalSessionStore(raw as Session, builder.keyCodec, builder.valueCodec, retention(builder)),
		logged: (inner, writer) => new ChangelogBackedSessionStore(inner as Session, writer as WindowedWriter),
		changelogKey: builder => windowedKeyCodec(builder.keyCodec) as Codec<unknown>,
	},
}

function retention(builder: StoreBuilder): number {
	if (builder.retentionMs === undefined) {
		throw new IllegalStateError(`The ${builder.kind} store '${builder.name}' has no retention`)
	}
	return builder.retentionMs
}

/**
 * The stores of one task (Kafka Streams' ProcessorStateManager). For every store of its subtopology it leases the
 * provider store of the task's partition and stacks on it: under exactly-once a write buffer, then changelog logging
 * to the changelog partition of the task's partition, then a handle that rejects writes once the stack is closed.
 */
export class TaskStateManager {
	private registered: RegisteredStore[] = []
	private readonly byName = new Map<string, RegisteredStore>()
	/** The stores leased from the state directory. */
	private leased: string[] = []
	private closed = false

	constructor(
		private readonly task: TaskId,
		private readonly partition: number,
		private readonly builders: readonly StoreBuilder[],
		private readonly deps: TaskStateManagerDeps
	) {}

	get stores(): readonly RegisteredStore[] {
		return this.registered
	}

	/** Whether a write buffer holds writes not applied yet. */
	get dirty(): boolean {
		return this.registered.some(store => (store.buffer?.size ?? 0) > 0)
	}

	/** Leases the task's provider stores and builds their stacks. On failure the stores leased so far are released. */
	async register(): Promise<void> {
		try {
			for (const builder of this.builders) {
				const raw = await this.deps.stateDirectory.lease(this.task, builder, this.partition)
				this.leased.push(builder.name)
				const store = this.stack(builder, raw)
				this.registered.push(store)
				this.byName.set(builder.name, store)
			}
		} catch (error) {
			await this.release()
			throw error
		}
	}

	/** The top of a store's stack. */
	store(name: string): StateStore {
		const store = this.byName.get(name)
		if (store === undefined) {
			throw new IllegalStateError(`Task ${this.task.key} has no state store '${name}'`)
		}
		return store.top
	}

	/** Exactly-once: applies the write buffers of every store; all of them settle, then the first failure is thrown. */
	async commitBuffers(): Promise<void> {
		const results = await Promise.allSettled(this.buffers().map(buffer => buffer.commitBuffer()))
		for (const result of results) {
			if (result.status === 'rejected') throw result.reason
		}
	}

	discardBuffers(): void {
		for (const buffer of this.buffers()) {
			buffer.discardBuffer()
		}
	}

	/**
	 * Checkpoints the changelog partitions written since the last commit. `positions`: the offset of the last
	 * acknowledged write per changelog partition (`<topic>:<partition>`). Under at-least-once the checkpoint is the
	 * next offset; under exactly-once one more, past the transaction's commit marker (a changelog partition has a
	 * single writer, so nothing else sits in between). The caller flushes the checkpoints once per commit.
	 */
	async checkpoint(positions: ReadonlyMap<string, bigint>, eos: boolean): Promise<void> {
		const next = eos ? 2n : 1n
		const writes: Promise<void>[] = []
		for (const { changelog } of this.registered) {
			if (changelog === undefined) continue
			const last = positions.get(`${changelog.topic}:${changelog.partition}`)
			if (last !== undefined)
				writes.push(this.deps.checkpoints.set(changelog.topic, changelog.partition, last + next))
		}
		await Promise.all(writes)
	}

	/** From now on, writes through the stack reject with IllegalStateError. */
	closeStack(): void {
		this.closed = true
	}

	/** Returns the provider stores to the state directory (`wipe`: invalidated and cleared first). */
	async release(options?: { wipe?: boolean }): Promise<void> {
		const leased = this.leased
		this.leased = []
		this.registered = []
		this.byName.clear()
		const results = await Promise.allSettled(
			leased.map(store => this.deps.stateDirectory.release(this.task, store, this.partition, options))
		)
		for (const result of results) {
			if (result.status === 'rejected') throw result.reason
		}
	}

	private buffers(): TransactionalLayer[] {
		return this.registered.flatMap(store => (store.buffer ? [store.buffer] : []))
	}

	private stack(builder: StoreBuilder, raw: StateStore): RegisteredStore {
		const layers = LAYERS[builder.kind]
		const buffer = this.deps.eos ? layers.transactional(raw, builder) : undefined
		let layer: StateStore = buffer ?? raw
		let changelog: RegisteredStore['changelog']
		if (builder.logging.enabled) {
			const { topic, skipRestoration } = builder.logging
			const keyCodec = layers.changelogKey(builder)
			layer = layers.logged(
				layer,
				new ChangelogWriter(topic, this.partition, keyCodec, builder.valueCodec, this.deps.output)
			)
			changelog = { topic, partition: this.partition, skipRestoration }
		}
		const top = storeHandle(layer, builder.kind, () => this.closed, `task ${this.task.key}`)
		return { builder, raw, top, buffer, changelog }
	}
}
