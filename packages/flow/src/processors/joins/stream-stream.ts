import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { WindowStore } from '@/state.js'

/**
 * Processor node for Stream-Stream join (inner join).
 * Stores records in a window store and looks up matching records from the other stream.
 */
export class StreamStreamJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly joinWindowMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await myStore.put({ key, windowStart, windowEnd }, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}
	}
}

/**
 * Processor node for Stream-Stream left join.
 */
export class StreamStreamLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamLeftJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await myStore.put({ key, windowStart, windowEnd }, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		if (!hasMatch) {
			const joinedValue = this.joiner(value, null)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}
	}
}

/**
 * Processor node for Stream-Stream outer join.
 */
export class StreamStreamOuterJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1 | null, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamOuterJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await myStore.put({ key, windowStart, windowEnd }, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		if (!hasMatch) {
			const joinedValue = this.joiner(value, null)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}
	}
}
