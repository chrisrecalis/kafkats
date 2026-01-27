import { Processor, type AnyProcessor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValue } from '@/types.js'

export class MapNode<K, V, K2, V2> extends Processor<K, V, K2, V2> {
	constructor(private readonly fn: (key: K | null, value: V | null) => KeyValue<K2, V2>) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K2, V2> {
		void worker
		return new MapNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const [nextKey, nextValue] = this.fn(record.key, record.value)
		const next: StreamRecord<K2, V2> = {
			...record,
			key: nextKey,
			value: nextValue,
		}
		await this.forward(next)
	}
}

export class MapValuesNode<K, V, V2> extends Processor<K, V, K, V2> {
	constructor(private readonly fn: (value: V | null) => V2 | null) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V2> {
		void worker
		return new MapValuesNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const next: StreamRecord<K, V2> = {
			...record,
			value: this.fn(record.value),
		}
		await this.forward(next)
	}
}

export class FlatMapValuesNode<K, V, V2> extends Processor<K, V, K, V2> {
	constructor(private readonly fn: (value: V | null) => Iterable<V2>) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V2> {
		void worker
		return new FlatMapValuesNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		for (const value of this.fn(record.value)) {
			const next: StreamRecord<K, V2> = {
				...record,
				value,
			}
			await this.forward(next)
		}
	}
}

export class FilterNode<K, V> extends Processor<K, V, K, V> {
	constructor(private readonly fn: (key: K | null, value: V | null) => boolean) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new FilterNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		if (this.fn(record.key, record.value)) {
			await this.forward(record)
		}
	}
}

export class PeekNode<K, V> extends Processor<K, V, K, V> {
	constructor(private readonly fn: (key: K | null, value: V | null) => void) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new PeekNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		this.fn(record.key, record.value)
		await this.forward(record)
	}
}

export class SelectKeyNode<K, V, K2> extends Processor<K, V, K2, V> {
	constructor(private readonly fn: (value: V | null, key: K | null) => K2) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K2, V> {
		void worker
		return new SelectKeyNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const next: StreamRecord<K2, V> = {
			...record,
			key: this.fn(record.value, record.key),
		}
		await this.forward(next)
	}
}

export class BranchNode<K, V> extends Processor<K, V, K, V> {
	private readonly branches: Array<{
		predicate: (key: K | null, value: V | null) => boolean
		node: Processor<K, V, unknown, unknown>
	}> = []

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new BranchNode<K, V>()
	}

	addBranch(predicate: (key: K | null, value: V | null) => boolean, node: Processor<K, V, unknown, unknown>): void {
		this.branches.push({ predicate, node })
	}

	protected override cloneEdges(
		cloned: Processor<K, V, K, V>,
		worker: WorkerContext,
		map: Map<AnyProcessor, AnyProcessor>
	): void {
		const branchClone = cloned as BranchNode<K, V>
		for (const branch of this.branches) {
			const childClone = branch.node.cloneGraph(worker, map)
			branchClone.addBranch(branch.predicate, childClone)
		}
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		for (const branch of this.branches) {
			if (branch.predicate(record.key, record.value)) {
				await branch.node.process(record)
				return
			}
		}
	}
}
