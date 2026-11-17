/**
 * Stateless processors (KS KStreamFilter, KStreamMap, KStreamMapValues, KStreamFlatMap, KStreamPeek, KStreamBranch,
 * KTable toStream). Each is a plain function of the record that returns its forward's result, so a synchronous chain
 * allocates no promise. Callbacks see null keys and values (ksdsl§1.4); results keep the input timestamp and headers.
 */
import type { KeyValue, ValueMapper } from '@/api/kstream.js'
import type { Change } from '@/operators/change.js'
import type { ForwardResult, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'

type Process<KIn, VIn, KOut, VOut> = (
	context: ProcessorContext<KOut, VOut>,
	record: StreamRecord<KIn, VIn>
) => ForwardResult

/** A supplier of processors that run `process` with their own context (a new processor per call). */
function stateless<KIn, VIn, KOut, VOut>(
	process: Process<KIn, VIn, KOut, VOut>
): ProcessorSupplier<KIn, VIn, KOut, VOut> {
	return () => {
		let context: ProcessorContext<KOut, VOut>
		return {
			init: ctx => {
				context = ctx
			},
			process: record => process(context, record),
		}
	}
}

/** A ValueMapper as a function of key and value: mappers declaring two or more parameters get the key (6.20). */
export function withKey<K, V, VR>(mapper: ValueMapper<K, V, VR>): (key: K, value: V) => VR {
	return mapper.length >= 2
		? (mapper as (key: K, value: V) => VR)
		: (_key, value) => (mapper as (value: V) => VR)(value)
}

/** Forwards the records one after the other, each once the previous one's subtree settled. */
function forwardEach<K, V>(context: ProcessorContext<K, V>, records: Iterator<StreamRecord<K, V>>): ForwardResult {
	for (let next = records.next(); !next.done; next = records.next()) {
		const result = context.forward(next.value)
		if (result !== undefined) {
			return result.then(() => forwardEach(context, records))
		}
	}
}

function* mapped<KIn, VIn, KOut, VOut>(
	record: StreamRecord<KIn, VIn>,
	pairs: Iterable<KeyValue<KOut, VOut>>
): Generator<StreamRecord<KOut, VOut>> {
	for (const [key, value] of pairs) {
		yield new StreamRecord(key, value, record.timestamp, record.headers)
	}
}

function* withValues<K, VIn, VOut>(
	record: StreamRecord<K, VIn>,
	values: Iterable<VOut>
): Generator<StreamRecord<K, VOut>> {
	for (const value of values) {
		yield record.withValue(value)
	}
}

/** filter (`negate` false) and filterNot (true). */
export function filterProcessor<K, V>(
	predicate: (key: K, value: V) => boolean,
	negate: boolean
): ProcessorSupplier<K, V, K, V> {
	return stateless((context, record) =>
		predicate(record.key, record.value) !== negate ? context.forward(record) : undefined
	)
}

export function mapProcessor<K, V, KR, VR>(
	mapper: (key: K, value: V) => KeyValue<KR, VR>
): ProcessorSupplier<K, V, KR, VR> {
	return stateless((context, record) => {
		const [key, value] = mapper(record.key, record.value)
		return context.forward(new StreamRecord(key, value, record.timestamp, record.headers))
	})
}

export function mapValuesProcessor<K, V, VR>(mapper: ValueMapper<K, V, VR>): ProcessorSupplier<K, V, K, VR> {
	const map = withKey(mapper)
	return stateless((context, record) => context.forward(record.withValue(map(record.key, record.value))))
}

export function flatMapProcessor<K, V, KR, VR>(
	mapper: (key: K, value: V) => Iterable<KeyValue<KR, VR>>
): ProcessorSupplier<K, V, KR, VR> {
	return stateless((context, record) => forwardEach(context, mapped(record, mapper(record.key, record.value))))
}

export function flatMapValuesProcessor<K, V, VR>(
	mapper: ValueMapper<K, V, Iterable<VR>>
): ProcessorSupplier<K, V, K, VR> {
	const map = withKey(mapper)
	return stateless((context, record) => forwardEach(context, withValues(record, map(record.key, record.value))))
}

export function selectKeyProcessor<K, V, KR>(mapper: (key: K, value: V) => KR): ProcessorSupplier<K, V, KR, V> {
	return stateless((context, record) => context.forward(record.withKey(mapper(record.key, record.value))))
}

/** peek: the action, then the record goes on. */
export function peekProcessor<K, V>(action: (key: K, value: V) => void): ProcessorSupplier<K, V, K, V> {
	return stateless((context, record) => {
		action(record.key, record.value)
		return context.forward(record)
	})
}

/** foreach: terminal. */
export function foreachProcessor<K, V>(action: (key: K, value: V) => void): ProcessorSupplier<K, V, never, never> {
	return stateless((_context, record) => {
		action(record.key, record.value)
	})
}

/** Merge points and branch heads. */
export function passThroughProcessor<K, V>(): ProcessorSupplier<K, V, K, V> {
	return stateless((context, record) => context.forward(record))
}

/** A branch of a splitter: its records go to `child`. A branch without a predicate (a default branch) takes any record. */
export interface BranchRoute<K, V> {
	readonly child: string
	readonly predicate?: (key: K, value: V) => boolean
}

/**
 * The splitter of `split()` and `branch()` (KS KStreamBranch): each record goes to the child of the first matching
 * branch only; records no branch matches are dropped. `branches` may grow until the topology is built.
 */
export function branchProcessor<K, V>(branches: readonly BranchRoute<K, V>[]): ProcessorSupplier<K, V, K, V> {
	return stateless((context, record) => {
		for (const { child, predicate } of branches) {
			if (predicate === undefined || predicate(record.key, record.value)) {
				return context.forward(record, child)
			}
		}
	})
}

/** KTable.toStream: the new value of every update (old values never reach streams), re-keyed by `mapper` if given. */
export function toStreamProcessor<K, V, KR>(
	mapper?: (key: K, value: V | null) => KR
): ProcessorSupplier<K, Change<V>, K | KR, V | null> {
	return stateless((context, record) => {
		const value = record.value.newValue
		const key = mapper === undefined ? record.key : mapper(record.key, value)
		return context.forward(new StreamRecord<K | KR, V | null>(key, value, record.timestamp, record.headers))
	})
}
