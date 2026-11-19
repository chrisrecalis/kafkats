/** Builders of topology fixtures for the topology unit tests. */
import { buffer } from '@/codec.js'
import type { Processor, ProcessorSupplier } from '@/processor/api.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import type { GlobalStoreDef, NodeName, StoreBuilder } from '@/topology/nodes.js'

export const APP = 'app'

export function builder(): InternalTopologyBuilder {
	return new InternalTopologyBuilder(APP)
}

/** A supplier that creates a new no-op processor per call. */
export function noop(): ProcessorSupplier<unknown, unknown, unknown, unknown> {
	return (): Processor<unknown, unknown, unknown, unknown> => ({ process() {} })
}

export interface StoreFixture {
	kind?: StoreBuilder['kind']
	/** Default: logged to `<app>-<name>-changelog`. */
	logging?: StoreBuilder['logging']
	clock?: StoreBuilder['clock']
}

export function store(name: string, fixture: StoreFixture = {}): StoreBuilder {
	return {
		name,
		kind: fixture.kind ?? 'key-value',
		timestamped: false,
		logging: fixture.logging ?? {
			enabled: true,
			topic: `${APP}-${name}-changelog`,
			configs: {},
			skipRestoration: false,
		},
		clock: fixture.clock,
		create: () => {
			throw new Error('not used by topology tests')
		},
		keyCodec: buffer(),
		valueCodec: buffer(),
	}
}

export function globalStore(name: string, topic: string): GlobalStoreDef {
	return {
		topic,
		sourceName: `${name}-source`,
		processorName: `${name}-processor`,
		store: store(name, { logging: { enabled: false } }),
		supplier: noop() as GlobalStoreDef['supplier'],
	}
}

/** source -> processor (optionally with stores); returns the processor's name. */
export function chain(
	b: InternalTopologyBuilder,
	source: NodeName,
	topics: readonly string[],
	processor: NodeName,
	stores: readonly string[] = []
): NodeName {
	b.addSource({ name: source, topics, internal: false })
	b.addProcessor({ name: processor, supplier: noop(), parents: [source], stores })
	return processor
}

/**
 * `parent` -> sink -> `<app>-<name>-repartition` -> source; returns the source's name. Unnamed repartitions get a
 * fresh owner, as the DSL does.
 */
export function repartition(
	b: InternalTopologyBuilder,
	parent: NodeName,
	name: string,
	options: { owner?: object; partitions?: number } = {}
): NodeName {
	const topic = b.addRepartitionTopic(name, options.owner ?? {}, options.partitions)
	b.addSink({ name: `${name}-sink`, parents: [parent], topic, internal: 'repartition' })
	b.addSource({ name: `${name}-source`, topics: [topic], internal: 'repartition' })
	return `${name}-source`
}

export function repartitionTopic(name: string): string {
	return `${APP}-${name}-repartition`
}
