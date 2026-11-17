/**
 * What every DSL object of one app shares (KS InternalStreamsBuilder): the topology builder, the changelog defaults
 * that store builders resolve against, the registrations every operator family makes (sources, processors), the
 * grace periods of the windowed aggregations and the builders of the Processor API stores.
 */
import type { Consumed } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { ChangelogDefaults } from '@/changelog/config.js'
import { sourceFormat } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { StateStore } from '@/state.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import type { NodeName, ProcessorDef, SourceDef, StoreBuilder } from '@/topology/nodes.js'

export class DslContext {
	/**
	 * The grace period of each windowed aggregation, by processor (KS GracePeriodGraphNode): how long after its end a
	 * window may still change, for a session including its gap. `suppress()` finds when windows close through it.
	 */
	readonly windowGraces = new Map<NodeName, number>()

	/**
	 * The builder of each Processor API store definition, made once per app: every processor connected to the store
	 * registers the same builder (`dsl/ops/process.ts`).
	 */
	readonly definedStores = new Map<StoreDefinition<StateStore>, StoreBuilder>()

	constructor(
		readonly builder: InternalTopologyBuilder,
		/** Store builders resolve their changelogs against these. */
		readonly changelog: ChangelogDefaults
	) {}

	get applicationId(): string {
		return this.builder.applicationId
	}

	/**
	 * The stream of a topic's records (`app.stream()`, and the read-back of repartition and through topics). Missing
	 * codecs read raw Buffers.
	 */
	stream<K, V>(
		topic: string,
		consumed: Consumed<K, V> = {},
		internal: SourceDef['internal'] = false
	): KStreamImpl<K, V> {
		return new KStreamImpl(this, this.source(topic, consumed, internal), sourceFormat(consumed), false)
	}

	/** Registers the source node `consumed.name ?? KSTREAM-SOURCE-<n>` of a topic; returns its name. */
	source<K, V>(topic: string, consumed: Consumed<K, V>, internal: SourceDef['internal'] = false): NodeName {
		const { keyCodec, valueCodec } = sourceFormat(consumed)
		const name = this.builder.nodeName(NodePrefix.SOURCE, consumed.name)
		this.builder.addSource({
			name,
			topics: [topic],
			keyCodec,
			valueCodec,
			timestampExtractor: consumed.timestampExtractor,
			offsetReset: consumed.offsetReset,
			internal,
		})
		return name
	}

	/**
	 * Registers the processor `name ?? <prefix>-<n>` under `parents`, connected to `stores`; returns its name. `marks`:
	 * whether it is a join that passes null keys (4.1.6).
	 */
	processor<KIn, VIn, KOut, VOut>(
		prefix: string,
		name: string | undefined,
		parents: readonly NodeName[],
		supplier: ProcessorSupplier<KIn, VIn, KOut, VOut>,
		stores: readonly string[] = [],
		marks: Pick<ProcessorDef, 'relaxedNullKeyJoin'> = {}
	): NodeName {
		const node = this.builder.nodeName(prefix, name)
		this.builder.addProcessor({
			name: node,
			parents,
			stores,
			supplier: supplier as ProcessorSupplier<unknown, unknown, unknown, unknown>,
			...marks,
		})
		return node
	}
}
