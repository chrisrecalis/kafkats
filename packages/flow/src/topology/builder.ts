/**
 * InternalTopologyBuilder: what the DSL registers (nodes, stores, global stores, internal topics), frozen by build()
 * into a BuiltTopology (3.3, 4.1.1; KS InternalTopologyBuilder). Structural mistakes (illegal or duplicate names
 * included) throw TopologyError at the call, so the DSL method that made them throws; rules spanning several
 * registrations are checked by build().
 */
import { TopologyError } from '@/api/errors.js'
import { createBuiltTopology, type BuiltTopology } from '@/topology/built-topology.js'
import type {
	GlobalStoreDef,
	NodeDef,
	NodeName,
	ProcessorDef,
	SinkDef,
	SourceDef,
	StoreBuilder,
} from '@/topology/nodes.js'
import { checkName } from '@/topology/names.js'
import { markNullKeys } from '@/topology/null-keys.js'
import { groupSubtopologies } from '@/topology/subtopologies.js'

interface RegisteredStore {
	readonly builder: StoreBuilder
	/** Processor API stores connected to several processors: registering the same builder again connects it. */
	readonly shared: boolean
	readonly global: boolean
}

export class InternalTopologyBuilder {
	private readonly nodes = new Map<NodeName, NodeDef>()
	/** Stores connected to each processor, in connection order. */
	private readonly connections = new Map<NodeName, Set<string>>()
	private readonly stores = new Map<string, RegisteredStore>()
	private readonly globalStores: GlobalStoreDef[] = []
	/** Names of the source and processor nodes of global stores: not part of any subtopology. */
	private readonly globalNodes = new Set<NodeName>()
	private readonly repartitionTopics = new Map<string, { readonly owner: object; readonly partitions?: number }>()
	private readonly throughTopics = new Set<string>()
	private nodeCounter = 0
	private storeCounter = 0
	private repartitionCounter = 0
	private built: BuiltTopology | undefined

	constructor(readonly applicationId: string) {}

	addSource(def: Omit<SourceDef, 'kind'>): void {
		this.checkOpen()
		this.checkNewNode(def.name)
		if (def.topics.length === 0) throw new TopologyError(`Source '${def.name}' reads no topic`)
		for (const topic of def.topics) {
			if (def.internal === 'repartition') this.checkRepartitionTopic(def.name, topic)
			if (def.internal === 'through' && !this.throughTopics.has(topic)) {
				throw new TopologyError(
					`Source '${def.name}' reads '${topic}' as a through topic, but it is not marked as one`
				)
			}
		}
		this.nodes.set(def.name, { kind: 'source', ...def })
	}

	/** Calls supplier() twice and throws TopologyError if both calls return the same object (KS checkSupplier). */
	addProcessor(def: Omit<ProcessorDef, 'kind'>): void {
		this.checkOpen()
		this.checkNewNode(def.name)
		this.checkParents(def.name, def.parents)
		this.checkStores(def.name, def.stores)
		checkSupplier(def.name, def.supplier)
		this.nodes.set(def.name, { kind: 'processor', ...def })
		this.connections.set(def.name, new Set(def.stores))
	}

	addSink(def: Omit<SinkDef, 'kind'>): void {
		this.checkOpen()
		this.checkNewNode(def.name)
		this.checkParents(def.name, def.parents)
		if (def.internal === 'repartition') {
			if (typeof def.topic !== 'string') {
				throw new TopologyError(`Repartition sink '${def.name}' must write a fixed topic`)
			}
			this.checkRepartitionTopic(def.name, def.topic)
		}
		this.nodes.set(def.name, { kind: 'sink', ...def })
	}

	/**
	 * A store name is registered once (B5): a duplicate throws, unless both registrations are `shared` with the same
	 * builder, which only connects it to more processors.
	 */
	addStateStore(builder: StoreBuilder, connectTo: readonly NodeName[] = [], options?: { shared?: boolean }): void {
		this.checkOpen()
		checkStoreNames(builder)
		const shared = options?.shared === true
		const existing = this.stores.get(builder.name)
		if (existing !== undefined && !(existing.shared && shared && existing.builder === builder)) {
			throw duplicateStore(existing.builder, builder)
		}
		for (const processor of connectTo) this.connectionsOf(processor)
		if (existing === undefined) this.stores.set(builder.name, { builder, shared, global: false })
		for (const processor of connectTo) this.connectProcessorAndStores(processor, [builder.name])
	}

	connectProcessorAndStores(processor: NodeName, stores: readonly string[]): void {
		this.checkOpen()
		const connected = this.connectionsOf(processor)
		this.checkStores(processor, stores)
		for (const store of stores) connected.add(store)
	}

	addGlobalStore(def: GlobalStoreDef): void {
		this.checkOpen()
		this.checkNewNode(def.sourceName)
		this.checkNewNode(def.processorName)
		if (def.sourceName === def.processorName) {
			throw new TopologyError(`Global store '${def.store.name}' needs distinct source and processor names`)
		}
		checkStoreNames(def.store)
		const existing = this.stores.get(def.store.name)
		if (existing !== undefined) throw duplicateStore(existing.builder, def.store)
		if (def.store.logging.enabled) {
			throw new TopologyError(
				`Global store '${def.store.name}' cannot have a changelog: its topic is its changelog`
			)
		}
		checkSupplier(def.processorName, def.supplier)
		this.globalStores.push(def)
		this.globalNodes.add(def.sourceName).add(def.processorName)
		this.stores.set(def.store.name, { builder: def.store, shared: false, global: true })
	}

	/**
	 * Registers `<applicationId>-<name>-repartition` and returns it. The same `owner` (a named grouping) may claim the
	 * name again and gets the same topic; another owner -> TopologyError (B4).
	 */
	addRepartitionTopic(name: string, owner: object, partitions?: number): string {
		this.checkOpen()
		checkName('Repartition topic', name)
		if (partitions !== undefined && !(Number.isInteger(partitions) && partitions > 0)) {
			throw new TopologyError(
				`Repartition topic '${name}' needs a positive integer partition count, got ${partitions}`
			)
		}
		const topic = `${this.applicationId}-${name}-repartition`
		// The application id and the suffix count too.
		checkName('Repartition topic', topic)
		const existing = this.repartitionTopics.get(topic)
		if (existing === undefined) {
			this.repartitionTopics.set(topic, { owner, partitions })
		} else if (existing.owner !== owner) {
			throw new TopologyError(
				`The repartition topic name '${name}' is already used by another operation; give each grouping its own name`
			)
		}
		return topic
	}

	/** A user topic written and read back by the app (deprecated through()): read from the earliest offset. */
	markThroughTopic(topic: string): void {
		this.checkOpen()
		this.throughTopics.add(topic)
	}

	/** The nodes registered so far, by name: the DSL walks its graph upstream through them (suppress's grace search). */
	registeredNodes(): ReadonlyMap<NodeName, NodeDef> {
		return this.nodes
	}

	/** `named`, or `<prefix>-<10-digit counter>` (KS `KSTREAM-FILTER-0000000003`). */
	nodeName(prefix: string, named?: string): NodeName {
		this.checkOpen()
		return named ?? `${prefix}-${String(this.nodeCounter++).padStart(10, '0')}`
	}

	/** Counter of generated store names (separate from node names). */
	storeId(): number {
		this.checkOpen()
		return this.storeCounter++
	}

	/** Counter of generated `repartition-<N>` topic names. */
	repartitionId(): number {
		this.checkOpen()
		return this.repartitionCounter++
	}

	/** Idempotent; afterwards every registration throws. */
	build(): BuiltTopology {
		if (this.built) return this.built
		const nodes = markNullKeys(
			[...this.nodes.values()].map(node =>
				node.kind === 'processor' ? { ...node, stores: [...(this.connections.get(node.name) ?? [])] } : node
			)
		)
		const local = [...this.stores.values()].filter(store => !store.global).map(store => store.builder)
		const repartitionTopics = new Map(
			[...this.repartitionTopics].map(([topic, { partitions }]) => [topic, partitions])
		)
		this.built = createBuiltTopology({
			applicationId: this.applicationId,
			subtopologies: groupSubtopologies({ nodes, stores: local, globalStores: this.globalStores }),
			globalStores: [...this.globalStores],
			repartitionTopics,
			throughTopics: new Set(this.throughTopics),
		})
		return this.built
	}

	private checkOpen(): void {
		if (this.built) throw new TopologyError('topology already built')
	}

	private checkNewNode(name: NodeName): void {
		checkName('Node', name)
		if (this.nodes.has(name) || this.globalNodes.has(name)) {
			throw new TopologyError(`A node named '${name}' already exists; node names must be unique`)
		}
	}

	private checkParents(name: NodeName, parents: readonly NodeName[]): void {
		if (parents.length === 0) throw new TopologyError(`'${name}' needs at least one parent`)
		for (const parent of parents) {
			const node = this.nodes.get(parent)
			if (node === undefined) throw new TopologyError(`Parent '${parent}' of '${name}' is not registered`)
			if (node.kind === 'sink') throw new TopologyError(`Sink '${parent}' cannot be the parent of '${name}'`)
		}
	}

	private checkStores(processor: NodeName, stores: readonly string[]): void {
		for (const name of stores) {
			const store = this.stores.get(name)
			if (store === undefined)
				throw new TopologyError(`State store '${name}' of '${processor}' is not registered`)
			if (store.global) {
				throw new TopologyError(
					`Global store '${name}' is readable by every processor and cannot be connected to '${processor}'`
				)
			}
		}
	}

	private checkRepartitionTopic(node: NodeName, topic: string): void {
		if (!this.repartitionTopics.has(topic)) {
			throw new TopologyError(`'${node}' uses '${topic}' as a repartition topic, but it is not registered`)
		}
	}

	private connectionsOf(processor: NodeName): Set<string> {
		const connected = this.connections.get(processor)
		if (connected === undefined) {
			throw new TopologyError(`'${processor}' is not a registered processor; stores connect to processors`)
		}
		return connected
	}
}

function checkSupplier(name: NodeName, supplier: () => unknown): void {
	if (supplier() === supplier()) {
		throw new TopologyError(
			`The supplier of '${name}' returned the same processor twice; it must create a new one per call`
		)
	}
}

/** The store's name and, if it has one, its changelog topic's (a custom `ChangelogConfig.topicName`, say). */
function checkStoreNames(store: StoreBuilder): void {
	checkName('State store', store.name)
	if (store.logging.enabled) {
		checkName('Changelog topic', store.logging.topic)
	}
}

function duplicateStore(existing: StoreBuilder, added: StoreBuilder): TopologyError {
	if (existing.kind !== added.kind) {
		return new TopologyError(
			`State store '${added.name}' already exists as a ${existing.kind} store and cannot be used as a ${added.kind} store`
		)
	}
	return new TopologyError(`State store '${added.name}' already exists; every store needs its own name`)
}
