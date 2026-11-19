/**
 * KTable.suppress (6.16; KS KTableImpl.suppress): a processor holding back the table's updates in a changelogged
 * buffer store, `<name>-store` or `KTABLE-SUPPRESS-STATE-STORE-<n>`, keyed and valued with the table's codecs. Until
 * windows close, they close after the grace of the windowed aggregations the table comes from, found upstream when
 * suppress() is called. The result has the table's codecs.
 */
import { TopologyError } from '@/api/errors.js'
import type { KTable } from '@/api/ktable.js'
import type { SuppressOptions } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix, suffixed } from '@/dsl/naming.js'
import { durationMs } from '@/dsl/windows.js'
import { suppressBufferStoreBuilder } from '@/operators/suppress-buffer.js'
import { KTableSuppress, type Suppression } from '@/operators/suppress.js'
import type { NodeDef, NodeName } from '@/topology/nodes.js'

export function suppress<K, V>(table: KTableImpl<K, V>, options: SuppressOptions): KTable<K, V> {
	const { ctx } = table
	const suppression = resolve(table, options)
	const codecs = { key: bufferCodec<K>(table.format, 'key'), value: bufferCodec<V>(table.format, 'value') }
	const store = suffixed(options.name, 'store') ?? storeName(ctx.builder, StorePrefix.SUPPRESS)
	ctx.builder.addStateStore(suppressBufferStoreBuilder(ctx.changelog, store))
	const node = new KTableSuppress(table.table, store, suppression, codecs)
	const processor = ctx.processor(NodePrefix.TABLE_SUPPRESS, options.name, [table.node], node.supplier, [store])
	return new KTableImpl(ctx, processor, table.format, node)
}

/** The options as the processor takes them; buffers are unbounded by default (KS BufferConfig.unbounded()). */
function resolve<K, V>(table: KTableImpl<K, V>, options: SuppressOptions): Suppression {
	const maxRecords = bufferLimit(options.maxRecords, 'maxRecords')
	const maxBytes = bufferLimit(options.maxBytes, 'maxBytes')
	if (options.until === 'window-close') {
		const graceMs = windowCloseGraceMs(table.ctx, table.node)
		return { until: 'window-close', durationMs: graceMs, maxRecords, maxBytes, whenFull: 'shutdown' }
	}
	return {
		until: 'time-limit',
		durationMs: durationMs(options.timeLimit, 'The suppress time limit'),
		maxRecords,
		maxBytes,
		whenFull: options.whenFull ?? 'emit-early',
	}
}

/** A buffer limit: a non-negative whole number or Infinity; undefined: unbounded. */
function bufferLimit(limit: number | undefined, name: string): number {
	if (limit === undefined || limit === Number.POSITIVE_INFINITY) {
		return Number.POSITIVE_INFINITY
	}
	if (!Number.isSafeInteger(limit) || limit < 0) {
		throw new TopologyError(`suppress() ${name} must be a non-negative whole number, got ${limit}`)
	}
	return limit
}

/** The buffer keeps the table's keys and values in the table's codecs. */
function bufferCodec<T>(format: StreamFormat, part: 'key' | 'value'): Codec<T> {
	const codec = part === 'key' ? format.keyCodec : format.valueCodec
	if (codec === undefined) {
		throw new TopologyError(
			`suppress() needs a ${part} codec to buffer the table's ${part}s: they were changed upstream, so give ` +
				`the operator that changed them one (its Materialized)`
		)
	}
	return codec as Codec<T>
}

/**
 * When the windows of `node`'s table close (KS GraphGraceSearchUtil.findAndVerifyWindowGrace): after the grace of the
 * windowed aggregation the table comes from; the largest grace when several parents lead to windowed aggregations,
 * which each of them must. Repartition topics are crossed to the nodes that write them.
 */
export function windowCloseGraceMs(ctx: DslContext, node: NodeName): number {
	const nodes = ctx.builder.registeredNodes()
	const parentsOf = (def: NodeDef): readonly NodeName[] => {
		if (def.kind === 'processor') {
			return def.parents
		}
		if (def.kind === 'source' && def.internal === 'repartition') {
			return [...nodes.values()].flatMap(writer =>
				writer.kind === 'sink' && def.topics.includes(writer.topic as string) ? writer.parents : []
			)
		}
		return []
	}
	const search = (name: NodeName, chain: string): number => {
		const grace = ctx.windowGraces.get(name)
		if (grace !== undefined) {
			return grace
		}
		const path = chain === '' ? name : `${name}->${chain}`
		const parents = parentsOf(nodes.get(name)!)
		if (parents.length === 0) {
			throw new TopologyError(
				`suppress() until 'window-close' needs a windowed aggregation upstream: window close time is only ` +
					`defined for windowed computations. Got [${path}].`
			)
		}
		return Math.max(...parents.map(parent => search(parent, path)))
	}
	return search(node, '')
}
