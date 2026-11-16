/**
 * Tables read from a topic or a stream (6.0, 6.11, 6.19): `app.table()`, `KStream.toTable()` and `app.globalTable()`.
 * Each is materialized in a timestamped key-value store; the table's format is the store's codecs (B6).
 */
import type { KTable } from '@/api/ktable.js'
import type { Consumed, Materialized, NamedOptions } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec, sourceFormat, type StreamFormat } from '@/dsl/format.js'
import { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { KTableSource } from '@/operators/ktable-source.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import { timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'

export type TableConsumed<K, V> = Consumed<K, V> & { materialized?: Materialized<K, V> }

/** `app.table()`: the source node, then the table's KTableSource (`<name>-table-source` when the source is named). */
export function table<K, V>(ctx: DslContext, topic: string, options: TableConsumed<K, V> = {}): KTable<K, V> {
	const source = ctx.source(topic, options)
	const name = options.name === undefined ? undefined : `${options.name}-table-source`
	const store = storeName(ctx.builder, StorePrefix.table(topic), options.materialized?.storeName)
	return materialize(ctx, source, store, name, NodePrefix.TABLE_SOURCE, sourceFormat(options), options.materialized)
}

/** `KStream.toTable()`: repartitions first if the stream's keys changed upstream (6.0). */
export function toTable<K, V>(stream: KStreamImpl<K, V>, options: Materialized<K, V> & NamedOptions = {}): KTable<K, V> {
	const operator = 'toTable()'
	const codecs = {
		key: requireCodec(options.key, stream.format.keyCodec, 'key', operator),
		value: requireCodec(options.value, stream.format.valueCodec, 'value', operator),
	}
	const self = repartitionIfRequired(stream, { name: options.name, owner: {}, codecs, operator })
	const store = storeName(self.ctx.builder, StorePrefix.TO_TABLE, options.storeName)
	return materialize(self.ctx, self.node, store, options.name, NodePrefix.TO_TABLE, self.format, options)
}

/**
 * `app.globalTable()`: every instance reads the whole topic into `<applicationId>.<store>` outside the consumer group
 * (no changelog: the topic is one); only usable as the lookup side of a stream join.
 */
export function globalTable<K, V>(ctx: DslContext, topic: string, options: TableConsumed<K, V> = {}) {
	const { builder } = ctx
	const format = sourceFormat(options)
	const store = storeName(builder, StorePrefix.globalTable(topic), options.materialized?.storeName)
	const codecs = storeCodecs(format, options.materialized)
	const sourceName = builder.nodeName(NodePrefix.SOURCE, options.name)
	const processorName = builder.nodeName(
		NodePrefix.TABLE_SOURCE,
		options.name === undefined ? undefined : `${options.name}-table-source`
	)
	builder.addGlobalStore({
		topic,
		sourceName,
		processorName,
		store: timestampedKeyValueStoreBuilder(ctx.changelog, { name: store, ...codecs, logging: false }),
		supplier: new KTableSource<K, V>(store).supplier as ProcessorSupplier<unknown, unknown, never, never>,
		keyCodec: format.keyCodec,
		valueCodec: format.valueCodec,
		timestampExtractor: options.timestampExtractor,
	})
	return new GlobalKTableImpl<K, V>(ctx, store, { keyCodec: codecs.keyCodec, valueCodec: codecs.valueCodec })
}

/** Registers the table's store and its KTableSource under `parent`. */
function materialize<K, V>(
	ctx: DslContext,
	parent: string,
	store: string,
	name: string | undefined,
	prefix: string,
	format: StreamFormat,
	materialized: Materialized<K, V> | undefined
): KTableImpl<K, V> {
	const codecs = storeCodecs(format, materialized)
	ctx.builder.addStateStore(
		timestampedKeyValueStoreBuilder(ctx.changelog, { name: store, ...codecs, logging: materialized?.changelog })
	)
	const node = new KTableSource<K, V>(store)
	const processor = ctx.processor(prefix, name, [parent], node.supplier, [store])
	const windowed = materialized?.key === undefined ? format.windowed : undefined
	return new KTableImpl(ctx, processor, { ...codecs, windowed }, node)
}

/** The store's codecs: Materialized first, then the format (6.0); both are required. */
function storeCodecs<K, V>(
	format: StreamFormat,
	materialized: Materialized<K, V> | undefined
): { keyCodec: Codec<K>; valueCodec: Codec<V> } {
	return {
		keyCodec: requireCodec(materialized?.key, format.keyCodec, 'key', 'a table'),
		valueCodec: requireCodec(materialized?.value, format.valueCodec, 'value', 'a table'),
	}
}
