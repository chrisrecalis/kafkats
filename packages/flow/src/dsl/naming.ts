/**
 * Generated names (4.1.1, 6.22). A `{ name }` option wins; otherwise names come from the builder's counters, so they
 * depend on the order of DSL calls, as in Kafka Streams. Persisted state is keyed by store names, never by node names.
 */
import type { InternalTopologyBuilder } from '@/topology/builder.js'

/** Prefixes of generated node names, for `builder.nodeName(prefix, options?.name)` (KS `KSTREAM-FILTER-0000000003`). */
export const NodePrefix = {
	SOURCE: 'KSTREAM-SOURCE',
	SINK: 'KSTREAM-SINK',
	FILTER: 'KSTREAM-FILTER',
	MAP: 'KSTREAM-MAP',
	MAP_VALUES: 'KSTREAM-MAPVALUES',
	FLAT_MAP: 'KSTREAM-FLATMAP',
	FLAT_MAP_VALUES: 'KSTREAM-FLATMAPVALUES',
	PEEK: 'KSTREAM-PEEK',
	FOREACH: 'KSTREAM-FOREACH',
	KEY_SELECT: 'KSTREAM-KEY-SELECT',
	MERGE: 'KSTREAM-MERGE',
	BRANCH: 'KSTREAM-BRANCH',
	BRANCH_CHILD: 'KSTREAM-BRANCHCHILD',
	PROCESSOR: 'KSTREAM-PROCESSOR',
	PROCESS_VALUES: 'KSTREAM-PROCESSVALUES',
	REPARTITION: 'KSTREAM-REPARTITION',
	TO_TABLE: 'KSTREAM-TOTABLE',
	AGGREGATE: 'KSTREAM-AGGREGATE',
	REDUCE: 'KSTREAM-REDUCE',
	JOIN: 'KSTREAM-JOIN',
	LEFT_JOIN: 'KSTREAM-LEFTJOIN',
	JOIN_THIS: 'KSTREAM-JOINTHIS',
	JOIN_OTHER: 'KSTREAM-JOINOTHER',
	OUTER_THIS: 'KSTREAM-OUTERTHIS',
	OUTER_OTHER: 'KSTREAM-OUTEROTHER',
	TABLE_SOURCE: 'KTABLE-SOURCE',
	TABLE_FILTER: 'KTABLE-FILTER',
	TABLE_MAP_VALUES: 'KTABLE-MAPVALUES',
	TABLE_TRANSFORM_VALUES: 'KTABLE-TRANSFORMVALUES',
	TABLE_TO_STREAM: 'KTABLE-TOSTREAM',
	TABLE_SELECT: 'KTABLE-SELECT',
	TABLE_AGGREGATE: 'KTABLE-AGGREGATE',
	TABLE_REDUCE: 'KTABLE-REDUCE',
	TABLE_JOIN_THIS: 'KTABLE-JOINTHIS',
	TABLE_JOIN_OTHER: 'KTABLE-JOINOTHER',
	TABLE_MERGE: 'KTABLE-MERGE',
	TABLE_SUPPRESS: 'KTABLE-SUPPRESS',
} as const

/** Prefixes of generated store names: `<prefix>-<store counter>` (today's defaults, dsl§12.1). */
export const StorePrefix = {
	COUNT: 'count-store',
	REDUCE: 'reduce-store',
	AGGREGATE: 'aggregate-store',
	WINDOW_COUNT: 'window-count-store',
	WINDOW_REDUCE: 'window-reduce-store',
	WINDOW_AGGREGATE: 'window-aggregate-store',
	TABLE_COUNT: 'table-count-store',
	TABLE_REDUCE: 'table-reduce-store',
	TABLE_AGGREGATE: 'table-aggregate-store',
	TO_TABLE: 'toTable-store',
	STREAM_JOIN: 'stream-join',
	STREAM_LEFT_JOIN: 'stream-left-join',
	STREAM_OUTER_JOIN: 'stream-outer-join',
	SUPPRESS: 'KTABLE-SUPPRESS-STATE-STORE',
	table: (topic: string) => `table-${topic}`,
	globalTable: (topic: string) => `global-table-${topic}`,
} as const

/** The store name: `name` (Materialized.storeName) or a generated `<prefix>-<store counter>`. */
export function storeName(builder: InternalTopologyBuilder, prefix: string, name?: string): string {
	return name ?? `${prefix}-${builder.storeId()}`
}

/**
 * The base name of a repartition topic: `name` or a generated `repartition-<N>`; the builder registers it as
 * `<applicationId>-<base>-repartition` (`addRepartitionTopic`).
 */
export function repartitionName(builder: InternalTopologyBuilder, name?: string): string {
	return name ?? `repartition-${builder.repartitionId()}`
}
