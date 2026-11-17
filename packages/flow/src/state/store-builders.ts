/**
 * Store builders: how a store of the topology is created for each task, and how it is logged. A builder is shared by
 * every task of its subtopology; the provider store it creates is the bottom of one task's store stack. Window and
 * session stores of time-based processors take a `clock` (6.2): their stack then gets the clipping layer of
 * `state/time-clipping.ts`; their retention comes from the DSL's retention model (`dsl/retention.ts`).
 */
import { resolveStoreChangelog, type ChangelogConfig, type ChangelogDefaults } from '@/changelog/config.js'
import type { Codec } from '@/codec.js'
import type {
	KeyValueStore,
	SessionStore,
	TimestampedKeyValueStore,
	TimestampedWindowStore,
	ValueAndTimestamp,
	WindowStore,
} from '@/state.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import type { StoreBuilder, StoreClockSpec } from '@/topology/nodes.js'

export interface StoreBuilderOptions<K, V> {
	readonly name: string
	readonly keyCodec: Codec<K>
	readonly valueCodec: Codec<V>
	/** Changelog logging; default: logged to `<applicationId>-<name>-changelog`. */
	readonly logging?: boolean | ChangelogConfig
}

export interface WindowStoreBuilderOptions<K, V> extends StoreBuilderOptions<K, V> {
	readonly retentionMs: number
	readonly windowSizeMs: number
	/** A store clock: reads clipped by the retention, expiry by the clock (6.2). */
	readonly clock?: StoreClockSpec
}

export interface SessionStoreBuilderOptions<K, V> extends StoreBuilderOptions<K, V> {
	readonly retentionMs: number
	/** A store clock: reads clipped by the retention, expiry by the clock (6.2). */
	readonly clock?: StoreClockSpec
}

export function keyValueStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: StoreBuilderOptions<K, V>
): StoreBuilder<KeyValueStore<K, V>> {
	const { name, keyCodec, valueCodec } = options
	return {
		name,
		kind: 'key-value',
		timestamped: false,
		logging: resolveStoreChangelog(defaults, { name }, options.logging),
		keyCodec,
		valueCodec,
		create: (provider, providerName) => provider.createKeyValueStore(providerName, { keyCodec, valueCodec }),
	}
}

/** A key-value store whose values carry the timestamp of their last update (`[int64BE timestamp][value]`). */
export function timestampedKeyValueStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: StoreBuilderOptions<K, V>
): StoreBuilder<TimestampedKeyValueStore<K, V>> {
	const valueCodec = valueAndTimestampCodec(options.valueCodec)
	return { ...keyValueStoreBuilder(defaults, { ...options, valueCodec }), timestamped: true }
}

export function windowStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: WindowStoreBuilderOptions<K, V>
): StoreBuilder<WindowStore<K, V>> {
	const { name, keyCodec, valueCodec, retentionMs, windowSizeMs, clock } = options
	return {
		name,
		kind: 'window',
		timestamped: false,
		logging: resolveStoreChangelog(defaults, { name, retentionMs }, options.logging),
		retentionMs,
		windowSizeMs,
		clock,
		keyCodec,
		valueCodec,
		create: (provider, providerName) =>
			provider.createWindowStore(providerName, { keyCodec, valueCodec, retentionMs, windowSizeMs }),
	}
}

/** A window store whose values carry a timestamp (`[int64BE timestamp][value]`): time-window aggregations (6.4). */
export function timestampedWindowStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: WindowStoreBuilderOptions<K, V>
): StoreBuilder<TimestampedWindowStore<K, V>> {
	const valueCodec = valueAndTimestampCodec(options.valueCodec)
	return { ...windowStoreBuilder(defaults, { ...options, valueCodec }), timestamped: true }
}

export function sessionStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: SessionStoreBuilderOptions<K, V>
): StoreBuilder<SessionStore<K, V>> {
	const { name, keyCodec, valueCodec, retentionMs, clock } = options
	return {
		name,
		kind: 'session',
		timestamped: false,
		logging: resolveStoreChangelog(defaults, { name, retentionMs }, options.logging),
		retentionMs,
		clock,
		keyCodec,
		valueCodec,
		create: (provider, providerName) =>
			provider.createSessionStore(providerName, { keyCodec, valueCodec, retentionMs }),
	}
}

/**
 * A session(-layout) store whose values carry a timestamp (`[int64BE timestamp][value]`): sliding-window aggregations,
 * whose windows `[start, start + d]` are kept as sessions so the windows of one key are read together (6.6).
 */
export function timestampedSessionStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: SessionStoreBuilderOptions<K, V>
): StoreBuilder<SessionStore<K, ValueAndTimestamp<V>>> {
	const valueCodec = valueAndTimestampCodec(options.valueCodec)
	return { ...sessionStoreBuilder(defaults, { ...options, valueCodec }), timestamped: true }
}
