/**
 * Store builders: how a store of the topology is created for each task, and how it is logged. A builder is shared by
 * every task of its subtopology; the provider store it creates is the bottom of one task's store stack.
 */
import { resolveStoreChangelog, type ChangelogConfig, type ChangelogDefaults } from '@/changelog/config.js'
import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, TimestampedKeyValueStore, WindowStore } from '@/state.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import type { StoreBuilder } from '@/topology/nodes.js'

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
}

export interface SessionStoreBuilderOptions<K, V> extends StoreBuilderOptions<K, V> {
	readonly retentionMs: number
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
	const { name, keyCodec, valueCodec, retentionMs, windowSizeMs } = options
	return {
		name,
		kind: 'window',
		timestamped: false,
		logging: resolveStoreChangelog(defaults, { name, retentionMs }, options.logging),
		retentionMs,
		windowSizeMs,
		keyCodec,
		valueCodec,
		create: (provider, providerName) =>
			provider.createWindowStore(providerName, { keyCodec, valueCodec, retentionMs, windowSizeMs }),
	}
}

export function sessionStoreBuilder<K, V>(
	defaults: ChangelogDefaults,
	options: SessionStoreBuilderOptions<K, V>
): StoreBuilder<SessionStore<K, V>> {
	const { name, keyCodec, valueCodec, retentionMs } = options
	return {
		name,
		kind: 'session',
		timestamped: false,
		logging: resolveStoreChangelog(defaults, { name, retentionMs }, options.logging),
		retentionMs,
		keyCodec,
		valueCodec,
		create: (provider, providerName) =>
			provider.createSessionStore(providerName, { keyCodec, valueCodec, retentionMs }),
	}
}
