export const flowVersion = '0.1.0'

// Core flow API
export { flow, topic, TimeWindows, SessionWindows, SlidingWindows } from '@/flow.js'

// Types
export type {
	FlowApp,
	FlowConfig,
	KStream,
	KTable,
	KGroupedStream,
	KGroupedTable,
	WindowedKGroupedStream,
	Windowed,
	KeyValue,
	StreamState,
	WindowDuration,
	Topic,
	Consumed,
	Produced,
	Grouped,
	Materialized,
	Joined,
} from '@/flow.js'

// Codecs
export { codec, string, json, buffer } from '@/codec.js'
export type { Codec } from '@/codec.js'

// State stores
export {
	inMemory,
	InMemoryStateStoreProvider,
	InMemoryKeyValueStore,
	InMemoryWindowStore,
	InMemorySessionStore,
} from '@/state/memory.js'
export {
	ChangelogBackedKeyValueStore,
	ChangelogBackedWindowStore,
	ChangelogBackedSessionStore,
} from '@/state/changelog.js'
export type {
	StateStoreProvider,
	ChangelogCheckpointStore,
	KeyValueStore,
	WindowStore,
	SessionStore,
	KeyValueStoreOptions,
	WindowStoreOptions,
	SessionStoreOptions,
	WindowedKey,
} from '@/state.js'

// Changelog
export {
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
	DEFAULT_CHANGELOG_CONFIGS,
	ChangelogWriter,
	ChangelogRestorer,
	ChangelogPartitionMismatchError,
	SourceTopicNotFoundError,
} from '@/changelog.js'
export type { ChangelogConfig, ChangelogTopicSpec } from '@/changelog.js'
