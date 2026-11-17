export const flowVersion = '0.1.0'

// Core flow API
export { flow } from '@/app/flow.js'
export { topic } from '@/topic.js'
export { TimeWindows, SessionWindows, SlidingWindows, JoinWindows } from '@/dsl/windows.js'

// Types
export type {
	FlowApp,
	FlowConfig,
	CloseOptions,
	StreamState,
	TopologyDescription,
	SubtopologyDescription,
	GlobalStoreDescription,
	TopologyNodeDescription,
} from '@/api/app.js'
export type { KStream, BranchedKStream, KeyValue, ValueMapper, ValueJoiner } from '@/api/kstream.js'
export type { KTable, GlobalKTable } from '@/api/ktable.js'
export type {
	KGroupedStream,
	KGroupedTable,
	TimeWindowedKStream,
	SessionWindowedKStream,
	SlidingWindowedKStream,
	Windowed,
} from '@/api/grouped.js'
export type {
	WindowDuration,
	NamedOptions,
	Consumed,
	Produced,
	RecordPartitioner,
	Grouped,
	Materialized,
	AggregateOptions,
	EmitStrategy,
	WindowedAggregateOptions,
	TableOptions,
	TransformValuesOptions,
	ProcessOptions,
	StreamJoined,
	Joined,
	Repartitioned,
	SuppressOptions,
	SplitOptions,
	BranchOptions,
} from '@/api/options.js'
export type { StoreDefinition, StoreOptions, StoresFactory } from '@/api/stores.js'
export type { Topic } from '@/topic.js'
export type { ResetPolicy, TopicNameExtractor } from '@/topology/nodes.js'

// Errors and handlers
export {
	FlowConfigError,
	TopologyError,
	StreamsError,
	IllegalStateError,
	TaskMigratedError,
	TaskCorruptedError,
	FailedProcessingError,
	InvalidTimestampError,
	AssignmentError,
	NoOffsetResetError,
} from '@/api/errors.js'
export type {
	ErrorHandlerContext,
	HandlerResponse,
	DeserializationExceptionHandler,
	ProcessingExceptionHandler,
	ProductionExceptionHandler,
	UncaughtExceptionResponse,
	UncaughtExceptionHandler,
} from '@/api/handlers.js'
export type { CollectedRecord } from '@/processor/output.js'

// Processor API
export { StreamRecord, FixedKeyRecord } from '@/processor/record.js'
export type { RecordHeaders } from '@/processor/record.js'
export type {
	Processor,
	ProcessorSupplier,
	ProcessorContext,
	FixedKeyProcessor,
	FixedKeyProcessorSupplier,
	FixedKeyProcessorContext,
	ValueTransformerWithKey,
	ValueTransformerWithKeySupplier,
	ForwardingDisabledContext,
	RecordMetadata,
	TaskId,
	Punctuator,
	PunctuationType,
	Cancellable,
} from '@/processor/api.js'
export { TimestampExtractors } from '@/processor/timestamps.js'
export type { TimestampExtractor, ConsumedRecord } from '@/processor/timestamps.js'

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
export type { InMemoryProviderOptions } from '@/state/memory.js'
export {
	ChangelogBackedKeyValueStore,
	ChangelogBackedWindowStore,
	ChangelogBackedSessionStore,
} from '@/state/logging.js'
export type {
	StateStoreProvider,
	ChangelogCheckpointStore,
	StateStore,
	KeyValueStore,
	WindowStore,
	SessionStore,
	TimestampedKeyValueStore,
	TimestampedWindowStore,
	ReadOnlyTimestampedKeyValueStore,
	ValueAndTimestamp,
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
	CoPartitioningError,
	SourceTopicNotFoundError,
} from '@/changelog.js'
export type { ChangelogConfig, ChangelogRestorationOptions } from '@/changelog.js'
