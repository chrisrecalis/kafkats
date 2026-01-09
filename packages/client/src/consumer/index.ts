/**
 * Consumer module exports
 */

// Main consumer
export { Consumer } from './consumer.js'
export { ShareConsumer } from '../share-consumer/share-consumer.js'

// Types
export type {
	// Decoder
	Decoder,
	DecoderLike,
	// Subscription
	TopicSubscription,
	SubscriptionInput,
	MsgOf,
	KeyOf,
	// Message
	Message,
	ConsumeContext,
	// Handlers
	MessageHandler,
	BatchHandler,
	// Options
	RunEachOptions,
	RunBatchOptions,
	// Configuration
	ConsumerConfig,
	AutoOffsetReset,
	IsolationLevel,
	PartitionAssignmentStrategy,
	// Internal types
	TopicPartition,
	TopicPartitionOffset,
	ConsumerGroupState,
	ConsumerGroupEvents,
	ConsumerEvents,
} from './types.js'

export {
	normalizeDecoder,
	DEFAULT_CONSUMER_CONFIG,
	DEFAULT_RUN_EACH_OPTIONS,
	DEFAULT_RUN_BATCH_OPTIONS,
} from './types.js'

export type {
	ShareConsumerConfig,
	ShareSubscriptionInput,
	ShareMessage,
	ShareMessageHandler,
	ShareRunEachOptions,
	ShareConsumerEvents,
} from '../share-consumer/types.js'

export { DEFAULT_SHARE_CONSUMER_CONFIG, DEFAULT_SHARE_RUN_EACH_OPTIONS } from '../share-consumer/types.js'

// Assignors
export type { PartitionAssignor, MemberSubscription, RebalanceProtocol } from './assignors/index.js'
export {
	RangeAssignor,
	rangeAssignor,
	StickyAssignor,
	stickyAssignor,
	CooperativeStickyAssignor,
	cooperativeStickyAssignor,
	CONSUMER_PROTOCOL_TYPE,
} from './assignors/index.js'

// Consumer group (for advanced use)
export { ConsumerGroup } from './consumer-group.js'
export type { JoinResult } from './consumer-group.js'

// Offset manager (for advanced use)
export { OffsetManager } from './offset-manager.js'

// Fetch manager (for advanced use)
export { FetchManager } from './fetch-manager.js'
export type { FetchManagerConfig } from './fetch-manager.js'

// Partition tracker (for advanced use - coordinates partition ownership and processing state)
export { PartitionTracker } from './partition-tracker.js'
export type { PartitionTrackerConfig } from './partition-tracker.js'
