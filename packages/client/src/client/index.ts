/**
 * Low-Level Kafka Client
 *
 * Provides typed access to Kafka protocol operations with:
 * - Automatic API version negotiation
 * - Broker connection management
 * - Cluster topology discovery
 * - Coordinator caching
 */

// Main client
export { KafkaClient, type ProduceOptions, type FetchOptions } from './kafka-client.js'

// Topic helper - shared between producer and consumer
export { topic } from '@/topic.js'
export type { TopicDefinition, TopicOptions, DecoderLike } from '@/topic.js'

// Codecs
export { codec, string, json, buffer } from '@/codec.js'
export type { Codec } from '@/codec.js'

// Producer
export { Producer } from '@/producer/index.js'
export type {
	ProducerConfig,
	ProducerMessage,
	SendResult,
	Acks,
	CompressionName,
	Partitioner,
	PartitionerName,
	PartitionerFunction,
} from '@/producer/index.js'

// Consumer
export { Consumer, ShareConsumer } from '@/consumer/index.js'
export type {
	ConsumerConfig,
	TopicSubscription,
	Message,
	ConsumeContext,
	MessageHandler,
	BatchHandler,
	RunEachOptions,
	RunBatchOptions,
	ShareConsumerConfig,
	ShareSubscriptionInput,
	ShareMessage,
	ShareMessageHandler,
	ShareRunEachOptions,
	ShareConsumerEvents,
} from '@/consumer/index.js'

// Admin
export { Admin } from '@/admin/index.js'
export type {
	AdminConfig,
	TopicDescription,
	PartitionInfo,
	ConsumerGroupListing,
	ConsumerGroupDescription,
	MemberDescription,
	TopicPartition,
	ClusterDescription,
	BrokerDescription,
	CreateTopicsResult,
	DeleteTopicsResult,
	DeleteGroupsResult,
} from '@/admin/index.js'

// Cluster and broker
export { Cluster } from './cluster.js'
export { Broker } from './broker.js'

// Types
export type {
	KafkaClientConfig,
	BrokerConfig,
	BrokerApiVersions,
	VersionRange,
	ClusterMetadata,
	BrokerInfo,
	TopicMetadata,
	PartitionMetadata,
	CoordinatorType,
	CoordinatorEntry,
	ClusterConfig,
	ClusterEvents,
} from './types.js'

// Errors
export {
	KafkaError,
	KafkaProtocolError,
	KafkaFeatureUnsupportedError,
	KafkaFeatureDisabledError,
	LeaderNotAvailableError,
	CoordinatorNotAvailableError,
	NotCoordinatorError,
	UnsupportedVersionError,
	UnknownTopicOrPartitionError,
	BrokerNotAvailableError,
	ConnectionError,
	TimeoutError,
	GroupError,
	RebalanceInProgressError,
	UnknownMemberIdError,
	IllegalGenerationError,
	RecordTooLargeError,
	SendTimeoutError,
	METADATA_REFRESH_ERROR_CODES,
	shouldRefreshMetadata,
	createKafkaError,
	throwIfError,
	isKafkaError,
	isRetriable,
} from './errors.js'
