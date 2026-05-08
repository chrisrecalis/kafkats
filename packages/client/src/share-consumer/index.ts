/** ShareConsumer module exports (Kafka Share Groups / KIP-932, GA in Kafka 4.2). */

export { ShareConsumer } from './share-consumer.js'

export type {
	ConsumeContext,
	ShareConsumerAcquireMode,
	ShareConsumerConfig,
	ShareConsumerEvents,
	ShareKeyOf,
	ShareMessage,
	ShareMessageHandler,
	ShareMsgOf,
	ShareRunEachOptions,
	ShareSubscriptionInput,
	TopicPartition,
	TopicSubscription,
} from './types.js'

export { DEFAULT_SHARE_CONSUMER_CONFIG, DEFAULT_SHARE_RUN_EACH_OPTIONS } from './types.js'
