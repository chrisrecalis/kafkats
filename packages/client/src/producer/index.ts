/**
 * Producer module exports
 */

export { Producer } from './producer.js'
export { RecordAccumulator } from './accumulator.js'
export { murmur2, murmur2Partitioner, createRoundRobinPartitioner } from './partitioners/index.js'

// Re-export topic helper and types from shared module (via types.ts)
export { topic, normalizeDecoder } from './types.js'

export type {
	TopicDefinition,
	TopicOptions,
	Decoder,
	DecoderLike,
	ProducerConfig,
	ProducerMessage,
	SendResult,
	Acks,
	CompressionName,
	Partitioner,
	PartitionerName,
	PartitionerFunction,
	ProducerState,
	ProducerEvents,
	ResolvedProducerConfig,
	QueuedMessage,
	PartitionBatch,
} from './types.js'
