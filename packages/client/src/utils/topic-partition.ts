/**
 * Topic-partition key utilities
 *
 * Provides consistent key generation and parsing for topic:partition identifiers
 * used across consumer components (assignors, fetch manager, offset manager).
 */

/**
 * Topic-partition identifier
 */
export interface TopicPartition {
	topic: string
	partition: number
}

/**
 * Create a unique string key for a topic-partition pair
 */
export function tpKey(topic: string, partition: number): string {
	return `${topic}:${partition}`
}

/**
 * Parse a topic-partition key back into its components
 */
export function parseKey(key: string): TopicPartition {
	const [topic, partitionStr] = key.split(':')
	return { topic: topic!, partition: parseInt(partitionStr!, 10) }
}
