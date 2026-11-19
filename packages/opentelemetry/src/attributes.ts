/**
 * Attribute and metric names of OpenTelemetry's messaging semantic conventions. They are still
 * marked as in development there and have been renamed before, which is why they live here and
 * not in @kafkats/client.
 */
export const ATTR = {
	system: 'messaging.system',
	topic: 'messaging.destination.name',
	partition: 'messaging.destination.partition.id',
	operationName: 'messaging.operation.name',
	operationType: 'messaging.operation.type',
	consumerGroup: 'messaging.consumer.group.name',
	clientId: 'messaging.client.id',
	batchSize: 'messaging.batch.message_count',
	offset: 'messaging.kafka.offset',
	errorType: 'error.type',
	/** Not part of the conventions: the broker a request went to. */
	brokerId: 'kafkats.broker.id',
	/** Not part of the conventions: the `@kafkats/flow` task that processed a record (`<subtopology>_<partition>`). */
	taskId: 'kafkats.flow.task.id',
} as const

export const SYSTEM_KAFKA = 'kafka'

/** The metrics the messaging conventions define. */
export const METRIC = {
	sentMessages: 'messaging.client.sent.messages',
	consumedMessages: 'messaging.client.consumed.messages',
	operationDuration: 'messaging.client.operation.duration',
	processDuration: 'messaging.process.duration',
} as const

/**
 * Metrics the conventions have no name for. All of them describe the inside of this client:
 * things a broker, and therefore a broker exporter, cannot see.
 */
export const KAFKATS_METRIC = {
	requestDuration: 'kafkats.producer.request.duration',
	requestRecords: 'kafkats.producer.request.records',
	queuedRecords: 'kafkats.producer.queue.records',
	queuedBytes: 'kafkats.producer.queue.bytes',
	inFlightRequests: 'kafkats.producer.requests.in_flight',
	unacknowledgedSends: 'kafkats.producer.sends.unacknowledged',
	rebalanceDuration: 'kafkats.consumer.rebalance.duration',
	positionLag: 'kafkats.consumer.position.lag',
	bufferedBytes: 'kafkats.consumer.buffered.bytes',
	assignedPartitions: 'kafkats.consumer.partitions.assigned',
	pausedPartitions: 'kafkats.consumer.partitions.paused',
} as const

/** The `error.type` of a failed operation: the error's class name. */
export function errorType(error: unknown): string {
	return error instanceof Error ? error.name : 'unknown'
}
