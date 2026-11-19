/**
 * Failure classification (4.6.1): by error class only. 'migrate' closes every task dirty and rejoins the group
 * (Kafka Streams' task migration), 'corrupted' revives the affected tasks in place, 'fatal' goes to the uncaught
 * exception handler.
 */
import { ErrorCode, isConnectionFailure, KafkaError, NoOffsetForPartitionError, TimeoutError } from '@kafkats/client'
import {
	AssignmentError,
	FailedProcessingError,
	IllegalStateError,
	InvalidTimestampError,
	NoOffsetResetError,
	StreamsError,
	TaskCorruptedError,
	TaskMigratedError,
	TopologyError,
} from '@/api/errors.js'
import { ChangelogDecodeError, ChangelogPartitionMismatchError, CoPartitioningError } from '@/changelog/errors.js'

/** Restart backoff after a migration: doubles from the initial value up to the maximum. */
export const RESTART_INITIAL_BACKOFF_MS = 500
export const RESTART_MAX_BACKOFF_MS = 30_000

export type FailureClass = 'migrate' | 'corrupted' | 'fatal'

/** Where a failure was caught. */
export type FailureSite =
	| 'processing'
	| 'punctuation'
	| 'commit'
	| 'restore'
	| 'assignment'
	| 'consumer-run'
	| 'global'
	| 'purge'

/**
 * Kafka error codes that rejoining cannot fix. ProducerFenced and InvalidProducerEpoch are deliberately not here:
 * the broker answers so after aborting a transaction that outlived `transactionTimeoutMs`, which is
 * indistinguishable from being fenced by another instance; a new producer resumes from the committed offsets.
 */
const FATAL_CODES: ReadonlySet<number> = new Set([
	ErrorCode.FencedInstanceId,
	ErrorCode.TopicAuthorizationFailed,
	ErrorCode.GroupAuthorizationFailed,
	ErrorCode.ClusterAuthorizationFailed,
	ErrorCode.TransactionalIdAuthorizationFailed,
	ErrorCode.SaslAuthenticationFailed,
	ErrorCode.UnsupportedVersion,
	ErrorCode.InvalidSessionTimeout,
	ErrorCode.InconsistentGroupProtocol,
	ErrorCode.InvalidConfig,
	ErrorCode.MessageTooLarge,
	ErrorCode.OffsetMetadataTooLarge,
	ErrorCode.InvalidTopicException,
])

/** Answers of a producer whose epoch is gone: its transaction cannot complete. */
const FENCED_CODES: ReadonlySet<number> = new Set([
	ErrorCode.ProducerFenced,
	ErrorCode.InvalidProducerEpoch,
	ErrorCode.OutOfOrderSequenceNumber,
	ErrorCode.InvalidProducerIdMapping,
])

/**
 * Send failures Kafka Streams never hands to the production handler (`RecordCollectorImpl.isFatalException`):
 * authentication, authorization, an invalid topic, oversized commit metadata. No answer of the handler makes them
 * succeed, and dropping every record that meets them would hide a misconfiguration behind a healthy-looking app.
 */
const FATAL_SEND_CODES: ReadonlySet<number> = new Set([
	ErrorCode.TopicAuthorizationFailed,
	ErrorCode.GroupAuthorizationFailed,
	ErrorCode.ClusterAuthorizationFailed,
	ErrorCode.TransactionalIdAuthorizationFailed,
	ErrorCode.SaslAuthenticationFailed,
	ErrorCode.SecurityDisabled,
	ErrorCode.InvalidTopicException,
	ErrorCode.OffsetMetadataTooLarge,
])

/** Errors no restart can fix: configuration, authorization, topology and user-code failures, bugs. */
const FATAL_CLASSES = [
	AssignmentError,
	ChangelogDecodeError,
	ChangelogPartitionMismatchError,
	CoPartitioningError,
	TopologyError,
	FailedProcessingError,
	InvalidTimestampError,
	NoOffsetResetError,
	NoOffsetForPartitionError,
	IllegalStateError,
] as const

export function isFatalKafkaError(error: unknown): boolean {
	return error instanceof KafkaError && FATAL_CODES.has(error.errorCode)
}

export function isFencedError(error: unknown): boolean {
	return error instanceof KafkaError && FENCED_CODES.has(error.errorCode)
}

export function isFatalSendError(error: unknown): boolean {
	return error instanceof KafkaError && FATAL_SEND_CODES.has(error.errorCode)
}

/** Failures that persisted through the producer's retries but may succeed later (connection, timeout, retriable). */
export function isRetriableError(error: unknown): boolean {
	return (
		isConnectionFailure(error) || error instanceof TimeoutError || (error instanceof KafkaError && error.retriable)
	)
}

export function classify(error: unknown, site: FailureSite): FailureClass {
	if (error instanceof TaskCorruptedError) {
		return 'corrupted'
	}
	if (error instanceof TaskMigratedError) {
		return 'migrate'
	}
	if (isFatalKafkaError(error) || FATAL_CLASSES.some(type => error instanceof type)) {
		return 'fatal'
	}
	// Fencing, aborted transactions, failed commits, timeouts, rebalances, connection failures and any other
	// Kafka error: a new producer and a rejoin start over from the committed offsets.
	if (error instanceof KafkaError || isConnectionFailure(error)) {
		return 'migrate'
	}
	// Flow raises a plain StreamsError around user code outside the processing handler: a processor's init() or
	// close(), a timestamp extractor, a misused forward(). Kafka Streams stops on them (StreamsException).
	if (error instanceof StreamsError) {
		return 'fatal'
	}
	// Any other error escaped user code (it would fail the same way again), unless no user code was on the stack.
	return site === 'processing' || site === 'punctuation' ? 'fatal' : 'migrate'
}

export function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error)
}
