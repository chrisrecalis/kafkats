import { KafkaError, ErrorCode, isConnectionFailure } from '@kafkats/client'

/** Restart backoff after a recoverable failure: doubles from the initial value up to the maximum. */
export const RESTART_INITIAL_BACKOFF_MS = 500
export const RESTART_MAX_BACKOFF_MS = 30_000

/**
 * Error codes that restarting a worker cannot fix.
 *
 * ProducerFenced / InvalidProducerEpoch are deliberately not here: the broker answers with them
 * after it aborted a transaction that outlived `transactionTimeoutMs` (a stalled broker, a long
 * GC pause), which is indistinguishable from being fenced by another instance. Either way the
 * transaction is gone, so the worker drops its uncommitted state, re-initializes the producer and
 * resumes from the committed offsets.
 */
const FATAL_ERROR_CODES = new Set<number>([
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
])

function isFatalKafkaError(error: unknown): boolean {
	return error instanceof KafkaError && FATAL_ERROR_CODES.has(error.errorCode)
}

/**
 * Whether a worker can recover from an error raised while running the topology by restarting
 * (rejoining the group and resuming from the last committed offsets). Only Kafka and connection errors qualify:
 * an error thrown by user code would fail the same way on the same record again.
 */
export function isRecoverable(error: unknown): boolean {
	// A dropped or unreachable broker connection surfaces as a network error, not a KafkaError.
	return isConnectionFailure(error) || (error instanceof KafkaError && !isFatalKafkaError(error))
}

/**
 * Whether a worker can recover from an infrastructure failure (commit, rebalance, fetch, restore).
 * No user code is involved, so anything except a known-fatal Kafka error is worth a restart.
 */
export function isInfrastructureRecoverable(error: unknown): boolean {
	return !isFatalKafkaError(error)
}

export function toError(err: unknown): Error {
	return err instanceof Error ? err : new Error(String(err))
}
