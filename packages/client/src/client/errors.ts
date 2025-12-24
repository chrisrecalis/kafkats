/**
 * Kafka client error hierarchy
 *
 * Provides typed exceptions for Kafka protocol errors with retriable flag
 */

import { ErrorCode, getErrorMessage, getErrorName, isRetriableError } from '@/protocol/messages/error-codes.js'
import type { ApiKey } from '@/protocol/messages/api-keys.js'
import type { CoordinatorType } from './types.js'

/**
 * Base class for all Kafka client errors
 */
export class KafkaError extends Error {
	/** The Kafka error code */
	readonly errorCode: ErrorCode

	/** Whether this error is retriable */
	readonly retriable: boolean

	constructor(message: string, errorCode: ErrorCode = ErrorCode.UnknownServerError, retriable?: boolean) {
		super(message)
		this.name = 'KafkaError'
		this.errorCode = errorCode
		this.retriable = retriable ?? isRetriableError(errorCode)

		// Maintains proper stack trace for where error was thrown (V8 only)
		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, this.constructor)
		}
	}
}

/**
 * Error returned from Kafka broker via protocol response
 */
export class KafkaProtocolError extends KafkaError {
	constructor(errorCode: ErrorCode, context?: string) {
		const name = getErrorName(errorCode)
		const message = getErrorMessage(errorCode)
		const contextStr = context ? ` (${context})` : ''
		super(`${name}: ${message}${contextStr}`, errorCode)
		this.name = 'KafkaProtocolError'
	}
}

/**
 * Leader is not available for partition
 */
export class LeaderNotAvailableError extends KafkaError {
	readonly topic: string
	readonly partition: number

	constructor(topic: string, partition: number) {
		super(`Leader not available for ${topic}-${partition}`, ErrorCode.LeaderNotAvailable)
		this.name = 'LeaderNotAvailableError'
		this.topic = topic
		this.partition = partition
	}
}

/**
 * Coordinator is not available
 */
export class CoordinatorNotAvailableError extends KafkaError {
	readonly coordinatorType: CoordinatorType
	readonly key: string

	constructor(coordinatorType: CoordinatorType, key: string) {
		super(`${coordinatorType} coordinator not available for ${key}`, ErrorCode.CoordinatorNotAvailable)
		this.name = 'CoordinatorNotAvailableError'
		this.coordinatorType = coordinatorType
		this.key = key
	}
}

/**
 * Not the coordinator for this group/transaction
 */
export class NotCoordinatorError extends KafkaError {
	readonly coordinatorType: CoordinatorType
	readonly key: string

	constructor(coordinatorType: CoordinatorType, key: string) {
		super(`Not the ${coordinatorType} coordinator for ${key}`, ErrorCode.NotCoordinator)
		this.name = 'NotCoordinatorError'
		this.coordinatorType = coordinatorType
		this.key = key
	}
}

/**
 * API version not supported by broker
 */
export class UnsupportedVersionError extends KafkaError {
	readonly apiKey: ApiKey
	readonly requestedVersion: number
	readonly supportedRange?: { min: number; max: number }

	constructor(apiKey: ApiKey, requestedVersion: number, supportedRange?: { min: number; max: number }) {
		const rangeStr = supportedRange ? ` (broker supports ${supportedRange.min}-${supportedRange.max})` : ''
		super(
			`API ${apiKey} version ${requestedVersion} not supported${rangeStr}`,
			ErrorCode.UnsupportedVersion,
			false // Not retriable
		)
		this.name = 'UnsupportedVersionError'
		this.apiKey = apiKey
		this.requestedVersion = requestedVersion
		this.supportedRange = supportedRange
	}
}

/**
 * Topic or partition does not exist
 */
export class UnknownTopicOrPartitionError extends KafkaError {
	readonly topic: string
	readonly partition?: number

	constructor(topic: string, partition?: number) {
		const partitionStr = partition !== undefined ? `-${partition}` : ''
		super(`Unknown topic or partition: ${topic}${partitionStr}`, ErrorCode.UnknownTopicOrPartition)
		this.name = 'UnknownTopicOrPartitionError'
		this.topic = topic
		this.partition = partition
	}
}

/**
 * Broker is not available
 */
export class BrokerNotAvailableError extends KafkaError {
	readonly nodeId: number

	constructor(nodeId: number) {
		super(`Broker ${nodeId} is not available`, ErrorCode.BrokerNotAvailable)
		this.name = 'BrokerNotAvailableError'
		this.nodeId = nodeId
	}
}

/**
 * Connection error (not a protocol error)
 */
export class ConnectionError extends KafkaError {
	readonly host: string
	readonly port: number
	override readonly cause?: Error

	constructor(host: string, port: number, cause?: Error) {
		const causeStr = cause ? `: ${cause.message}` : ''
		super(`Connection failed to ${host}:${port}${causeStr}`, ErrorCode.NetworkException)
		this.name = 'ConnectionError'
		this.host = host
		this.port = port
		this.cause = cause
	}
}

/**
 * Request timed out
 */
export class TimeoutError extends KafkaError {
	readonly timeoutMs: number

	constructor(message: string, timeoutMs: number) {
		super(`${message} (timeout: ${timeoutMs}ms)`, ErrorCode.RequestTimedOut)
		this.name = 'TimeoutError'
		this.timeoutMs = timeoutMs
	}
}

/**
 * Group-related errors
 */
export class GroupError extends KafkaError {
	readonly groupId: string

	constructor(message: string, errorCode: ErrorCode, groupId: string) {
		super(message, errorCode)
		this.name = 'GroupError'
		this.groupId = groupId
	}
}

/**
 * Rebalance is in progress
 */
export class RebalanceInProgressError extends GroupError {
	constructor(groupId: string) {
		super(`Rebalance in progress for group ${groupId}`, ErrorCode.RebalanceInProgress, groupId)
		this.name = 'RebalanceInProgressError'
	}
}

/**
 * Unknown member ID
 */
export class UnknownMemberIdError extends GroupError {
	readonly memberId: string

	constructor(groupId: string, memberId: string) {
		super(`Unknown member ${memberId} in group ${groupId}`, ErrorCode.UnknownMemberId, groupId)
		this.name = 'UnknownMemberIdError'
		this.memberId = memberId
	}
}

/**
 * Illegal generation
 */
export class IllegalGenerationError extends GroupError {
	readonly generationId: number

	constructor(groupId: string, generationId: number) {
		super(`Illegal generation ${generationId} for group ${groupId}`, ErrorCode.IllegalGeneration, groupId)
		this.name = 'IllegalGenerationError'
		this.generationId = generationId
	}
}

/**
 * Producer record is too large
 */
export class RecordTooLargeError extends KafkaError {
	readonly topic: string
	readonly partition: number
	readonly size: number
	readonly maxSize: number

	constructor(topic: string, partition: number, size: number, maxSize: number) {
		super(
			`Record too large for ${topic}-${partition}: ${size} bytes exceeds ${maxSize} byte limit`,
			ErrorCode.MessageTooLarge,
			false // Not retriable
		)
		this.name = 'RecordTooLargeError'
		this.topic = topic
		this.partition = partition
		this.size = size
		this.maxSize = maxSize
	}
}

/**
 * Producer send timeout
 */
export class SendTimeoutError extends KafkaError {
	readonly topic: string
	readonly partition: number

	constructor(topic: string, partition: number, timeoutMs: number) {
		super(`Send timeout for ${topic}-${partition} after ${timeoutMs}ms`, ErrorCode.RequestTimedOut)
		this.name = 'SendTimeoutError'
		this.topic = topic
		this.partition = partition
	}
}

/**
 * Producer was fenced by a newer producer instance
 *
 * This error occurs when another producer with the same transactional ID
 * or a producer with the same producer ID but a higher epoch is created.
 * The fenced producer must re-initialize to continue producing.
 */
export class ProducerFencedError extends KafkaError {
	readonly producerId: bigint
	readonly producerEpoch: number

	constructor(producerId: bigint, producerEpoch: number, message?: string) {
		super(
			message ?? `Producer ${producerId} (epoch ${producerEpoch}) has been fenced`,
			ErrorCode.ProducerFenced,
			false // Not retriable - must re-initialize
		)
		this.name = 'ProducerFencedError'
		this.producerId = producerId
		this.producerEpoch = producerEpoch
	}
}

// ==================== Transaction Errors ====================

/**
 * Invalid transaction state error
 *
 * Thrown when a transaction operation is attempted in an invalid state.
 */
export class InvalidTxnStateError extends KafkaError {
	readonly transactionalId: string
	readonly currentState: string
	readonly expectedStates: string[]

	constructor(transactionalId: string, currentState: string, expectedStates: string[]) {
		super(
			`Invalid transaction state for ${transactionalId}: ${currentState}, expected one of [${expectedStates.join(', ')}]`,
			ErrorCode.InvalidTxnState,
			false
		)
		this.name = 'InvalidTxnStateError'
		this.transactionalId = transactionalId
		this.currentState = currentState
		this.expectedStates = expectedStates
	}
}

/**
 * Transaction aborted error
 *
 * Thrown when a transaction is aborted, either due to user code throwing
 * or an error during the transaction.
 */
export class TransactionAbortedError extends KafkaError {
	readonly transactionalId: string
	override readonly cause?: Error

	constructor(transactionalId: string, cause?: Error) {
		super(
			`Transaction aborted for ${transactionalId}${cause ? `: ${cause.message}` : ''}`,
			ErrorCode.None, // Not a Kafka protocol error
			false
		)
		this.name = 'TransactionAbortedError'
		this.transactionalId = transactionalId
		this.cause = cause
	}
}

// ==================== SASL Authentication Errors ====================

/**
 * SASL authentication failed
 */
export class SaslAuthenticationError extends KafkaError {
	readonly mechanism: string
	readonly serverMessage?: string

	constructor(mechanism: string, serverMessage?: string) {
		const msgPart = serverMessage ? `: ${serverMessage}` : ''
		super(`SASL ${mechanism} authentication failed${msgPart}`, ErrorCode.SaslAuthenticationFailed, false)
		this.name = 'SaslAuthenticationError'
		this.mechanism = mechanism
		this.serverMessage = serverMessage
	}
}

/**
 * SASL mechanism not supported by broker
 */
export class UnsupportedSaslMechanismError extends KafkaError {
	readonly requestedMechanism: string
	readonly supportedMechanisms: string[]

	constructor(requestedMechanism: string, supportedMechanisms: string[]) {
		super(
			`SASL mechanism ${requestedMechanism} not supported. Broker supports: ${supportedMechanisms.join(', ')}`,
			ErrorCode.UnsupportedSaslMechanism,
			false
		)
		this.name = 'UnsupportedSaslMechanismError'
		this.requestedMechanism = requestedMechanism
		this.supportedMechanisms = supportedMechanisms
	}
}

/**
 * Illegal SASL state
 */
export class IllegalSaslStateError extends KafkaError {
	constructor(message: string) {
		super(message, ErrorCode.IllegalSaslState, false)
		this.name = 'IllegalSaslStateError'
	}
}

/**
 * Error codes that should trigger a metadata refresh
 */
export const METADATA_REFRESH_ERROR_CODES = new Set<ErrorCode>([
	ErrorCode.UnknownTopicOrPartition,
	ErrorCode.LeaderNotAvailable,
	ErrorCode.NotLeaderOrFollower,
	ErrorCode.BrokerNotAvailable,
	ErrorCode.ReplicaNotAvailable,
	ErrorCode.NotController,
	ErrorCode.CoordinatorNotAvailable,
	ErrorCode.NotCoordinator,
])

/**
 * Check if an error code should trigger a metadata refresh
 */
export function shouldRefreshMetadata(errorCode: ErrorCode): boolean {
	return METADATA_REFRESH_ERROR_CODES.has(errorCode)
}

/**
 * Create a typed KafkaError from an error code
 */
export function createKafkaError(code: ErrorCode, context?: string): KafkaError {
	switch (code) {
		case ErrorCode.None:
			throw new Error('Cannot create error from ErrorCode.None')
		case ErrorCode.LeaderNotAvailable:
			return new KafkaProtocolError(code, context)
		case ErrorCode.CoordinatorNotAvailable:
			return new KafkaProtocolError(code, context)
		case ErrorCode.NotCoordinator:
			return new KafkaProtocolError(code, context)
		case ErrorCode.UnsupportedVersion:
			return new KafkaProtocolError(code, context)
		default:
			return new KafkaProtocolError(code, context)
	}
}

/**
 * Throw a KafkaError if the error code indicates an error
 */
export function throwIfError(errorCode: ErrorCode, context?: string): void {
	if (errorCode !== ErrorCode.None) {
		throw createKafkaError(errorCode, context)
	}
}

/**
 * Check if an error is a KafkaError
 */
export function isKafkaError(error: unknown): error is KafkaError {
	return error instanceof KafkaError
}

/**
 * Check if an error is retriable
 */
export function isRetriable(error: unknown): boolean {
	if (isKafkaError(error)) {
		return error.retriable
	}
	return false
}
