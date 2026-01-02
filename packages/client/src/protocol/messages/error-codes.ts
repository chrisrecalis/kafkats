/**
 * Kafka error codes and utilities
 */

/**
 * All Kafka error codes as of Kafka 3.x
 */
export enum ErrorCode {
	// Success
	None = 0,

	// General errors
	UnknownServerError = -1,
	OffsetOutOfRange = 1,
	CorruptMessage = 2,
	UnknownTopicOrPartition = 3,
	InvalidFetchSize = 4,
	LeaderNotAvailable = 5,
	NotLeaderOrFollower = 6,
	RequestTimedOut = 7,
	BrokerNotAvailable = 8,
	ReplicaNotAvailable = 9,
	MessageTooLarge = 10,
	StaleControllerEpoch = 11,
	OffsetMetadataTooLarge = 12,
	NetworkException = 13,
	CoordinatorLoadInProgress = 14,
	CoordinatorNotAvailable = 15,
	NotCoordinator = 16,
	InvalidTopicException = 17,
	RecordListTooLarge = 18,
	NotEnoughReplicas = 19,
	NotEnoughReplicasAfterAppend = 20,
	InvalidRequiredAcks = 21,
	IllegalGeneration = 22,
	InconsistentGroupProtocol = 23,
	InvalidGroupId = 24,
	UnknownMemberId = 25,
	InvalidSessionTimeout = 26,
	RebalanceInProgress = 27,
	InvalidCommitOffsetSize = 28,
	TopicAuthorizationFailed = 29,
	GroupAuthorizationFailed = 30,
	ClusterAuthorizationFailed = 31,
	InvalidTimestamp = 32,
	UnsupportedSaslMechanism = 33,
	IllegalSaslState = 34,
	UnsupportedVersion = 35,
	TopicAlreadyExists = 36,
	InvalidPartitions = 37,
	InvalidReplicationFactor = 38,
	InvalidReplicaAssignment = 39,
	InvalidConfig = 40,
	NotController = 41,
	InvalidRequest = 42,
	UnsupportedForMessageFormat = 43,
	PolicyViolation = 44,
	OutOfOrderSequenceNumber = 45,
	DuplicateSequenceNumber = 46,
	InvalidProducerEpoch = 47,
	InvalidTxnState = 48,
	InvalidProducerIdMapping = 49,
	InvalidTransactionTimeout = 50,
	ConcurrentTransactions = 51,
	TransactionCoordinatorFenced = 52,
	TransactionalIdAuthorizationFailed = 53,
	SecurityDisabled = 54,
	OperationNotAttempted = 55,
	KafkaStorageError = 56,
	LogDirNotFound = 57,
	SaslAuthenticationFailed = 58,
	UnknownProducerId = 59,
	ReassignmentInProgress = 60,
	DelegationTokenAuthDisabled = 61,
	DelegationTokenNotFound = 62,
	DelegationTokenOwnerMismatch = 63,
	DelegationTokenRequestNotAllowed = 64,
	DelegationTokenAuthorizationFailed = 65,
	DelegationTokenExpired = 66,
	InvalidPrincipalType = 67,
	NonEmptyGroup = 68,
	GroupIdNotFound = 69,
	FetchSessionIdNotFound = 70,
	InvalidFetchSessionEpoch = 71,
	ListenerNotFound = 72,
	TopicDeletionDisabled = 73,
	FencedLeaderEpoch = 74,
	UnknownLeaderEpoch = 75,
	UnsupportedCompressionType = 76,
	StaleBrokerEpoch = 77,
	OffsetNotAvailable = 78,
	MemberIdRequired = 79,
	PreferredLeaderNotAvailable = 80,
	GroupMaxSizeReached = 81,
	FencedInstanceId = 82,
	EligibleLeadersNotAvailable = 83,
	ElectionNotNeeded = 84,
	NoReassignmentInProgress = 85,
	GroupSubscribedToTopic = 86,
	InvalidRecord = 87,
	UnstableOffsetCommit = 88,
	ThrottlingQuotaExceeded = 89,
	ProducerFenced = 90,
	ResourceNotFound = 91,
	DuplicateResource = 92,
	UnacceptableCredential = 93,
	InconsistentVoterSet = 94,
	InvalidUpdateVersion = 95,
	FeatureUpdateFailed = 96,
	PrincipalDeserializationFailure = 97,
	SnapshotNotFound = 98,
	PositionOutOfRange = 99,
	UnknownTopicId = 100,
	DuplicateBrokerRegistration = 101,
	BrokerIdNotRegistered = 102,
	InconsistentTopicId = 103,
	InconsistentClusterId = 104,
	TransactionalIdNotFound = 105,
	FetchSessionTopicIdError = 106,
	IneligibleReplica = 107,
	NewLeaderElected = 108,
	InvalidRecordState = 121,
	ShareSessionNotFound = 122,
	InvalidShareSessionEpoch = 123,
	ShareSessionLimitReached = 133,
}

/**
 * Set of retriable error codes
 * These errors indicate transient conditions that may resolve on retry
 */
const RETRIABLE_ERRORS = new Set<ErrorCode>([
	ErrorCode.CorruptMessage,
	ErrorCode.UnknownTopicOrPartition,
	ErrorCode.LeaderNotAvailable,
	ErrorCode.NotLeaderOrFollower,
	ErrorCode.RequestTimedOut,
	ErrorCode.ReplicaNotAvailable,
	ErrorCode.NetworkException,
	ErrorCode.CoordinatorLoadInProgress,
	ErrorCode.CoordinatorNotAvailable,
	ErrorCode.NotCoordinator,
	ErrorCode.NotEnoughReplicas,
	ErrorCode.NotEnoughReplicasAfterAppend,
	ErrorCode.NotController,
	ErrorCode.KafkaStorageError,
	ErrorCode.FetchSessionIdNotFound,
	ErrorCode.InvalidFetchSessionEpoch,
	ErrorCode.ShareSessionNotFound,
	ErrorCode.InvalidShareSessionEpoch,
	ErrorCode.ListenerNotFound,
	ErrorCode.FencedLeaderEpoch,
	ErrorCode.UnknownLeaderEpoch,
	ErrorCode.OffsetNotAvailable,
	ErrorCode.PreferredLeaderNotAvailable,
	ErrorCode.EligibleLeadersNotAvailable,
	ErrorCode.UnstableOffsetCommit,
	ErrorCode.ThrottlingQuotaExceeded,
	ErrorCode.ConcurrentTransactions,
])

/**
 * Check if an error code represents a retriable error
 *
 * @param code - The error code to check
 * @returns true if the error is retriable
 */
export function isRetriableError(code: ErrorCode): boolean {
	return RETRIABLE_ERRORS.has(code)
}

/**
 * Get the human-readable name for an error code
 *
 * @param code - The error code
 * @returns The error name or "Unknown" if not recognized
 */
export function getErrorName(code: ErrorCode): string {
	return ErrorCode[code] ?? `Unknown(${code})`
}

/**
 * Get a descriptive message for an error code
 *
 * @param code - The error code
 * @returns A descriptive error message
 */
export function getErrorMessage(code: ErrorCode): string {
	const messages: Partial<Record<ErrorCode, string>> = {
		[ErrorCode.None]: 'No error',
		[ErrorCode.UnknownServerError]: 'The server experienced an unexpected error',
		[ErrorCode.OffsetOutOfRange]: 'The requested offset is not within the range of offsets',
		[ErrorCode.CorruptMessage]: 'The message is corrupt',
		[ErrorCode.UnknownTopicOrPartition]: 'The topic or partition does not exist',
		[ErrorCode.InvalidFetchSize]: 'The fetch size is invalid',
		[ErrorCode.LeaderNotAvailable]: 'Leader not available for partition',
		[ErrorCode.NotLeaderOrFollower]: 'This server is not the leader for that partition',
		[ErrorCode.RequestTimedOut]: 'The request timed out',
		[ErrorCode.BrokerNotAvailable]: 'The broker is not available',
		[ErrorCode.ReplicaNotAvailable]: 'The replica is not available',
		[ErrorCode.MessageTooLarge]: 'The message is too large',
		[ErrorCode.CoordinatorLoadInProgress]: 'The coordinator is loading and hence cannot process requests',
		[ErrorCode.CoordinatorNotAvailable]: 'The coordinator is not available',
		[ErrorCode.NotCoordinator]: 'This is not the correct coordinator',
		[ErrorCode.InvalidTopicException]: 'The topic name is invalid',
		[ErrorCode.NotEnoughReplicas]: 'Messages are rejected since there are fewer in-sync replicas than required',
		[ErrorCode.NotEnoughReplicasAfterAppend]:
			'Messages are written to the log, but with fewer in-sync replicas than required',
		[ErrorCode.IllegalGeneration]: 'The generation id is illegal',
		[ErrorCode.InconsistentGroupProtocol]: 'The group protocol is inconsistent',
		[ErrorCode.InvalidGroupId]: 'The group id is invalid',
		[ErrorCode.UnknownMemberId]: 'The member id is unknown',
		[ErrorCode.InvalidSessionTimeout]: 'The session timeout is invalid',
		[ErrorCode.RebalanceInProgress]: 'The group is rebalancing',
		[ErrorCode.TopicAuthorizationFailed]: 'Topic authorization failed',
		[ErrorCode.GroupAuthorizationFailed]: 'Group authorization failed',
		[ErrorCode.ClusterAuthorizationFailed]: 'Cluster authorization failed',
		[ErrorCode.UnsupportedVersion]: 'The broker does not support the requested API version',
		[ErrorCode.TopicAlreadyExists]: 'Topic already exists',
		[ErrorCode.InvalidPartitions]: 'Invalid number of partitions',
		[ErrorCode.InvalidReplicationFactor]: 'Invalid replication factor',
		[ErrorCode.SaslAuthenticationFailed]: 'SASL authentication failed',
		[ErrorCode.MemberIdRequired]:
			'The group member needs to have a valid member id before actually entering a consumer group',
		[ErrorCode.FencedInstanceId]: 'The member with the given instance id has been fenced',
		[ErrorCode.InvalidRecordState]:
			'The record state is invalid. The acknowledgement of delivery could not be completed',
		[ErrorCode.ShareSessionNotFound]: 'The share session was not found',
		[ErrorCode.InvalidShareSessionEpoch]: 'The share session epoch is invalid',
		[ErrorCode.ShareSessionLimitReached]: 'The limit of share sessions has been reached',
	}

	return messages[code] ?? `Unknown error code: ${code}`
}

/**
 * Create an Error object from an error code
 *
 * @param code - The error code
 * @param context - Optional context information
 * @returns An Error object with appropriate message
 */
export function createError(code: ErrorCode, context?: string): Error {
	const name = getErrorName(code)
	const message = getErrorMessage(code)
	const contextStr = context ? ` (${context})` : ''
	return new Error(`${name}: ${message}${contextStr}`)
}
