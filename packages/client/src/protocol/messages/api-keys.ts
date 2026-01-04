/**
 * Kafka API key definitions and version information
 */

/**
 * All Kafka API keys as of Kafka 3.x
 */
export enum ApiKey {
	Produce = 0,
	Fetch = 1,
	ListOffsets = 2,
	Metadata = 3,
	LeaderAndIsr = 4,
	StopReplica = 5,
	UpdateMetadata = 6,
	ControlledShutdown = 7,
	OffsetCommit = 8,
	OffsetFetch = 9,
	FindCoordinator = 10,
	JoinGroup = 11,
	Heartbeat = 12,
	LeaveGroup = 13,
	SyncGroup = 14,
	DescribeGroups = 15,
	ListGroups = 16,
	SaslHandshake = 17,
	ApiVersions = 18,
	CreateTopics = 19,
	DeleteTopics = 20,
	DeleteRecords = 21,
	InitProducerId = 22,
	OffsetForLeaderEpoch = 23,
	AddPartitionsToTxn = 24,
	AddOffsetsToTxn = 25,
	EndTxn = 26,
	WriteTxnMarkers = 27,
	TxnOffsetCommit = 28,
	DescribeAcls = 29,
	CreateAcls = 30,
	DeleteAcls = 31,
	DescribeConfigs = 32,
	AlterConfigs = 33,
	AlterReplicaLogDirs = 34,
	DescribeLogDirs = 35,
	SaslAuthenticate = 36,
	CreatePartitions = 37,
	CreateDelegationToken = 38,
	RenewDelegationToken = 39,
	ExpireDelegationToken = 40,
	DescribeDelegationToken = 41,
	DeleteGroups = 42,
	ElectLeaders = 43,
	IncrementalAlterConfigs = 44,
	AlterPartitionReassignments = 45,
	ListPartitionReassignments = 46,
	OffsetDelete = 47,
	DescribeClientQuotas = 48,
	AlterClientQuotas = 49,
	DescribeUserScramCredentials = 50,
	AlterUserScramCredentials = 51,
	DescribeQuorum = 55,
	AlterPartition = 56,
	UpdateFeatures = 57,
	Envelope = 58,
	DescribeCluster = 60,
	DescribeProducers = 61,
	DescribeTransactions = 65,
	ListTransactions = 66,
	AllocateProducerIds = 67,
	ShareGroupHeartbeat = 76,
	ShareGroupDescribe = 77,
	ShareFetch = 78,
	ShareAcknowledge = 79,
}

/**
 * API version range information
 */
export interface ApiVersionRange {
	apiKey: ApiKey
	minVersion: number
	maxVersion: number
}

/**
 * Minimum flexible version for each API
 * Flexible versions use compact encoding (UVARINT prefixes, tagged fields)
 *
 * Note: SaslHandshake v1 is NOT flexible despite being version 1
 */
export const FLEXIBLE_VERSION_MIN: Partial<Record<ApiKey, number>> = {
	[ApiKey.Produce]: 9,
	[ApiKey.Fetch]: 12,
	[ApiKey.ListOffsets]: 6,
	[ApiKey.Metadata]: 9,
	[ApiKey.OffsetCommit]: 8,
	[ApiKey.OffsetFetch]: 6,
	[ApiKey.FindCoordinator]: 3,
	[ApiKey.JoinGroup]: 6,
	[ApiKey.Heartbeat]: 4,
	[ApiKey.LeaveGroup]: 4,
	[ApiKey.SyncGroup]: 4,
	[ApiKey.DescribeGroups]: 5,
	[ApiKey.ListGroups]: 3,
	[ApiKey.ApiVersions]: 3,
	[ApiKey.CreateTopics]: 5,
	[ApiKey.DeleteTopics]: 4,
	[ApiKey.InitProducerId]: 2,
	[ApiKey.OffsetForLeaderEpoch]: 4,
	[ApiKey.AddPartitionsToTxn]: 3,
	[ApiKey.AddOffsetsToTxn]: 3,
	[ApiKey.EndTxn]: 3,
	[ApiKey.TxnOffsetCommit]: 3,
	[ApiKey.DescribeAcls]: 2,
	[ApiKey.CreateAcls]: 2,
	[ApiKey.DeleteAcls]: 2,
	[ApiKey.DescribeConfigs]: 4,
	[ApiKey.AlterConfigs]: 2,
	[ApiKey.AlterReplicaLogDirs]: 2,
	[ApiKey.DescribeLogDirs]: 2,
	[ApiKey.SaslAuthenticate]: 2,
	[ApiKey.CreatePartitions]: 2,
	[ApiKey.CreateDelegationToken]: 2,
	[ApiKey.RenewDelegationToken]: 2,
	[ApiKey.ExpireDelegationToken]: 2,
	[ApiKey.DescribeDelegationToken]: 2,
	[ApiKey.DeleteGroups]: 2,
	[ApiKey.ElectLeaders]: 2,
	[ApiKey.IncrementalAlterConfigs]: 1,
	[ApiKey.AlterPartitionReassignments]: 0,
	[ApiKey.ListPartitionReassignments]: 0,
	[ApiKey.OffsetDelete]: 0,
	[ApiKey.DescribeClientQuotas]: 1,
	[ApiKey.AlterClientQuotas]: 1,
	[ApiKey.DescribeUserScramCredentials]: 0,
	[ApiKey.AlterUserScramCredentials]: 0,
	[ApiKey.DescribeQuorum]: 0,
	[ApiKey.AlterPartition]: 0,
	[ApiKey.UpdateFeatures]: 0,
	[ApiKey.Envelope]: 0,
	[ApiKey.DescribeCluster]: 0,
	[ApiKey.DescribeProducers]: 0,
	[ApiKey.DescribeTransactions]: 0,
	[ApiKey.ListTransactions]: 0,
	[ApiKey.AllocateProducerIds]: 0,
	// Share Groups (Kafka 4.1+)
	[ApiKey.ShareGroupHeartbeat]: 1,
	[ApiKey.ShareFetch]: 1,
	[ApiKey.ShareAcknowledge]: 1,
}

/**
 * Check if a specific API version uses flexible encoding
 *
 * @param apiKey - The API key to check
 * @param version - The API version to check
 * @returns true if the version uses flexible encoding
 */
export function isFlexibleVersion(apiKey: ApiKey, version: number): boolean {
	const minFlexible = FLEXIBLE_VERSION_MIN[apiKey]
	return minFlexible !== undefined && version >= minFlexible
}

/**
 * Get the human-readable name for an API key
 */
export function getApiName(apiKey: ApiKey): string {
	return ApiKey[apiKey] ?? `Unknown(${apiKey})`
}
