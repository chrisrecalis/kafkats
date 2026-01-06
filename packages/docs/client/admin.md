# Admin API

The admin client provides cluster management operations for topics, consumer groups, and cluster metadata.

## Creating an Admin Client

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

await client.connect()

const admin = client.admin()
```

The admin client shares the underlying cluster connection with the `KafkaClient`, so no additional connections are created.

## Topic Operations

### Fetching Topic Offsets

Get the earliest or latest offsets for a topic's partitions:

```typescript
const offsets = await admin.fetchTopicOffsets('events', [0, 1, 2], 'latest')

for (const [partition, offset] of offsets) {
	console.log(`Partition ${partition}: offset ${offset}`)
}
```

Fetch earliest offsets:

```typescript
const earliestOffsets = await admin.fetchTopicOffsets('events', [0, 1, 2], 'earliest')
```

With isolation level for transactional topics:

```typescript
// Get the last stable offset (LSO) - only committed transactional messages
const committedOffsets = await admin.fetchTopicOffsets('events', [0, 1, 2], 'latest', {
	isolationLevel: 'read_committed',
})
```

#### Options

| Option           | Type                                     | Default            | Description                                   |
| ---------------- | ---------------------------------------- | ------------------ | --------------------------------------------- |
| `topic`          | `string`                                 | -                  | Topic name                                    |
| `partitions`     | `number[]`                               | -                  | Partition indices to fetch offsets for        |
| `which`          | `'earliest' \| 'latest'`                 | -                  | Which offset to fetch                         |
| `isolationLevel` | `'read_uncommitted' \| 'read_committed'` | `read_uncommitted` | Controls visibility of transactional messages |

### Listing Topics

Get a list of all topic names in the cluster:

```typescript
const topics = await admin.listTopics()
console.log(topics) // ['orders', 'events', 'logs', ...]
```

### Describing Topics

Get detailed metadata for specific topics:

```typescript
const descriptions = await admin.describeTopics(['orders', 'events'])

for (const topic of descriptions) {
	console.log({
		name: topic.name,
		topicId: topic.topicId,
		isInternal: topic.isInternal,
		partitionCount: topic.partitions.length,
	})

	for (const partition of topic.partitions) {
		console.log({
			partition: partition.partitionIndex,
			leader: partition.leaderId,
			replicas: partition.replicas,
			isr: partition.isr,
		})
	}
}
```

Describe all topics by omitting the argument:

```typescript
const allTopics = await admin.describeTopics()
```

### Topic Description Structure

```typescript
interface TopicDescription {
	name: string // Topic name
	topicId: string // Topic UUID
	isInternal: boolean // Internal Kafka topic
	partitions: PartitionInfo[]
}

interface PartitionInfo {
	partitionIndex: number // Partition number
	leaderId: number // Leader broker ID
	leaderEpoch: number // Leader epoch
	replicas: number[] // Replica broker IDs
	isr: number[] // In-sync replica IDs
	offlineReplicas: number[] // Offline replica IDs
}
```

### Creating Topics

Create topics in the cluster:

```typescript
const results = await admin.createTopics([
	{
		name: 'orders',
		numPartitions: 6,
		replicationFactor: 3,
		configs: {
			'retention.ms': '604800000', // 7 days
			'cleanup.policy': 'delete',
		},
	},
])

for (const result of results) {
	if (result.errorCode === 0) {
		console.log(`Created topic: ${result.name}`)
	} else {
		console.log(`Failed to create ${result.name}: ${result.errorMessage}`)
	}
}
```

Validate topic configuration without creating:

```typescript
const results = await admin.createTopics([{ name: 'test-topic', numPartitions: 3 }], { validateOnly: true })
```

### Deleting Topics

Delete topics from the cluster:

```typescript
const results = await admin.deleteTopics(['old-topic', 'unused-topic'])

for (const result of results) {
	if (result.errorCode === 0) {
		console.log(`Deleted topic: ${result.name}`)
	} else {
		console.log(`Failed to delete ${result.name}: ${result.errorMessage}`)
	}
}
```

::: warning
Topic deletion is irreversible. All data in the topic will be lost.
:::

## Consumer Group Operations

### Listing Consumer Groups

List all consumer groups in the cluster:

```typescript
const groups = await admin.listGroups()

for (const group of groups) {
	console.log({
		groupId: group.groupId,
		protocolType: group.protocolType, // 'consumer'
		state: group.state, // 'Stable', 'Empty', etc.
	})
}
```

Filter groups by state:

```typescript
const stableGroups = await admin.listGroups({
	statesFilter: ['Stable'],
})
```

### Describing Consumer Groups

Get detailed information about consumer groups:

```typescript
const descriptions = await admin.describeGroups(['my-consumer-group'])

for (const group of descriptions) {
	console.log({
		groupId: group.groupId,
		state: group.state,
		protocol: group.protocol,
		members: group.members.length,
	})

	for (const member of group.members) {
		console.log({
			memberId: member.memberId,
			clientId: member.clientId,
			clientHost: member.clientHost,
			assignment: member.assignment, // Topic-partitions
		})
	}
}
```

### Consumer Group Description Structure

```typescript
interface ConsumerGroupDescription {
	groupId: string // Group ID
	state: string // 'Stable', 'Empty', 'Dead', etc.
	protocolType: string // Usually 'consumer'
	protocol: string // Assignment strategy name
	members: MemberDescription[]
	errorCode: number // 0 if successful
}

interface MemberDescription {
	memberId: string // Unique member ID
	groupInstanceId: string | null // Static membership ID
	clientId: string // Client identifier
	clientHost: string // Member host
	assignment: TopicPartition[] // Assigned partitions
}

interface TopicPartition {
	topic: string
	partition: number
}
```

### Deleting Consumer Groups

Delete consumer groups that have no active members:

```typescript
const results = await admin.deleteGroups(['old-group'])

for (const result of results) {
	if (result.errorCode === 0) {
		console.log(`Deleted group: ${result.groupId}`)
	} else {
		console.log(`Failed to delete ${result.groupId}`)
	}
}
```

::: warning
Consumer groups can only be deleted when they have no active members. Attempting to delete a non-empty group returns `NonEmptyGroup` error.
:::

## Cluster Operations

### Describing the Cluster

Get cluster metadata including broker information:

```typescript
const cluster = await admin.describeCluster()

console.log({
	clusterId: cluster.clusterId,
	controllerId: cluster.controllerId,
	brokerCount: cluster.brokers.length,
})

for (const broker of cluster.brokers) {
	console.log({
		nodeId: broker.nodeId,
		host: broker.host,
		port: broker.port,
		rack: broker.rack,
	})
}
```

### Cluster Description Structure

```typescript
interface ClusterDescription {
	clusterId: string | null // Cluster identifier
	controllerId: number // Controller broker ID
	brokers: BrokerDescription[]
}

interface BrokerDescription {
	nodeId: number // Broker ID
	host: string // Broker host
	port: number // Broker port
	rack: string | null // Rack identifier
}
```

## ACL Operations

Access Control Lists (ACLs) control which users can perform which operations on which resources.

### Describing ACLs

Query ACLs matching a filter:

```typescript
import { AclResourceType, AclResourcePatternType, AclOperation, AclPermissionType } from '@kafkats/client'

// Describe all ACLs for a specific topic
const result = await admin.describeAcls({
	resourceTypeFilter: AclResourceType.TOPIC,
	resourceNameFilter: 'my-topic',
	patternTypeFilter: AclResourcePatternType.LITERAL,
	principalFilter: null, // null matches any
	hostFilter: null,
	operation: AclOperation.ANY,
	permissionType: AclPermissionType.ANY,
})

for (const resource of result.resources) {
	console.log(`${resource.resourceType}: ${resource.resourceName}`)
	for (const acl of resource.acls) {
		console.log(`  ${acl.principal} ${acl.permissionType} ${acl.operation}`)
	}
}
```

Describe all ACLs in the cluster:

```typescript
const result = await admin.describeAcls({
	resourceTypeFilter: AclResourceType.ANY,
	resourceNameFilter: null,
	patternTypeFilter: AclResourcePatternType.ANY,
	principalFilter: null,
	hostFilter: null,
	operation: AclOperation.ANY,
	permissionType: AclPermissionType.ANY,
})
```

### Creating ACLs

Create ACL bindings to grant or deny access:

```typescript
// Allow User:alice to read from my-topic
const results = await admin.createAcls([
	{
		resourceType: AclResourceType.TOPIC,
		resourceName: 'my-topic',
		resourcePatternType: AclResourcePatternType.LITERAL,
		principal: 'User:alice',
		host: '*',
		operation: AclOperation.READ,
		permissionType: AclPermissionType.ALLOW,
	},
])

for (const result of results) {
	if (result.errorCode === 0) {
		console.log('ACL created successfully')
	} else {
		console.log(`Failed: ${result.errorMessage}`)
	}
}
```

Create prefixed ACLs to match multiple resources:

```typescript
// Allow User:bob to write to all topics starting with "events-"
await admin.createAcls([
	{
		resourceType: AclResourceType.TOPIC,
		resourceName: 'events-',
		resourcePatternType: AclResourcePatternType.PREFIXED,
		principal: 'User:bob',
		host: '*',
		operation: AclOperation.WRITE,
		permissionType: AclPermissionType.ALLOW,
	},
])
```

### Deleting ACLs

Delete ACLs matching a filter:

```typescript
// Delete all ACLs for User:alice on my-topic
const results = await admin.deleteAcls([
	{
		resourceTypeFilter: AclResourceType.TOPIC,
		resourceNameFilter: 'my-topic',
		patternTypeFilter: AclResourcePatternType.LITERAL,
		principalFilter: 'User:alice',
		hostFilter: null,
		operation: AclOperation.ANY,
		permissionType: AclPermissionType.ANY,
	},
])

for (const result of results) {
	console.log(`Deleted ${result.matchingAcls.length} ACLs`)
}
```

### ACL Types

#### Resource Types

| Type               | Description              |
| ------------------ | ------------------------ |
| `TOPIC`            | Topic resource           |
| `GROUP`            | Consumer group           |
| `CLUSTER`          | Cluster-level operations |
| `TRANSACTIONAL_ID` | Transactional ID         |
| `DELEGATION_TOKEN` | Delegation token         |
| `ANY`              | Match any (for filters)  |

#### Operations

| Operation          | Description             |
| ------------------ | ----------------------- |
| `READ`             | Read from resource      |
| `WRITE`            | Write to resource       |
| `CREATE`           | Create resource         |
| `DELETE`           | Delete resource         |
| `ALTER`            | Alter resource          |
| `DESCRIBE`         | Describe resource       |
| `CLUSTER_ACTION`   | Cluster actions         |
| `DESCRIBE_CONFIGS` | Describe configs        |
| `ALTER_CONFIGS`    | Alter configs           |
| `IDEMPOTENT_WRITE` | Idempotent writes       |
| `ALL`              | All operations          |
| `ANY`              | Match any (for filters) |

#### Pattern Types

| Type       | Description                |
| ---------- | -------------------------- |
| `LITERAL`  | Exact resource name match  |
| `PREFIXED` | Resource name prefix match |
| `ANY`      | Match any (for filters)    |

#### Permission Types

| Type    | Description             |
| ------- | ----------------------- |
| `ALLOW` | Allow the operation     |
| `DENY`  | Deny the operation      |
| `ANY`   | Match any (for filters) |

## Error Handling

Admin operations return results with error codes for each item:

```typescript
import { ErrorCode } from '@kafkats/client'

const results = await admin.deleteTopics(['my-topic'])

for (const result of results) {
	switch (result.errorCode) {
		case ErrorCode.None:
			console.log(`Success: ${result.name}`)
			break
		case ErrorCode.UnknownTopicOrPartition:
			console.log(`Topic not found: ${result.name}`)
			break
		case ErrorCode.TopicAuthorizationFailed:
			console.log(`Not authorized: ${result.name}`)
			break
		default:
			console.log(`Error ${result.errorCode}: ${result.errorMessage}`)
	}
}
```

### Common Error Codes

| Error Code                   | Description                          |
| ---------------------------- | ------------------------------------ |
| `None` (0)                   | Operation succeeded                  |
| `UnknownTopicOrPartition`    | Topic does not exist                 |
| `TopicAlreadyExists`         | Topic already exists (create)        |
| `NonEmptyGroup`              | Group has active members (delete)    |
| `GroupIdNotFound`            | Group does not exist                 |
| `TopicAuthorizationFailed`   | Not authorized for topic operation   |
| `GroupAuthorizationFailed`   | Not authorized for group operation   |
| `ClusterAuthorizationFailed` | Not authorized for cluster operation |

## Admin Options

Configure admin behavior:

```typescript
const admin = client.admin({
	requestTimeoutMs: 30000, // Timeout for admin operations (default: 30s)
})
```

| Option             | Type     | Default | Description                     |
| ------------------ | -------- | ------- | ------------------------------- |
| `requestTimeoutMs` | `number` | `30000` | Timeout for admin requests (ms) |

## Next Steps

- [Error Handling](/client/errors) - Error types and recovery
- [Cluster API](/client/advanced/cluster) - Low-level cluster operations
- [Configuration](/client/configuration) - Full configuration reference
