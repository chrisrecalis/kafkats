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
