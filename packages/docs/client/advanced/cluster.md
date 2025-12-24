# Cluster API

The Cluster class manages broker connections and metadata discovery. It's used internally by Producer and Consumer but can be accessed directly for advanced use cases.

## Accessing the Cluster

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

// Access the internal cluster
const cluster = client.cluster
```

## Cluster Metadata

Get information about the cluster:

```typescript
// Fetch fresh metadata
const metadata = await cluster.fetchMetadata()

console.log({
	brokers: metadata.brokers,
	topics: metadata.topics,
})
```

### Metadata Structure

```typescript
interface ClusterMetadata {
	brokers: BrokerInfo[]
	topics: TopicMetadata[]
	controllerId: number
}

interface BrokerInfo {
	nodeId: number
	host: string
	port: number
	rack?: string
}

interface TopicMetadata {
	name: string
	partitions: PartitionMetadata[]
	isInternal: boolean
}

interface PartitionMetadata {
	partitionIndex: number
	leader: number
	replicas: number[]
	isr: number[] // In-sync replicas
}
```

## Getting Brokers

```typescript
// Get broker for a specific partition
const broker = await cluster.getBrokerForPartition('my-topic', 0)

// Get the controller broker
const controller = await cluster.getController()

// Get group coordinator
const coordinator = await cluster.findGroupCoordinator('my-group')
```

## Topic Management

### List Topics

```typescript
const metadata = await cluster.fetchMetadata()
const topicNames = metadata.topics.map(t => t.name)
```

### Get Partition Count

```typescript
const metadata = await cluster.fetchMetadata({ topics: ['my-topic'] })
const topic = metadata.topics.find(t => t.name === 'my-topic')
const partitionCount = topic?.partitions.length ?? 0
```

### Get Partition Leaders

```typescript
const metadata = await cluster.fetchMetadata({ topics: ['my-topic'] })
const topic = metadata.topics.find(t => t.name === 'my-topic')

for (const partition of topic?.partitions ?? []) {
	console.log(`Partition ${partition.partitionIndex}: leader=${partition.leader}`)
}
```

## Metadata Refresh

Metadata is cached and refreshed automatically. Force a refresh:

```typescript
// Force metadata refresh
await cluster.refreshMetadata()

// Refresh metadata for specific topics
await cluster.refreshMetadata({ topics: ['topic-a', 'topic-b'] })
```

## Connection Management

The cluster manages a pool of connections to brokers:

```typescript
// Connections are created on-demand and reused
const broker = await cluster.getBrokerForPartition('my-topic', 0)
// broker has an active connection
```

## Advanced Usage

### Direct Broker Access

For low-level operations:

```typescript
const broker = await cluster.getBrokerForPartition('my-topic', 0)

// Use broker APIs directly
const response = await broker.fetch({
	topics: [
		{
			topic: 'my-topic',
			partitions: [{ partition: 0, fetchOffset: 0n }],
		},
	],
})
```

### Custom Metadata Handling

```typescript
// Listen for metadata updates
cluster.on('metadataUpdate', metadata => {
	console.log('Metadata updated:', metadata)
})
```

## Error Handling

```typescript
import { BrokerNotAvailableError, LeaderNotAvailableError } from '@kafkats/client'

try {
	const broker = await cluster.getBrokerForPartition('my-topic', 0)
} catch (error) {
	if (error instanceof LeaderNotAvailableError) {
		// Wait for leader election
		await delay(1000)
		// Retry
	}
}
```

## Best Practices

1. **Let kafkats manage connections** - Use Producer/Consumer APIs when possible
2. **Avoid caching metadata** - It can become stale
3. **Handle retriable errors** - Leader changes are normal
4. **Close properly** - Cluster is closed when client is closed
