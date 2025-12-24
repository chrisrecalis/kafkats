# Broker API

The Broker class provides typed protocol operations on a single Kafka broker connection. It's the lowest-level API for direct Kafka protocol access.

## Getting a Broker

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

// Get broker for a partition
const broker = await client.cluster.getBrokerForPartition('my-topic', 0)

// Get group coordinator
const coordinator = await client.cluster.findGroupCoordinator('my-group')
```

## Produce API

Low-level produce request:

```typescript
import { RecordBatch } from '@kafkats/client'

const response = await broker.produce({
	topics: [
		{
			topic: 'my-topic',
			partitions: [
				{
					partition: 0,
					records: recordBatch,
				},
			],
		},
	],
	acks: -1, // Wait for all replicas
	timeoutMs: 30000,
})

for (const topic of response.topics) {
	for (const partition of topic.partitions) {
		console.log(`Offset: ${partition.baseOffset}`)
	}
}
```

## Fetch API

Low-level fetch request:

```typescript
const response = await broker.fetch({
	topics: [
		{
			topic: 'my-topic',
			partitions: [
				{
					partition: 0,
					fetchOffset: 0n,
					maxBytes: 1048576,
				},
			],
		},
	],
	maxWaitMs: 5000,
	minBytes: 1,
	maxBytes: 10485760,
	isolationLevel: 0,
})

for (const topic of response.topics) {
	for (const partition of topic.partitions) {
		console.log(`High watermark: ${partition.highWatermark}`)
		// Process partition.records
	}
}
```

## Metadata API

```typescript
const metadata = await broker.metadata({
	topics: ['my-topic'],
	allowAutoTopicCreation: false,
})

for (const topic of metadata.topics) {
	console.log(`Topic: ${topic.name}, Partitions: ${topic.partitions.length}`)
}
```

## Offset APIs

### List Offsets

```typescript
const response = await broker.listOffsets({
	topics: [
		{
			topic: 'my-topic',
			partitions: [
				{
					partition: 0,
					timestamp: -1n, // Latest offset
				},
			],
		},
	],
})

// -1n = latest, -2n = earliest
```

### Commit Offsets

```typescript
await broker.offsetCommit({
	groupId: 'my-group',
	generationId: 1,
	memberId: 'member-id',
	topics: [
		{
			topic: 'my-topic',
			partitions: [
				{
					partition: 0,
					committedOffset: 100n,
				},
			],
		},
	],
})
```

### Fetch Committed Offsets

```typescript
const response = await broker.offsetFetch({
	groupId: 'my-group',
	topics: [
		{
			topic: 'my-topic',
			partitions: [0, 1, 2],
		},
	],
})
```

## Consumer Group APIs

### Find Coordinator

```typescript
const response = await broker.findCoordinator({
	key: 'my-group',
	keyType: 0, // 0 = group, 1 = transaction
})

console.log(`Coordinator: ${response.host}:${response.port}`)
```

### Join Group

```typescript
const response = await broker.joinGroup({
	groupId: 'my-group',
	sessionTimeoutMs: 30000,
	rebalanceTimeoutMs: 60000,
	memberId: '',
	protocolType: 'consumer',
	protocols: [
		{
			name: 'range',
			metadata: subscriptionMetadata,
		},
	],
})

console.log(`Member ID: ${response.memberId}`)
console.log(`Leader: ${response.leader}`)
```

### Sync Group

```typescript
const response = await broker.syncGroup({
	groupId: 'my-group',
	generationId: 1,
	memberId: 'member-id',
	assignments: [
		{
			memberId: 'member-id',
			assignment: assignmentData,
		},
	],
})
```

### Heartbeat

```typescript
await broker.heartbeat({
	groupId: 'my-group',
	generationId: 1,
	memberId: 'member-id',
})
```

### Leave Group

```typescript
await broker.leaveGroup({
	groupId: 'my-group',
	members: [
		{
			memberId: 'member-id',
		},
	],
})
```

## Transaction APIs

### Init Producer ID

```typescript
const response = await broker.initProducerId({
	transactionalId: 'my-txn',
	transactionTimeoutMs: 60000,
})

console.log(`Producer ID: ${response.producerId}`)
console.log(`Producer Epoch: ${response.producerEpoch}`)
```

### Add Partitions to Transaction

```typescript
await broker.addPartitionsToTxn({
	transactionalId: 'my-txn',
	producerId: 123n,
	producerEpoch: 0,
	topics: [
		{
			topic: 'my-topic',
			partitions: [0, 1],
		},
	],
})
```

### End Transaction

```typescript
await broker.endTxn({
	transactionalId: 'my-txn',
	producerId: 123n,
	producerEpoch: 0,
	committed: true, // or false to abort
})
```

## API Versions

Check supported API versions:

```typescript
const versions = await broker.apiVersions()

for (const api of versions.apiKeys) {
	console.log(`API ${api.apiKey}: versions ${api.minVersion}-${api.maxVersion}`)
}
```

## Error Handling

All broker operations can throw protocol errors:

```typescript
import { KafkaProtocolError } from '@kafkats/client'

try {
  await broker.produce({...})
} catch (error) {
  if (error instanceof KafkaProtocolError) {
    console.log('Error code:', error.code)
    console.log('Error message:', error.message)
  }
}
```

## Best Practices

1. **Use high-level APIs** - Producer/Consumer handle retries and metadata
2. **Check API versions** - Not all brokers support all APIs
3. **Handle errors** - Broker operations can fail for many reasons
4. **Don't cache brokers** - Leadership can change
