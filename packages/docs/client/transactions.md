# Transactions

kafkats supports Kafka transactions for exactly-once semantics (EOS).

## Overview

Transactions ensure that a group of messages are either all committed or all rolled back. This is essential for:

- **Exactly-once processing** - No duplicates or lost messages
- **Atomic multi-topic writes** - All-or-nothing across topics
- **Consume-transform-produce** - Atomic read-process-write patterns

## Enabling Transactions

Create a transactional producer:

```typescript
const producer = client.producer({
	transactionalId: 'my-transaction-id',
	acks: 'all', // Required for transactions
})
```

::: tip Transactional ID
The `transactionalId` must be unique per producer instance. Use a stable identifier like `${applicationName}-${instanceId}`.
:::

## Basic Transaction

```typescript
await producer.transaction(async txn => {
	// All sends in this callback are part of the transaction
	await txn.send('orders', [{ key: 'order-1', value: JSON.stringify({ status: 'created' }) }])

	await txn.send('inventory', [{ key: 'item-1', value: JSON.stringify({ delta: -1 }) }])

	// Transaction commits automatically when callback completes
})
```

If an error is thrown, the transaction is automatically aborted:

```typescript
await producer.transaction(async txn => {
	await txn.send('orders', [{ value: 'order-data' }])

	if (someCondition) {
		throw new Error('Abort transaction')
		// Transaction is rolled back, no messages are committed
	}
})
```

## Transaction Timeout

Configure transaction timeout:

```typescript
const producer = client.producer({
	transactionalId: 'my-txn',
	transactionTimeoutMs: 60000, // 60 seconds (default)
})
```

Use the abort signal for long-running operations:

```typescript
await producer.transaction(async txn => {
	// Cancel fetch if transaction times out
	const data = await fetch(url, { signal: txn.signal })
	await txn.send('results', [{ value: data }])
})
```

## Consume-Transform-Produce

For exactly-once stream processing, commit consumer offsets within the transaction:

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	isolationLevel: 'read_committed', // Only read committed messages
})

const producer = client.producer({
	transactionalId: 'my-processor',
	acks: 'all',
})

await consumer.runEach(
	'input',
	async (message, ctx) => {
		// Process message
		const result = await transform(message.value)

		// Atomically: send output + commit input offset
		await producer.transaction(async txn => {
			await txn.send('output', [{ value: result }])

			await txn.sendOffsets({
				groupId: 'my-group',
				offsets: [
					{
						topic: ctx.topic,
						partition: ctx.partition,
						offset: ctx.offset + 1n,
					},
				],
			})
		})
	},
	{ autoCommit: false }
)
```

::: tip Full EOS with consumers
For strict exactly-once consume-transform-produce you should provide `consumerGroupMetadata` (group id + generation id + member id). That information is required to atomically commit offsets with the consumer's current group membership.

**Recommended:** Use `@kafkats/flow` with `processingGuarantee: 'exactly_once'` for automatic exactly-once handling. Flow batches multiple messages into transactions and commits them periodically, which is more efficient than per-message transactions. See [Flow Processing Guarantees](/flow/getting-started#processing-guarantees) for details.
:::

## Transaction API

### ProducerTransaction

```typescript
interface ProducerTransaction {
	// Send messages within the transaction
	send(topic: string, messages: ProducerMessage[]): Promise<SendResult[]>
	send<V, K>(topicDef: TopicDefinition<V, K>, messages: ProducerMessage<V, K>[]): Promise<SendResult[]>

	// Commit consumer offsets (for exactly-once)
	sendOffsets(params: SendOffsetsParams): Promise<void>

	// Abort signal (fires on timeout or error)
	readonly signal: AbortSignal
}
```

### SendOffsetsParams

```typescript
interface SendOffsetsParams {
	// Simple: just the group ID
	groupId?: string

	// Full EOS: include consumer metadata
	consumerGroupMetadata?: {
		groupId: string
		generationId: number
		memberId: string
		groupInstanceId?: string
	}

	// Offsets to commit
	offsets: Array<{
		topic: string
		partition: number
		offset: bigint
	}>
}
```

## Idempotent Producer

For simpler exactly-once delivery (without full transactions):

```typescript
const producer = client.producer({
	idempotent: true,
	acks: 'all',
})

// Retries are safe - no duplicates
await producer.send('events', { value: 'data' })
```

Idempotent mode:

- Assigns a unique producer ID
- Uses sequence numbers per partition
- Safe retries without duplicates
- Does NOT support atomic multi-topic writes

## Error Handling

### Transaction Aborted

```typescript
import { InvalidTxnStateError } from '@kafkats/client'

try {
	await producer.transaction(async txn => {
		await txn.send('topic', [{ value: 'data' }])
		// ... long operation
	})
} catch (error) {
	if (error instanceof InvalidTxnStateError) {
		// Transaction was aborted (timeout, fenced, etc.)
	}
}
```

### Producer Fenced

When another producer with the same `transactionalId` starts:

```typescript
import { ProducerFencedError } from '@kafkats/client'

try {
	await producer.transaction(async txn => {
		// ...
	})
} catch (error) {
	if (error instanceof ProducerFencedError) {
		// Another producer took over - shut down this instance
		await producer.disconnect()
		process.exit(1)
	}
}
```

## Best Practices

1. **Use stable transactional IDs** - Based on application + instance identity
2. **Keep transactions short** - Avoid long-running operations inside
3. **Use abort signal** - Cancel external operations on transaction abort
4. **Handle fencing** - Shut down gracefully when fenced
5. **Read committed** - Use `isolationLevel: 'read_committed'` for consumers

## Consumer Isolation Levels

Configure how consumers see transactional messages:

```typescript
// Only committed transactions (default, recommended)
const consumer = client.consumer({
	groupId: 'my-group',
	isolationLevel: 'read_committed',
})

// All messages including uncommitted
const consumer = client.consumer({
	groupId: 'my-group',
	isolationLevel: 'read_uncommitted',
})
```
