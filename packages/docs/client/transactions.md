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
			await txn.sendOffsets(ctx)
		})
	},
	{ autoCommit: false, commitOffsets: false }
)
```

`sendOffsets(ctx)` commits `ctx.offset + 1` for the context's topic and partition. The context contains a
delivery-time snapshot of the consumer-group membership that delivered the record, so a rebalance makes a stale
transaction fail instead of committing offsets under a newer generation.

::: tip Batched processing
Pass explicit accumulated offsets as the second argument: `txn.sendOffsets(ctx, offsets)`. Use the first context
that opened the transaction so the entire batch remains bound to that consumer-group generation.

`@kafkats/flow` with `processingGuarantee: 'exactly_once'` handles this batching automatically. See
[Flow Processing Guarantees](/flow/getting-started#processing-guarantees) for details.
:::

## Concurrent Transactions

A producer can have one open transaction at a time — this is a Kafka protocol constraint, not a library limit. Concurrent `transaction()` calls **queue and wait for capacity** instead of throwing, so a single transactional producer is safe to share across handlers running with `partitionConcurrency` greater than 1:

```typescript
await consumer.runBatch(
	'input',
	async (messages, ctx) => {
		const results = await transform(messages)

		// Safe with partitionConcurrency > 1: transactions from concurrent
		// partition handlers wait their turn on the producer.
		await producer.transaction(async txn => {
			await txn.send('output', results)
			await txn.sendOffsets({
				consumerGroupMetadata, // group id + generation id + member id (KIP-447 zombie fencing)
				offsets: [{ topic: ctx.topic, partition: ctx.partition, offset: ctx.offset + 1n }],
			})
		})
	},
	// commitOffsets: false — offsets must only be committed through the
	// transaction, never by the consumer itself (e.g. during revoke/shutdown).
	{ autoCommit: false, commitOffsets: false, partitionConcurrency: 3 }
)
```

For strict exactly-once semantics, pass `consumerGroupMetadata` rather than a bare `groupId` — without the generation and member id, a zombie consumer's offset commits are not fenced (see the tip in [Consume-Transform-Produce](#consume-transform-produce)).

Things to know:

- **Only the transaction section serializes.** Message processing before `transaction()` still overlaps across partitions.
- **No ordering guarantee** between independent transactions. Per-partition ordering is preserved because `runEach`/`runBatch` don't deliver the next record for a partition until the handler returns.
- **The transaction timeout does not include queue time.** It starts when the transaction actually begins.
- **Nested transactions throw.** Calling `producer.transaction()` from inside the same producer's transaction callback throws immediately instead of deadlocking.
- **Watch for commit-bound throughput.** The producer emits `transaction:queued` (with the number of transactions ahead) whenever a call has to wait. If this fires sustainedly, your throughput ceiling is transaction commit latency — increase batch sizes to lower the transaction rate, or raise `transactionConcurrency` (below) so transactions actually overlap.

```typescript
producer.on('transaction:queued', ({ queued }) => {
	metrics.gauge('kafka.txn.queue_depth', queued)
})
```

## Parallel Transactions

The one-open-transaction limit is per transactional ID. `transactionConcurrency` lets a single producer run up to N transactions at once by managing a pool of N internal transactional producers ("lanes") behind the same `transaction()` API:

```typescript
const producer = client.producer({
	transactionalId: 'orders-processor',
	transactionConcurrency: 3, // up to 3 transactions in flight
})

await consumer.runBatch(
	'input',
	async (messages, ctx) => {
		const results = await transform(messages)

		// With transactionConcurrency: 3, transactions from the 3 concurrent
		// partition handlers genuinely overlap instead of queueing.
		await producer.transaction(async txn => {
			await txn.send('output', results)
			await txn.sendOffsets({
				consumerGroupMetadata,
				offsets: [{ topic: ctx.topic, partition: ctx.partition, offset: ctx.offset + 1n }],
			})
		})
	},
	{ autoCommit: false, commitOffsets: false, partitionConcurrency: 3 }
)
```

How it works:

- Lane 0 uses `transactionalId` verbatim; lanes 1..N-1 append `-1`..`-{N-1}` (e.g. `orders-processor`, `orders-processor-1`, `orders-processor-2`). Each lane initializes lazily on first use and fences its predecessor with the same ID.
- `transaction()` calls are admitted **first-in first-out**: a call takes a free lane immediately, or waits for the next one released. `transaction:queued` fires only when all lanes are busy.
- Transactions begin in call order but **may commit in any order** — same guarantee as independent transactions today. Per-partition input ordering is unaffected (`runEach`/`runBatch` still deliver one record/batch per partition at a time).
- `flush()` and `disconnect()` cover all lanes; disconnect still refuses while any transaction is active or queued.

::: warning Fencing and transactional IDs
Transactional-ID zombie fencing applies **per lane**. For consume-transform-produce, always pass `consumerGroupMetadata` to `sendOffsets()` (KIP-447) so a zombie's offset commits are fenced by consumer-group generation regardless of which lane they ran on. Also note that changing `transactionConcurrency` changes the set of transactional IDs in use: a transaction left open under an ID that is no longer used stays open until the broker's `transactional.id.expiration.ms` elapses.
:::

## Transaction API

### ProducerTransaction

```typescript
interface ProducerTransaction {
	// Send messages within the transaction
	send(topic: string, messages: ProducerMessage[]): Promise<SendResult[]>
	send<V, K>(topicDef: TopicDefinition<V, K>, messages: ProducerMessage<V, K>[]): Promise<SendResult[]>

	// Commit consumer offsets (for exactly-once)
	sendOffsets(ctx: ConsumeContext): Promise<void>
	sendOffsets(ctx: ConsumeContext, offsets: TopicPartitionOffset[]): Promise<void>
	sendOffsets(params: SendOffsetsParams): Promise<void>

	// Abort signal (fires on timeout or error)
	readonly signal: AbortSignal
}
```

### SendOffsetsParams

```typescript
interface SendOffsetsParams {
	// Low-level, unfenced form
	groupId?: string

	// Advanced escape hatch for callers managing raw group metadata
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

A context from a manually assigned consumer automatically uses the `groupId`-only form because there is no group
generation to fence. The object form remains available for standalone producer transactions and other low-level
callers without a consume context. For normal `runEach()`, `runBatch()`, and `stream()` processing, pass the
`ConsumeContext` supplied to the handler.

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
