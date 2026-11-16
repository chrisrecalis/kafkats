import { describe, expect, it } from 'vitest'
import type { ConsumeContext, Message, Producer } from '@kafkats/client'
import { codec } from '../../src/index.js'
import type { WorkerContext } from '../../src/processors/base.js'
import { PendingCheckpoints, type Checkpoints } from '../../src/runtime/checkpoints.js'
import { recordContextStorage } from '../../src/runtime/context.js'
import { PendingSends } from '../../src/runtime/pending-sends.js'
import { WorkerTransactions } from '../../src/runtime/transactions.js'
import { BufferedKeyValueStore } from '../../src/state/buffered.js'
import { InMemoryKeyValueStore } from '../../src/state/memory.js'

const str = codec.string()

/** A worker whose producer commits every transaction successfully. */
function workerWithTransactions() {
	const producer = {
		transaction: async (run: (tx: object) => Promise<void>) => run({ sendOffsets: async () => {} }),
	} as unknown as Producer
	const worker = {
		id: 0,
		producer,
		activeTransaction: null,
		pendingCheckpoints: new PendingCheckpoints(),
		sends: new PendingSends(),
		streamClocks: new Map(),
		dirtyStores: new Set(),
		failure: null,
		processedSinceRestart: false,
	} as unknown as WorkerContext
	const failures: unknown[] = []
	const transactions = new WorkerTransactions(
		worker,
		60_000,
		{ flush: async () => {} } as unknown as Checkpoints,
		error => failures.push(error)
	)
	const process = (work: () => Promise<void>) =>
		transactions.process({ topic: 't', partition: 0, offset: 0n } as Message<Buffer>, {} as ConsumeContext, () =>
			recordContextStorage.run({ worker, topic: 't', partition: 0 }, work)
		)
	const read = <T>(work: () => Promise<T>) => recordContextStorage.run({ worker, topic: 't', partition: 0 }, work)
	return { worker, transactions, failures, process, read }
}

describe('exactly-once: applying buffered state after the Kafka commit fails', () => {
	it('drops the buffers of every store of the transaction, not only of the one that failed', async () => {
		const { transactions, failures, process, read } = workerWithTransactions()
		const failingInner = new InMemoryKeyValueStore<string, string>('a', { keyCodec: str, valueCodec: str })
		failingInner.put = async () => {
			throw new Error('disk full')
		}
		const inner = new InMemoryKeyValueStore<string, string>('b', { keyCodec: str, valueCodec: str })
		const failing = new BufferedKeyValueStore<string, string>(failingInner, str, str)
		const store = new BufferedKeyValueStore<string, string>(inner, str, str)

		await process(async () => {
			await failing.put('k', 'buffered')
			await store.put('k', 'buffered')
		})
		await expect(transactions.commit()).rejects.toThrow('disk full')
		expect(failures).toHaveLength(1)

		// The worker restarts: its transaction is torn down and the stores are restored from the
		// changelog, which holds a newer value by now.
		await transactions.abort(new Error('worker stopped'))
		await inner.put('k', 'restored')

		expect(await read(() => store.get('k'))).toBe('restored')
	})
})
