import { AsyncLocalStorage } from 'node:async_hooks'
import { describe, expect, it } from 'vitest'

import {
	diagnostics,
	type CommitDiagnostics,
	type ProcessDiagnostics,
	type RebalanceDiagnostics,
	type SendDiagnostics,
} from '@/diagnostics.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

type Handlers<C> = { start?(context: C): void; asyncEnd?(context: C): void; error?(context: C): void }

/** Subscribe to a tracing channel, only for events of one topic or group; returns the unsubscribe function. */
function subscribe<C extends object>(
	channel: { subscribe(handlers: object): void; unsubscribe(handlers: object): void },
	mine: (context: C) => boolean,
	handlers: Handlers<C>
): () => void {
	const filtered: Handlers<C> = {}
	for (const name of ['start', 'asyncEnd', 'error'] as const) {
		const handler = handlers[name]
		if (handler) filtered[name] = context => mine(context) && handler(context)
	}
	channel.subscribe(filtered)
	return () => channel.unsubscribe(filtered)
}

describe.concurrent('Consumer (integration) - diagnostics', () => {
	it('carries headers added on send to the handler, and runs the handler in the subscriber context', async () => {
		const client = createClient('it-diag-trace')
		await client.connect()
		const topic = uniqueName('it-diag-trace')
		const groupId = uniqueName('it-diag-group')
		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])

		const activeTrace = new AsyncLocalStorage<string>()
		const stop = [
			subscribe<SendDiagnostics>(diagnostics.send, c => c.topic === topic, {
				start: context => {
					for (const message of context.messages) message.headers.traceparent = 'trace-1'
				},
			}),
			subscribe<ProcessDiagnostics>(diagnostics.process, c => c.topic === topic, {
				start: context => {
					// What a tracer does: continue the trace found in the message for the handler's duration.
					const trace = context.messages[0]!.headers.traceparent!.toString()
					context.runInContext = handler => activeTrace.run(trace, handler)
				},
			}),
		]
		try {
			const producer = client.producer({ lingerMs: 0 })
			await producer.send(topic, { value: Buffer.from('hello') })
			await producer.flush()

			const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			const seen: Array<string | undefined> = []
			await consumer.runEach(topic, async () => {
				// Also after an await: the context follows the handler's asynchronous work.
				await new Promise(resolve => setTimeout(resolve, 1))
				seen.push(activeTrace.getStore())
				consumer.stop()
			})
			expect(seen).toEqual(['trace-1'])
			await producer.disconnect()
		} finally {
			for (const unsubscribe of stop) unsubscribe()
			await client.disconnect()
		}
	})

	it('publishes handler calls, a failed handler, and offset commits', async () => {
		const client = createClient('it-diag-events')
		await client.connect()
		const topic = uniqueName('it-diag-events')
		const groupId = uniqueName('it-diag-group')
		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])
		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			topic,
			Array.from({ length: 5 }, (_, i) => ({ value: Buffer.from(String(i)) }))
		)
		await producer.flush()

		const processed: ProcessDiagnostics[] = []
		const failed: ProcessDiagnostics[] = []
		const commits: CommitDiagnostics[] = []
		const stop = [
			subscribe<ProcessDiagnostics>(diagnostics.process, c => c.topic === topic, {
				asyncEnd: context => processed.push(context),
				error: context => failed.push(context),
			}),
			subscribe<CommitDiagnostics>(diagnostics.commit, c => c.groupId === groupId, {
				asyncEnd: context => commits.push(context),
			}),
		]
		try {
			const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			await expect(
				consumer.runBatch(topic, async batch => {
					expect(batch).toHaveLength(5)
					throw new Error('handler failed')
				})
			).rejects.toThrow('handler failed')

			expect(failed).toHaveLength(1)
			expect(failed[0]).toMatchObject({ groupId, topic, partition: 0, batch: true })
			expect(failed[0]!.messages).toHaveLength(5)
			expect((failed[0]!.error as Error).message).toBe('handler failed')
			processed.length = 0

			const again = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			let handled = 0
			await again.runEach(topic, async () => {
				if (++handled === 5) again.stop()
			})
			expect(processed.map(p => [p.batch, p.messages.length, p.messages[0]!.offset])).toEqual(
				[0n, 1n, 2n, 3n, 4n].map(offset => [false, 1, offset])
			)
			// Stopping commits what was handled: the next offset to consume.
			expect(commits.at(-1)?.offsets).toEqual([{ topic, partition: 0, offset: 5n }])
			expect(commits.at(-1)?.error).toBeUndefined()
			await producer.disconnect()
		} finally {
			for (const unsubscribe of stop) unsubscribe()
			await client.disconnect()
		}
	})

	it('reports position, end and buffered bytes per partition, and publishes a rebalance', async () => {
		const client = createClient('it-diag-metrics')
		await client.connect()
		const topic = uniqueName('it-diag-metrics')
		const groupId = uniqueName('it-diag-group')
		await client.createTopics([{ name: topic, numPartitions: 2, replicationFactor: 1 }])
		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			topic,
			Array.from({ length: 20 }, (_, i) => ({ value: Buffer.from(String(i)), partition: 0 }))
		)
		await producer.flush()

		const rebalances: RebalanceDiagnostics[] = []
		const stop = subscribe<RebalanceDiagnostics>(diagnostics.rebalance, c => c.groupId === groupId, {
			asyncEnd: context => rebalances.push(context),
		})
		try {
			const first = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			expect(first.metrics()).toEqual({ partitions: [] })

			let handled = 0
			let release!: () => void
			const held = new Promise<void>(resolve => (release = resolve))
			const firstRun = first.runEach(topic, async () => {
				// Hold the first record so the rest stays fetched but unhandled.
				if (++handled === 1) await held
			})
			for (const deadline = Date.now() + 15_000; handled < 1 && Date.now() < deadline;) {
				await new Promise(resolve => setTimeout(resolve, 20))
			}
			await new Promise(resolve => setTimeout(resolve, 300))

			const waiting = first.metrics().partitions.find(p => p.partition === 0)!
			expect(waiting.position).toBe(20n)
			expect(waiting.highWatermark).toBe(20n)
			expect(waiting.paused).toBe(false)
			expect(
				first
					.metrics()
					.partitions.map(p => p.partition)
					.sort()
			).toEqual([0, 1])
			release()
			for (const deadline = Date.now() + 15_000; handled < 20 && Date.now() < deadline;) {
				await new Promise(resolve => setTimeout(resolve, 20))
			}
			expect(first.metrics().partitions.find(p => p.partition === 0)!.bufferedBytes).toBe(0)

			// A second member makes the first one go through a rebalance.
			const second = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			const secondRun = second.runEach(topic, async () => {})
			for (const deadline = Date.now() + 20_000; rebalances.length === 0 && Date.now() < deadline;) {
				await new Promise(resolve => setTimeout(resolve, 50))
			}
			expect(rebalances.length).toBeGreaterThan(0)
			expect(rebalances[0]!.groupId).toBe(groupId)
			expect(rebalances[0]!.assignment!.length).toBeLessThanOrEqual(2)
			expect(rebalances[0]!.error).toBeUndefined()

			first.stop()
			second.stop()
			await Promise.all([firstRun, secondRun])
			await producer.disconnect()
		} finally {
			stop()
			await client.disconnect()
		}
	})
})
