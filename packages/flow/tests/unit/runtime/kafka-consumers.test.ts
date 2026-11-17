import { EventEmitter } from 'node:events'
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import type { BatchContext, ConsumerConfig, KafkaClient, Message, TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { GroupConsumerHooks } from '@/runtime/io.js'
import { KafkaGroupConsumer, KafkaRestoreConsumer } from '@/runtime/kafka-consumers.js'
import { deferred, type Deferred } from '../support/fake-io.js'

type Handler = (messages: Message<Buffer, Buffer>[], ctx: BatchContext) => Promise<void>

/** The client consumer as the adapters use it; `runBatch` runs until the test ends the run. */
class StubConsumer extends EventEmitter {
	isRunning = false
	rebalancePending = false
	readonly run: Deferred<void> = deferred()
	readonly runBatch = vi.fn((_topics: string[], _handler: Handler, _options: object) => {
		this.isRunning = true
		return this.run.promise
	})
	readonly stop = vi.fn(() => {
		this.isRunning = false
	})
	readonly pause = vi.fn()
	readonly resume = vi.fn()
	readonly seek = vi.fn()
	readonly nextOffset = vi.fn(() => 7n)
	readonly currentLag = vi.fn(() => 2n)
	readonly commit = vi.fn(() => Promise.resolve())
	readonly committed = vi.fn(() => Promise.resolve(new Map([['a:0', { offset: 4n, metadata: 'm' }]])))
	readonly groupMetadata = vi.fn(() => ({ groupId: 'app', generationId: 3, memberId: 'member' }))

	constructor(readonly config: ConsumerConfig) {
		super()
	}

	/** The handler and options of the run. */
	get handler(): Handler {
		return this.runBatch.mock.calls[0]![1]
	}
}

function stubClient(): { client: Pick<KafkaClient, 'consumer'>; consumers: StubConsumer[] } {
	const consumers: StubConsumer[] = []
	const consumer = (config: ConsumerConfig) => {
		const created = new StubConsumer(config)
		consumers.push(created)
		return created
	}
	return { client: { consumer } as unknown as Pick<KafkaClient, 'consumer'>, consumers }
}

function message(topic: string, partition: number, offset: bigint): Message<Buffer, Buffer> {
	return { topic, partition, offset, timestamp: 0n, key: null, value: Buffer.from('v'), headers: {} }
}

const batchContext = (topic: string, partition: number) => ({ topic, partition }) as BatchContext
const tp = (topic: string, partition: number): TopicPartition => ({ topic, partition })
const signal = () => new AbortController().signal

function hooks(): GroupConsumerHooks & { [K in keyof GroupConsumerHooks]: ReturnType<typeof vi.fn> } {
	return {
		onPartitionsLost: vi.fn(() => Promise.resolve()),
		onPartitionsRevoked: vi.fn(() => Promise.resolve()),
		onPartitionsAssigned: vi.fn(() => Promise.resolve()),
	}
}

let unhandled: Mock<(reason: unknown) => void>
beforeEach(() => {
	unhandled = vi.fn()
	process.on('unhandledRejection', unhandled)
})
afterEach(async () => {
	await new Promise(resolve => setImmediate(resolve))
	process.off('unhandledRejection', unhandled)
	expect(unhandled).not.toHaveBeenCalled()
})

describe('KafkaGroupConsumer', () => {
	function started(onBatch = vi.fn(), runHooks = hooks()) {
		const { client, consumers } = stubClient()
		const port = new KafkaGroupConsumer(client, { groupId: 'app', partitionAssignmentStrategy: 'range' })
		const stub = consumers[0]!
		const run = port.run(['a', 'b'], onBatch, runHooks)
		return { port, stub, run, onBatch, hooks: runHooks }
	}

	it('runs the topics without client commits, one batch at a time, and hands each batch on synchronously', async () => {
		const { stub, onBatch } = started()
		expect(stub.config).toMatchObject({ groupId: 'app', partitionAssignmentStrategy: 'range' })
		const [topics, , options] = stub.runBatch.mock.calls[0]!
		expect(topics).toEqual(['a', 'b'])
		expect(options).toEqual({ autoCommit: false, commitOffsets: false, partitionConcurrency: 1 })

		const batch = [message('b', 1, 5n), message('b', 1, 6n)]
		const handled = stub.handler(batch, batchContext('b', 1))
		expect(onBatch).toHaveBeenCalledWith(tp('b', 1), batch)
		await expect(handled).resolves.toBeUndefined()
	})

	it('fails the batch, and so the run, when enqueueing throws', async () => {
		const { stub } = started(
			vi.fn(() => {
				throw new Error('bug')
			})
		)
		await expect(stub.handler([message('a', 0, 0n)], batchContext('a', 0))).rejects.toThrow('bug')
	})

	it("routes the client's rebalance hooks to the run's, awaited", async () => {
		const runHooks = hooks()
		const revoked = deferred()
		runHooks.onPartitionsRevoked.mockReturnValue(revoked.promise)
		const { stub } = started(vi.fn(), runHooks)
		const assigned = [{ topic: 'a', partition: 0, offset: 3n, committed: null }]

		await stub.config.onPartitionsAssigned!(assigned)
		expect(runHooks.onPartitionsAssigned).toHaveBeenCalledWith(assigned)

		let done = false
		const revoking = stub.config.onPartitionsRevoked!([tp('a', 0)]).then(() => (done = true))
		await new Promise(resolve => setImmediate(resolve))
		expect(runHooks.onPartitionsRevoked).toHaveBeenCalledWith([tp('a', 0)])
		expect(done).toBe(false)
		revoked.resolve()
		await revoking
		expect(done).toBe(true)
	})

	it("runs the synchronous part of the run's loss hook within the client's call", async () => {
		const order: string[] = []
		const lost = deferred()
		const runHooks = hooks()
		runHooks.onPartitionsLost.mockImplementation(() => {
			order.push('hook')
			return lost.promise
		})
		const { stub } = started(vi.fn(), runHooks)
		const handled = stub.config.onPartitionsLost!([tp('a', 0)])
		order.push('returned')
		expect(order).toEqual(['hook', 'returned'])
		lost.resolve()
		await handled
	})

	it('stop() stops the client and resolves once the run ended, however it ended', async () => {
		const { port, stub, run } = started()
		run.catch(() => {})
		let stopped = false
		const stopping = port.stop({ signal: signal() }).then(() => (stopped = true))
		expect(stub.stop).toHaveBeenCalledTimes(1)
		await new Promise(resolve => setImmediate(resolve))
		expect(stopped).toBe(false)
		stub.run.reject(new Error('the run failed'))
		await stopping
		expect(stopped).toBe(true)
	})

	it('stop() rejects with an AbortError once its signal aborts first, and a later failure of the run is not lost', async () => {
		const { port, stub, run } = started()
		const runFailure = run.catch((error: unknown) => error)
		const controller = new AbortController()
		const stopping = port.stop({ signal: controller.signal })
		controller.abort()
		await expect(stopping).rejects.toMatchObject({ name: 'AbortError' })
		stub.run.reject(new Error('late'))
		await expect(runFailure).resolves.toMatchObject({ message: 'late' })
	})

	it('stop() before run() resolves at once', async () => {
		const { client } = stubClient()
		await expect(
			new KafkaGroupConsumer(client, { groupId: 'app' }).stop({ signal: signal() })
		).resolves.toBeUndefined()
	})

	it('pauses, resumes and seeks a running client, and ignores them once it stopped', () => {
		const { port, stub } = started()
		port.pause([tp('a', 0)])
		port.resume([tp('a', 1)])
		port.seek(tp('a', 2), 9n)
		expect(stub.pause).toHaveBeenCalledWith([tp('a', 0)])
		expect(stub.resume).toHaveBeenCalledWith([tp('a', 1)])
		expect(stub.seek).toHaveBeenCalledWith('a', 2, 9n)

		void port.stop({ signal: signal() }).catch(() => {})
		port.pause([tp('a', 0)])
		port.resume([tp('a', 0)])
		port.seek(tp('a', 0), 1n)
		expect(stub.pause).toHaveBeenCalledTimes(1)
		expect(stub.resume).toHaveBeenCalledTimes(1)
		expect(stub.seek).toHaveBeenCalledTimes(1)
		stub.run.resolve()
	})

	it('commits offsets with their metadata and reads committed ones, with the signal of the call', async () => {
		const { port, stub } = started()
		const options = { signal: signal() }
		const offsets = [{ topic: 'a', partition: 0, offset: 4n, metadata: 'AgAA' }]
		await port.commit(offsets, options)
		expect(stub.commit).toHaveBeenCalledWith(offsets, options)
		await expect(port.committed([tp('a', 0)], options)).resolves.toEqual(
			new Map([['a:0', { offset: 4n, metadata: 'm' }]])
		)
		expect(stub.committed).toHaveBeenCalledWith([tp('a', 0)], options)
	})

	it("reads the client's offsets, lag, group metadata and rebalance state", () => {
		const { port, stub } = started()
		expect(port.nextOffset(tp('a', 1))).toBe(7n)
		expect(stub.nextOffset).toHaveBeenCalledWith('a', 1)
		expect(port.currentLag(tp('a', 1))).toBe(2n)
		expect(stub.currentLag).toHaveBeenCalledWith('a', 1)
		expect(port.groupMetadata()).toEqual({ groupId: 'app', generationId: 3, memberId: 'member' })
		expect(port.rebalancePending).toBe(false)
		stub.rebalancePending = true
		expect(port.rebalancePending).toBe(true)
	})

	it("hands on the client's rebalance, offset and error events", () => {
		const { port, stub } = started()
		const listeners = {
			rebalancePending: vi.fn(),
			rebalanceComplete: vi.fn(),
			offsetOutOfRange: vi.fn(),
			offsetReset: vi.fn(),
			error: vi.fn(),
		}
		for (const [event, listener] of Object.entries(listeners)) port.on(event as 'error', listener)
		const outOfRange = { topic: 'a', partition: 0, offset: 5n }
		const reset = { topic: 'a', partition: 1, from: null, to: 0n, reason: 'no-commit' }
		const error = new Error('transient')
		stub.emit('rebalancePending')
		stub.emit('rebalanceComplete')
		stub.emit('offsetOutOfRange', outOfRange)
		stub.emit('offsetReset', reset)
		stub.emit('error', error)
		expect(listeners.rebalancePending).toHaveBeenCalledTimes(1)
		expect(listeners.rebalanceComplete).toHaveBeenCalledTimes(1)
		expect(listeners.offsetOutOfRange).toHaveBeenCalledWith(outOfRange)
		expect(listeners.offsetReset).toHaveBeenCalledWith(reset)
		expect(listeners.error).toHaveBeenCalledWith(error)
	})
})

describe('KafkaRestoreConsumer', () => {
	function restoring() {
		const { client, consumers } = stubClient()
		const port = new KafkaRestoreConsumer(client, { groupId: 'app-restore-0_0-1', autoOffsetReset: 'none' })
		return { port, stub: consumers[0]! }
	}

	it('runs its manual assignment from the given offsets without commits, awaiting each batch', async () => {
		const { port, stub } = restoring()
		expect(stub.config).toEqual({ groupId: 'app-restore-0_0-1', autoOffsetReset: 'none' })
		port.assign([
			{ topic: 'c1', partition: 0, offset: 5n },
			{ topic: 'c2', partition: 0, offset: 0n },
		])
		port.assign([{ topic: 'c1', partition: 1, offset: 2n }])
		const applied = deferred()
		const onBatch = vi.fn(() => applied.promise)
		void port.run(onBatch, vi.fn())
		const [topics, , options] = stub.runBatch.mock.calls[0]!
		expect(topics).toEqual(['c1', 'c2'])
		expect(options).toEqual({
			autoCommit: false,
			commitOffsets: false,
			partitionConcurrency: 1,
			assignment: [
				{ topic: 'c1', partition: 0, offset: 5n },
				{ topic: 'c2', partition: 0, offset: 0n },
				{ topic: 'c1', partition: 1, offset: 2n },
			],
		})

		const batch = [message('c1', 1, 2n)]
		let done = false
		const handled = stub.handler(batch, batchContext('c1', 1)).then(() => (done = true))
		expect(onBatch).toHaveBeenCalledWith(tp('c1', 1), batch)
		await new Promise(resolve => setImmediate(resolve))
		expect(done).toBe(false)
		applied.resolve()
		await handled
		stub.run.resolve()
	})

	it('reports an out-of-range position of a partition', () => {
		const { port, stub } = restoring()
		port.assign([{ topic: 'c1', partition: 0, offset: 5n }])
		const onOutOfRange = vi.fn()
		void port.run(vi.fn(), onOutOfRange)
		stub.emit('offsetOutOfRange', { topic: 'c1', partition: 0, offset: 5n })
		expect(onOutOfRange).toHaveBeenCalledWith(tp('c1', 0), 5n)
		stub.run.resolve()
	})

	it('takes its partitions before it runs', () => {
		const { port, stub } = restoring()
		void port.run(vi.fn(), vi.fn())
		expect(() => port.assign([{ topic: 'c1', partition: 0, offset: 0n }])).toThrow(IllegalStateError)
		stub.run.resolve()
	})

	it('is positioned at the next record to deliver, pauses while running, and stops', async () => {
		const { port, stub } = restoring()
		port.assign([{ topic: 'c1', partition: 0, offset: 0n }])
		const run = port.run(vi.fn(), vi.fn())
		expect(port.position(tp('c1', 0))).toBe(7n)
		expect(stub.nextOffset).toHaveBeenCalledWith('c1', 0)
		port.pause([tp('c1', 0)])
		expect(stub.pause).toHaveBeenCalledWith([tp('c1', 0)])
		const stopping = port.stop({ signal: signal() })
		expect(stub.stop).toHaveBeenCalledTimes(1)
		stub.run.resolve()
		await stopping
		await run
	})
})
