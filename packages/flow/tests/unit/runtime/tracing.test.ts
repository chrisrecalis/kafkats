import { AsyncLocalStorage } from 'node:async_hooks'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { FailedProcessingError } from '@/api/errors.js'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { flowDiagnostics, type FlowProcessDiagnostics } from '@/runtime/tracing.js'
import { TestDriver } from '@/testing/driver.js'
import { pipe, TaskHarness } from '../support/task-harness.js'

type Event = 'start' | 'end' | 'asyncStart' | 'asyncEnd' | 'error'

/** Subscribes to every event of the channel; `onStart` runs in the `start` handler. Unsubscribed after each test. */
function listen(onStart: (processing: FlowProcessDiagnostics) => void = () => {}) {
	const events: { event: Event; processing: FlowProcessDiagnostics }[] = []
	const record = (event: Event) => (message: unknown) => {
		const processing = message as FlowProcessDiagnostics
		events.push({ event, processing })
		if (event === 'start') onStart(processing)
	}
	const handlers = {
		start: record('start'),
		end: record('end'),
		asyncStart: record('asyncStart'),
		asyncEnd: record('asyncEnd'),
		error: record('error'),
	}
	flowDiagnostics.process.subscribe(handlers)
	subscriptions.push(() => flowDiagnostics.process.unsubscribe(handlers))
	return events
}

const subscriptions: (() => void)[] = []
afterEach(() => {
	for (const unsubscribe of subscriptions.splice(0)) unsubscribe()
})

/** The trace context of an upstream send, as a record carries it. */
const traceparent = Buffer.from('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')

/** Calls `body` with each record's value, then forwards the record once what it returned settled. */
function processor(body: (value: string) => unknown): ProcessorSupplier<string, string, string, string> {
	return () => {
		let context: ProcessorContext<string, string>
		return {
			init: ctx => {
				context = ctx
			},
			process: record => {
				const result = body(record.value)
				return result instanceof Promise ? result.then(() => context.forward(record)) : context.forward(record)
			},
		}
	}
}

async function runningTask(supplier: ProcessorSupplier<string, string, string, string>) {
	const h = new TaskHarness({ build: b => pipe(b, { processor: supplier }) })
	const task = await h.start(h.task(0, 0))
	return { h, task }
}

describe('kafkats:flow:process (6.26)', () => {
	it('one operation per record, with where it came from; processing runs in the scope a subscriber asked for, across awaits and into the sends', async () => {
		const storage = new AsyncLocalStorage<string>()
		const seen: string[] = []
		const { h, task } = await runningTask(
			processor(async value => {
				seen.push(`${value} before await: ${storage.getStore()}`)
				await new Promise(resolve => setImmediate(resolve))
				seen.push(`${value} after await: ${storage.getStore()}`)
			})
		)
		const send = h.producer.send.bind(h.producer)
		vi.spyOn(h.producer, 'send').mockImplementation((topic, message) => {
			seen.push(`send ${message.value}: ${storage.getStore()}`)
			return send(topic, message)
		})
		const events = listen(processing =>
			processing.runIn(run => storage.run(`${processing.topic}-${processing.offset}`, run))
		)
		h.enqueue(task, 'input', [{ value: 'a', headers: { traceparent } }, { value: 'b' }])

		await expect(task.processOne(h.clock.now(), true)).resolves.toBe(true)
		await expect(task.processOne(h.clock.now(), true)).resolves.toBe(true)

		expect(seen).toEqual([
			'a before await: input-0',
			'a after await: input-0',
			'send a: input-0',
			'b before await: input-1',
			'b after await: input-1',
			'send b: input-1',
		])
		expect(storage.getStore()).toBeUndefined()
		expect(events.map(({ event, processing }) => `${event} ${processing.offset}`)).toEqual([
			'start 0',
			'end 0',
			'asyncStart 0',
			'asyncEnd 0',
			'start 1',
			'end 1',
			'asyncStart 1',
			'asyncEnd 1',
		])
		const [first] = events
		expect(first!.processing).toMatchObject({
			applicationId: 'app',
			taskId: '0_0',
			topic: 'input',
			partition: 0,
			offset: 0n,
			headers: { traceparent },
		})
		expect(first!.processing.error).toBeUndefined()
	})

	it('the scopes of several subscribers nest in the order they asked', async () => {
		const order: string[] = []
		const { h, task } = await runningTask(processor(() => void order.push('process')))
		const scope = (name: string) => (processing: FlowProcessDiagnostics) =>
			processing.runIn(run => {
				order.push(`enter ${name}`)
				try {
					return run()
				} finally {
					order.push(`leave ${name}`)
				}
			})
		listen(scope('first'))
		listen(scope('second'))
		h.enqueue(task, 'input', [{ value: 'a' }])

		await task.processOne(h.clock.now(), true)

		expect(order).toEqual(['enter first', 'enter second', 'process', 'leave second', 'leave first'])
	})

	it('a record whose processing fails ends with the error, also when it threw synchronously; the caller gets the same failure', async () => {
		const { h, task } = await runningTask(
			processor(value => {
				if (value === 'sync') throw new Error('sync failure')
				return Promise.reject(new Error('async failure'))
			})
		)
		h.enqueue(task, 'input', [{ value: 'sync' }, { value: 'async' }])
		const events = listen()
		for (const value of ['sync', 'async']) {
			events.length = 0
			const failure = await Promise.resolve(task.processOne(h.clock.now(), true)).catch((error: unknown) => error)

			expect(failure).toBeInstanceOf(FailedProcessingError)
			expect((failure as Error).cause).toEqual(new Error(`${value} failure`))
			expect(events.map(({ event }) => event)).toEqual(['start', 'end', 'error', 'asyncStart', 'asyncEnd'])
			expect(events.at(-1)!.processing.error).toBe(failure)
		}
	})

	it('untraced (the caller found no subscribers), a record publishes nothing and a synchronous subtree stays synchronous', async () => {
		const { h, task } = await runningTask(processor(() => {}))
		const events = listen()
		h.enqueue(task, 'input', [{ value: 'a' }, { value: 'b' }])

		expect(task.processOne(h.clock.now())).toBe(true)
		expect(events).toEqual([])
		// Traced, the same subtree's result is a promise: asyncEnd follows the processing.
		const traced = task.processOne(h.clock.now(), true)
		expect(traced).toBeInstanceOf(Promise)
		await traced
		expect(events.map(({ event }) => event)).toEqual(['start', 'end', 'asyncStart', 'asyncEnd'])
	})

	it('the TestDriver traces the records it processes', async () => {
		const storage = new AsyncLocalStorage<bigint>()
		const seen: (bigint | undefined)[] = []
		const driver = new TestDriver({ applicationId: 'traced-app' })
		driver
			.input('orders')
			.peek(() => void seen.push(storage.getStore()))
			.to('out')
		const events = listen(processing => processing.runIn(run => storage.run(processing.offset, run)))

		await driver.run(async ({ send }) => {
			await send('orders', 'one', { headers: { traceparent } })
			await send('orders', 'two')
		})

		expect(seen).toEqual([0n, 1n])
		const started = events.filter(({ event }) => event === 'start').map(({ processing }) => processing)
		expect(started).toMatchObject([
			{ applicationId: 'traced-app', taskId: '0_0', topic: 'orders', partition: 0, offset: 0n },
			{ applicationId: 'traced-app', taskId: '0_0', topic: 'orders', partition: 0, offset: 1n },
		])
		expect(started[0]!.headers).toEqual({ traceparent })
	})
})
