import { createHook } from 'node:async_hooks'
import { describe, expect, it, vi } from 'vitest'
import { ErrorCode, KafkaError } from '@kafkats/client'
import {
	FailedProcessingError,
	IllegalStateError,
	StreamsError,
	TaskCorruptedError,
	TaskMigratedError,
	TopologyError,
} from '@/api/errors.js'
import type { ProcessingExceptionHandler } from '@/api/handlers.js'
import type { Processor, ProcessorContext } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { string } from '@/codec.js'
import { capture, ProcessorHarness, processorDef, sinkDef, sourceDef } from '../support/processor-harness.js'

type Ctx = ProcessorContext<unknown, unknown>

function forwarding(transform: (record: StreamRecord<unknown, unknown>) => StreamRecord<unknown, unknown>) {
	return (): Processor<unknown, unknown, unknown, unknown> => {
		let ctx: Ctx
		return { init: c => void (ctx = c), process: record => ctx.forward(transform(record)) }
	}
}

const throwing = (error: unknown) => (): Processor<unknown, unknown> => ({
	process: () => {
		throw error
	},
})

const rejecting = (error: unknown) => (): Processor<unknown, unknown> => ({ process: () => Promise.reject(error) })

/** src -> a (re-stamps the record) -> b (fails) -> c */
async function chain(failing: () => Processor<unknown, unknown>, processing?: ProcessingExceptionHandler) {
	const a = capture('a-seen', ['a'])
	const c = capture('c', ['b'])
	const harness = await ProcessorHarness.start(
		[
			sourceDef('src', ['in']),
			processorDef(
				'a',
				['src'],
				forwarding(record => record.withTimestamp(20).withHeaders({ at: Buffer.from('a') }))
			),
			a.def,
			processorDef('b', ['a'], failing),
			c.def,
		],
		{ processing }
	)
	return { harness, reachedA: a.records, reachedC: c.records }
}

const input = { key: 'k', value: 1, timestamp: 10, offset: 5n, rawKey: Buffer.from('rk'), rawValue: Buffer.from('rv') }

describe('processing exception handler', () => {
	it.each([
		['throws', throwing],
		['rejects', rejecting],
	])("'continue' at the failing node drops the record there when the processor %s", async (_, failing) => {
		const error = new Error('boom')
		const processing = vi.fn<ProcessingExceptionHandler>(() => 'continue')
		const { harness, reachedA, reachedC } = await chain(failing(error), processing)
		await harness.process('in', input)
		expect(reachedA).toHaveLength(1)
		expect(reachedC).toHaveLength(0)
		expect(harness.task.droppedCount('processing')).toBe(1)
		expect(processing).toHaveBeenCalledTimes(1)
		const [ctx, record, seen] = processing.mock.calls[0]!
		expect(seen).toBe(error)
		expect(record).toMatchObject({ key: 'k', value: 1, timestamp: 20 })
		expect(ctx).toEqual({
			topic: 'in',
			partition: 0,
			offset: 5n,
			headers: { at: Buffer.from('a') },
			timestamp: 20,
			processorNodeId: 'b',
			taskId: harness.task.options.taskId,
			sourceRawKey: Buffer.from('rk'),
			sourceRawValue: Buffer.from('rv'),
		})
	})

	it.each([
		['throws', throwing],
		['rejects', rejecting],
	])("'fail' is a FailedProcessingError that upstream nodes do not handle again when it %s", async (_, failing) => {
		const error = new Error('boom')
		const processing = vi.fn<ProcessingExceptionHandler>(() => 'fail')
		const { harness } = await chain(failing(error), processing)
		const failure = await harness.process('in', input).catch((e: unknown) => e)
		expect(failure).toBeInstanceOf(FailedProcessingError)
		expect((failure as Error).cause).toBe(error)
		expect((failure as StreamsError).taskId).toBe(harness.task.options.taskId)
		expect(processing).toHaveBeenCalledTimes(1)
	})

	it('a handler that throws or answers anything else is fatal', async () => {
		const error = new Error('boom')
		const handlerError = new Error('handler')
		const throwingHandler = await chain(throwing(error), () => {
			throw handlerError
		})
		await expect(throwingHandler.harness.process('in', input)).rejects.toMatchObject({
			name: 'FailedProcessingError',
			cause: handlerError,
		})
		const invalid = await chain(throwing(error), (() => 'skip') as unknown as ProcessingExceptionHandler)
		await expect(invalid.harness.process('in', input)).rejects.toMatchObject({
			name: 'FailedProcessingError',
			message: expect.stringMatching(/answered skip/),
			cause: error,
		})
	})

	it.each([
		new TaskMigratedError('migrated'),
		new TaskCorruptedError('corrupted', []),
		new IllegalStateError('closed'),
		new FailedProcessingError('failed below'),
		new TopologyError('not connected'),
		new KafkaError('not leader', ErrorCode.NotLeaderOrFollower),
	])('runtime control errors pass untouched: %s', async error => {
		const processing = vi.fn<ProcessingExceptionHandler>(() => 'continue')
		const { harness } = await chain(throwing(error), processing)
		await expect(harness.process('in', input)).rejects.toBe(error)
		expect(processing).not.toHaveBeenCalled()
	})
})

describe('init and close', () => {
	it('failures are wrapped with the node name', async () => {
		const cause = new Error('cannot')
		await expect(
			ProcessorHarness.start([
				sourceDef('src', ['in']),
				processorDef('p', ['src'], () => ({ init: () => Promise.reject(cause), process: () => {} })),
			])
		).rejects.toMatchObject({ name: 'StreamsError', message: "failed to initialize processor 'p'", cause })

		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('p', ['src'], () => ({
				process: () => {},
				close: () => {
					throw cause
				},
			})),
		])
		await expect(harness.close()).rejects.toMatchObject({ message: "failed to close processor 'p'", cause })
	})
})

describe('lean path', () => {
	it('a synchronous stateless chain completes synchronously and allocates no promise', async () => {
		const harness = await ProcessorHarness.start([
			sourceDef('src', ['in']),
			processorDef('filter', ['src'], () => {
				let ctx: Ctx
				return {
					init: c => void (ctx = c),
					process: record => (record.value !== 0 ? ctx.forward(record) : undefined),
				}
			}),
			processorDef(
				'map',
				['filter'],
				forwarding(record => record.withValue(`v${String(record.value)}`))
			),
			sinkDef('sink', ['map'], 'out', { keyCodec: string(), valueCodec: string() }),
		])
		const source = harness.node('src')
		const records = [1, 2, 3].map(value => new StreamRecord<unknown, unknown>('k', value, value))
		let promises = 0
		const hook = createHook({
			init: (_id, type) => {
				if (type === 'PROMISE') promises++
			},
		})
		hook.enable()
		const results = records.map(record => source.process(record))
		hook.disable()
		expect(results).toEqual([undefined, undefined, undefined])
		expect(promises).toBe(0)
		expect(harness.output('out').map(record => record.value?.toString())).toEqual(['v1', 'v2', 'v3'])
	})
})
