import { describe, expect, it, vi } from 'vitest'
import type { ProcessingExceptionHandler } from '@/api/handlers.js'
import type { Cancellable, InternalProcessorContext, Processor, ProcessorContext } from '@/processor/api.js'
import { PunctuationQueue } from '@/processor/punctuation.js'
import { StreamRecord } from '@/processor/record.js'
import {
	capture,
	type HarnessOptions,
	ProcessorHarness,
	processorDef,
	sourceDef,
} from '../support/processor-harness.js'

describe('PunctuationQueue', () => {
	it('runs due schedules with the current time and skips missed intervals', async () => {
		const queue = new PunctuationQueue()
		const calls: number[] = []
		queue.schedule(0, 10, time => void calls.push(time))
		expect(queue.nextDue()).toBe(0)
		expect(await queue.maybePunctuate(0)).toBe(true)
		expect(queue.nextDue()).toBe(10)
		expect(queue.canPunctuate(9)).toBe(false)
		expect(await queue.maybePunctuate(9)).toBe(false)
		expect(await queue.maybePunctuate(10)).toBe(true)
		expect(await queue.maybePunctuate(45)).toBe(true)
		expect(calls).toEqual([0, 10, 45])
		expect(queue.nextDue()).toBe(50)
	})

	it('runs in due order, equal due times in scheduling order', async () => {
		const queue = new PunctuationQueue()
		const calls: string[] = []
		queue.schedule(20, 100, () => void calls.push('c'))
		queue.schedule(10, 100, () => void calls.push('a'))
		queue.schedule(10, 100, () => void calls.push('b'))
		await queue.maybePunctuate(20)
		expect(calls).toEqual(['a', 'b', 'c'])
		expect(queue.size).toBe(3)
	})

	it('cancel before the due time, inside the punctuator or after a re-add removes the schedule', async () => {
		const queue = new PunctuationQueue()
		const calls: string[] = []
		const early = queue.schedule(10, 10, () => void calls.push('early'))
		early.cancel()
		let inside!: Cancellable
		inside = queue.schedule(10, 10, () => {
			calls.push('inside')
			inside.cancel()
		})
		const later = queue.schedule(10, 10, () => void calls.push('later'))
		await queue.maybePunctuate(10)
		later.cancel()
		await queue.maybePunctuate(100)
		expect(calls).toEqual(['inside', 'later'])
		expect(queue.size).toBe(0)
		expect(queue.nextDue()).toBeUndefined()
	})

	it('a schedule added by a punctuator runs in the same round when due', async () => {
		const queue = new PunctuationQueue()
		const calls: string[] = []
		queue.schedule(0, 10, () => {
			calls.push('first')
			if (calls.length === 1) queue.schedule(0, 10, () => void calls.push('added'))
		})
		await queue.maybePunctuate(5)
		expect(calls).toEqual(['first', 'added'])
	})
})

type Ctx = ProcessorContext<unknown, unknown>

/** src -> p (schedules in init) -> out */
async function scheduling(init: (ctx: Ctx) => void, options: HarnessOptions = {}) {
	const out = capture('out', ['p'])
	const supplier = (): Processor<unknown, unknown, unknown, unknown> => ({ init, process: () => {} })
	const harness = await ProcessorHarness.start(
		[sourceDef('src', ['in']), processorDef('p', ['src'], supplier), out.def],
		options
	)
	return { harness, out: out.records }
}

describe('context.schedule', () => {
	it('stream time: first due 0, fires once stream time is known, with the stream time', async () => {
		const calls: number[] = []
		const { harness } = await scheduling(ctx => void ctx.schedule(10, 'stream-time', time => void calls.push(time)))
		expect(harness.scheduled).toBe(1)
		expect(await harness.punctuateStreamTime()).toBe(false)
		await harness.process('in', { key: 'k', value: 1, timestamp: 5 })
		expect(await harness.punctuateStreamTime()).toBe(true)
		await harness.process('in', { key: 'k', value: 1, timestamp: 27 })
		await harness.punctuateStreamTime()
		await harness.punctuateStreamTime()
		expect(calls).toEqual([5, 27])
	})

	it('wall clock: first due now + interval, with the wall clock', async () => {
		const calls: number[] = []
		const { harness } = await scheduling(
			ctx => void ctx.schedule(100, 'wall-clock', time => void calls.push(time)),
			{
				wallClock: 1000,
			}
		)
		expect(await harness.advanceWallClock(99)).toBe(false)
		expect(await harness.advanceWallClock(1)).toBe(true)
		expect(await harness.advanceWallClock(350)).toBe(true)
		expect(calls).toEqual([1100, 1450])
		expect(harness.task.wallClockPunctuation.nextDue()).toBe(1500)
	})

	it.each([0, -1, 1.5, Number.NaN])('rejects the interval %s', async interval => {
		const errors: unknown[] = []
		await scheduling(ctx => {
			try {
				ctx.schedule(interval, 'stream-time', () => {})
			} catch (error) {
				errors.push(error)
			}
		})
		expect(errors).toEqual([expect.any(TypeError)])
	})

	it('a punctuator forwards with the punctuation time and no input record', async () => {
		const seen: unknown[] = []
		const { harness, out } = await scheduling(ctx => {
			ctx.schedule(10, 'wall-clock', time => {
				const internal = ctx as InternalProcessorContext<unknown, unknown>
				seen.push(ctx.recordMetadata(), internal.recordContext())
				return ctx.forward(new StreamRecord('k', 'tick', time))
			})
		})
		await harness.advanceWallClock(10)
		expect(out.map(record => record.value)).toEqual(['tick'])
		expect(seen).toEqual([
			null,
			{ topic: null, partition: -1, offset: -1n, timestamp: 10, headers: {}, rawKey: null, rawValue: null },
		])
	})

	it("failures go to the processing handler with no record; 'continue' keeps the schedule", async () => {
		const processing = vi.fn<ProcessingExceptionHandler>(() => 'continue')
		const error = new Error('tick failed')
		const { harness } = await scheduling(
			ctx =>
				void ctx.schedule(10, 'wall-clock', () => {
					throw error
				}),
			{ processing }
		)
		await harness.advanceWallClock(10)
		await harness.advanceWallClock(10)
		expect(processing).toHaveBeenCalledTimes(2)
		expect(processing.mock.calls[0]).toEqual([
			expect.objectContaining({ topic: null, partition: -1, offset: -1n, timestamp: 10, processorNodeId: 'p' }),
			null,
			error,
		])
		expect(harness.task.droppedCount('processing')).toBe(2)
	})

	it("'fail' rejects the punctuation round", async () => {
		const { harness } = await scheduling(
			ctx =>
				void ctx.schedule(10, 'stream-time', async () => {
					throw new Error('tick failed')
				})
		)
		harness.streamTime = 0
		await expect(harness.punctuateStreamTime()).rejects.toMatchObject({ name: 'FailedProcessingError' })
	})

	it('closing the task drops its punctuations', async () => {
		const { harness } = await scheduling(ctx => {
			ctx.schedule(10, 'stream-time', () => {})
			ctx.schedule(10, 'wall-clock', () => {})
		})
		await harness.close()
		expect(harness.task.streamPunctuation.size).toBe(0)
		expect(harness.task.wallClockPunctuation.size).toBe(0)
	})
})
