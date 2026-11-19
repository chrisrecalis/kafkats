/**
 * The exception handlers at work in the runtime (3.14, 4.6, DL7c), over fake IO: each response of the
 * deserialization, processing and production handlers where it is asked (serialization and send origin, changelog
 * records, exactly-once), handlers that throw or answer something else, and the uncaught exception handler's
 * 'restart' and function form. (Punctuators go through the processing handler the same way; process() comes with the
 * Processor API, so its runtime case waits for it: processor/punctuation.test covers the route.)
 */
import { describe, expect, it, vi } from 'vitest'
import { ErrorCode, KafkaProtocolError, string, type Codec } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { FailedProcessingError, StreamsError } from '@/api/errors.js'
import type { ErrorHandlerContext, HandlerResponse } from '@/api/handlers.js'
import { codec } from '@/codec.js'
import type { CollectedRecord } from '@/processor/output.js'
import { RuntimeHarness as Harness } from '../support/runtime-harness.js'

const strings = { key: string(), value: string() }

/** `in` (JSON values) -> mapValues (throws for 'boom') -> `out` through `sink` (default: strings). */
function pipeline(sink: Codec<string> = string()) {
	return (app: FlowApp): void => {
		app.stream('in', { key: string(), value: codec.json<string>() })
			.mapValues(value => {
				if (value === 'boom') throw new Error('boom')
				return value
			})
			.to('out', { key: string(), value: sink })
	}
}

function counting(app: FlowApp): void {
	app.stream('in', strings).groupByKey().count().toStream().to('out', { key: string() })
}

const json = (value: string) => JSON.stringify(value)
const tooLarge = () => new KafkaProtocolError(ErrorCode.MessageTooLarge)

/** A started harness with a commit every second. */
async function started(build: (app: FlowApp) => void, config: Partial<FlowConfig> = {}): Promise<Harness> {
	const h = new Harness(build, { commitIntervalMs: 1000, ...config })
	await h.started()
	return h
}

/** The app stopped with a FailedProcessingError caused by `cause` (if given); nothing more was committed. */
async function expectHalted(h: Harness, cause?: RegExp): Promise<void> {
	await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
	const error = h.lifecycle.error
	expect(error).toBeInstanceOf(StreamsError)
	if (cause) expect(String((error?.cause as Error | undefined)?.message ?? error?.message)).toMatch(cause)
	await h.stop()
	expect(h.committed()).toBeUndefined()
}

describe('deserialization handler', () => {
	it("'continue' skips a record that cannot be read; its offset is committed; the handler sees where it came from", async () => {
		const seen: ErrorHandlerContext[] = []
		const h = await started(pipeline(), {
			deserializationExceptionHandler: ctx => {
				seen.push(ctx)
				return 'continue'
			},
		})
		h.consumer.deliver('in:0', [
			{ key: 'k', value: '{not json' },
			{ key: 'k', value: json('a') },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['a']))
		expect(seen).toEqual([
			expect.objectContaining({
				topic: 'in',
				partition: 0,
				offset: 0n,
				sourceRawValue: Buffer.from('{not json'),
			}),
		])
		await h.stop()
		expect(h.committed()).toBe(2n)
	})

	it("'fail' (the default) stops the app", async () => {
		const h = await started(pipeline())
		h.consumer.deliver('in:0', [{ key: 'k', value: '{not json' }])
		await expectHalted(h, /JSON/)
		expect(h.output()).toEqual([])
	})
})

describe('processing handler', () => {
	it("'continue' drops the record at the failing node; later records are processed and committed", async () => {
		const handler = vi.fn((): HandlerResponse => 'continue')
		const h = await started(pipeline(), { processingExceptionHandler: handler })
		h.consumer.deliver('in:0', [
			{ key: 'k', value: json('boom') },
			{ key: 'k', value: json('a') },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['a']))
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ topic: 'in', offset: 0n }),
			expect.objectContaining({ key: 'k', value: 'boom' }),
			new Error('boom')
		)
		await h.stop()
		expect(h.committed()).toBe(2n)
	})

	const failing: [string, FlowConfig['processingExceptionHandler']][] = [
		["'fail' (the default)", undefined],
		[
			'a handler that throws',
			() => {
				throw new Error('handler bug')
			},
		],
		['a handler that answers something else', () => 'skip' as HandlerResponse],
	]
	for (const [name, handler] of failing) {
		it(`${name} stops the app`, async () => {
			const h = await started(pipeline(), { processingExceptionHandler: handler })
			h.consumer.deliver('in:0', [
				{ key: 'k', value: json('boom') },
				{ key: 'k', value: json('a') },
			])
			await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
			expect(h.lifecycle.error).toBeInstanceOf(FailedProcessingError)
			await expectHalted(h)
			expect(h.output()).toEqual([])
		})
	}
})

describe('production handler', () => {
	/** Encodes every value but 'bad'. */
	const picky: Codec<string> = {
		encode: value => {
			if (value === 'bad') throw new Error('cannot encode')
			return Buffer.from(value)
		},
		decode: buffer => buffer.toString(),
	}

	type Call = [CollectedRecord, unknown, 'serialization' | 'send']
	const recording =
		(response: HandlerResponse, calls: Call[]) =>
		(_ctx: unknown, record: CollectedRecord, error: unknown, origin: 'serialization' | 'send') => {
			calls.push([record, error, origin])
			return response
		}

	it("'continue' drops a record its sink cannot encode (origin 'serialization')", async () => {
		const calls: Call[] = []
		const h = await started(pipeline(picky), { productionExceptionHandler: recording('continue', calls) })
		h.consumer.deliver('in:0', [
			{ key: 'k', value: json('bad') },
			{ key: 'k', value: json('a') },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['a']))
		expect(calls.map(([, error, origin]) => [(error as Error).message, origin])).toEqual([
			['cannot encode', 'serialization'],
		])
		await h.stop()
		expect(h.committed()).toBe(2n)
	})

	it("'fail' stops the app at a record its sink cannot encode", async () => {
		const h = await started(pipeline(picky))
		h.consumer.deliver('in:0', [{ key: 'k', value: json('bad') }])
		await expectHalted(h, /cannot encode/)
	})

	it("at-least-once: 'continue' drops a record the broker rejects (origin 'send'); its offset is committed", async () => {
		const calls: Call[] = []
		const h = await started(pipeline(), { productionExceptionHandler: recording('continue', calls) })
		h.producer.steps.send.next(tooLarge())
		h.consumer.deliver('in:0', [
			{ key: 'k', value: json('a') },
			{ key: 'k', value: json('b') },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['b']))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed()).toBe(2n))
		expect(calls.map(([record, , origin]) => [record.topic, record.value?.toString(), origin])).toEqual([
			['out', 'a', 'send'],
		])
		expect(h.lifecycle.state).toBe('RUNNING')
		await h.stop()
	})

	it("at-least-once: 'fail' stops the app at the next commit after the broker rejected a record", async () => {
		const h = await started(pipeline())
		h.producer.steps.send.next(tooLarge())
		h.consumer.deliver('in:0', [{ key: 'k', value: json('a') }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		h.io.clock.advance(1000)
		await expectHalted(h, /MESSAGE_TOO_LARGE|MessageTooLarge|too large/i)
	})

	it("a rejected changelog record stops the app also under 'continue': the store would diverge from its changelog", async () => {
		const calls: Call[] = []
		const h = await started(counting, { productionExceptionHandler: recording('continue', calls) })
		const changelog = [...h.topology.internalTopics.values()].find(spec => spec.kind === 'changelog')!.name
		h.producer.steps.send.always(() => (h.producer.sent.at(-1)!.topic === changelog ? tooLarge() : 'ok'))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(calls).toHaveLength(1))
		expect(calls[0]![0].topic).toBe(changelog)
		h.io.clock.advance(1000)
		await expectHalted(h)
	})

	it('a send no answer can make succeed (authorization) stops the app without asking the handler', async () => {
		const handler = vi.fn((): HandlerResponse => 'continue')
		const h = await started(pipeline(), { productionExceptionHandler: handler })
		h.producer.steps.send.next(new KafkaProtocolError(ErrorCode.TopicAuthorizationFailed))
		h.consumer.deliver('in:0', [{ key: 'k', value: json('a') }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		h.io.clock.advance(1000)
		await expectHalted(h)
		expect(handler).not.toHaveBeenCalled()
	})

	it('exactly-once: a rejected record stops the app whatever the handler says (it poisons the transaction)', async () => {
		const handler = vi.fn((): HandlerResponse => 'continue')
		const h = await started(pipeline(), {
			processingGuarantee: 'exactly_once',
			productionExceptionHandler: handler,
		})
		h.producer.steps.send.next(tooLarge())
		h.consumer.deliver('in:0', [{ key: 'k', value: json('a') }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		h.io.clock.advance(1000)
		await expectHalted(h)
		expect(handler).not.toHaveBeenCalled()
		expect(h.producer.transactions.map(t => t.state)).toEqual(['aborted'])
		expect(h.output()).toEqual([])
	})
})

describe('uncaught exception handler', () => {
	it("'restart' rejoins after a fatal failure like a migration; the function form gets the error", async () => {
		const seen: Error[] = []
		const h = await started(pipeline(), {
			uncaughtExceptionHandler: error => {
				seen.push(error)
				return 'restart'
			},
		})
		h.consumer.deliver('in:0', [{ key: 'k', value: json('boom') }])
		await vi.waitFor(() => expect(seen).toHaveLength(1))
		expect(seen[0]).toBeInstanceOf(FailedProcessingError)
		expect(h.lifecycle.state).toBe('REBALANCING')
		expect(h.lifecycle.error).toBe(seen[0])
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		h.io.clock.advance(500)
		await h.joined(2)

		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.consumer.deliver('in:0', [{ key: 'k', value: json('a') }])
		await vi.waitFor(() => expect(h.output()).toEqual(['a']))
		await h.stop()
		expect(h.committed()).toBe(1n)
	})

	it("an uncaught exception handler that throws stops the app ('shutdown')", async () => {
		const h = await started(pipeline(), {
			uncaughtExceptionHandler: () => {
				throw new Error('handler bug')
			},
		})
		h.consumer.deliver('in:0', [{ key: 'k', value: json('boom') }])
		await expectHalted(h)
		expect(h.io.created.consumers).toHaveLength(1)
	})

	it("'restart' after a global table failure stops the app: a global table cannot be restarted (KS)", async () => {
		const handler = vi.fn(() => 'restart' as const)
		const h = new Harness(
			app => {
				const users = app.globalTable('users', { key: string(), value: codec.json<string>() })
				app.stream('in', strings)
					.join(users, (value, user) => `${value}:${user}`)
					.to('out', strings)
			},
			{ uncaughtExceptionHandler: handler },
			{ partitions: { in: 1, out: 1, users: 1 } }
		)
		await h.started()
		h.io.cluster.append('users', 0, { key: 'k', value: '{not json' })
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(handler).toHaveBeenCalledOnce()
		h.io.clock.advance(60_000)
		expect(h.io.created.consumers).toHaveLength(1)
		await h.stop()
	})
})
