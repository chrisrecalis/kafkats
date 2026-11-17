import { describe, expect, it } from 'vitest'
import {
	ConnectionError,
	ErrorCode,
	KafkaProtocolError,
	ProducerFencedError,
	RecordTooLargeError,
	TimeoutError,
	TransactionAbortedError,
} from '@kafkats/client'
import { FailedProcessingError, IllegalStateError, StreamsError, TaskMigratedError } from '@/api/errors.js'
import type { ErrorHandlerContext, HandlerResponse, ProductionExceptionHandler } from '@/api/handlers.js'
import { string } from '@/codec.js'
import type { InternalProcessorContext, ProcessorContext } from '@/processor/api.js'
import type { CollectedRecord, SendKind } from '@/processor/output.js'
import { classify } from '@/runtime/errors.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import type { StreamTask } from '@/runtime/task.js'
import type { KeyValueStore } from '@/state.js'
import { kvStore, passThrough, pipe, TaskHarness } from '../support/task-harness.js'

const RECORD: CollectedRecord = {
	topic: 'output',
	key: Buffer.from('k'),
	value: Buffer.from('v'),
	timestamp: 5,
	headers: { h: Buffer.from('1') },
}

function changelog(partition = 0): CollectedRecord {
	return { topic: 'app-s-changelog', partition, key: Buffer.from('k'), value: Buffer.from('v') }
}

/** A harness whose producer answers sends only when the test says so (`pending`, in send order). */
function manual(options: { eos?: boolean; production?: ProductionExceptionHandler } = {}) {
	const h = new TaskHarness({
		build: b => pipe(b, { processor: passThrough }),
		eos: options.eos,
		handlers: options.production ? { production: options.production } : {},
	})
	const sends = h.producer.steps.send.always('hang')
	return { h, pending: sends.hung, ackAll: () => sends.answerAll() }
}

async function running(h: TaskHarness, partition = 0): Promise<StreamTask> {
	return h.start(h.task(0, partition))
}

function send(h: TaskHarness, task: StreamTask, kind: SendKind, record: CollectedRecord = RECORD): void {
	h.collector.forTask(task).send(kind, record)
}

const signal = () => AbortSignal.timeout(5000)

describe('RecordCollector at-least-once', () => {
	it('sends through the producer with the record timestamp and headers; a send needs a commit', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await running(h)
		expect(task.needsCommit(() => undefined)).toBe(false)
		send(h, task, 'sink', { ...RECORD, partition: 2 })
		expect(h.producer.sent).toEqual([
			{
				topic: 'output',
				message: {
					key: Buffer.from('k'),
					value: Buffer.from('v'),
					partition: 2,
					timestamp: new Date(5),
					headers: { h: Buffer.from('1') },
				},
			},
		])
		expect(task.needsCommit(() => undefined)).toBe(true)
		await expect(h.collector.flush(signal())).resolves.toBeUndefined()
	})

	it('changelog records report their acked offsets per task until taken', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), partitions: 2 })
		const a = await running(h, 0)
		const b = await running(h, 1)
		send(h, a, 'changelog', changelog(0))
		send(h, a, 'changelog', changelog(0))
		send(h, b, 'changelog', changelog(1))
		send(h, a, 'sink')
		await h.collector.flush(signal())
		expect(h.collector.takeChangelogOffsets(a)).toEqual(new Map([['app-s-changelog:0', 1n]]))
		expect(h.collector.takeChangelogOffsets(a)).toEqual(new Map())
		expect(h.collector.takeChangelogOffsets(b)).toEqual(new Map([['app-s-changelog:1', 0n]]))
	})

	it('a retriable failure corrupts only the task that sent the record, for good; its changelog acks are dropped', async () => {
		const { h, pending, ackAll } = manual()
		const a = await running(h, 0)
		const b = await running(h, 0)
		send(h, a, 'sink')
		send(h, b, 'sink')
		pending[0]!.answer(new ConnectionError('broker', 9092))
		ackAll()
		await h.collector.flush(signal())
		expect([a.corrupted, b.corrupted]).toEqual([true, false])
		send(h, a, 'changelog', changelog())
		ackAll()
		await h.collector.flush(signal())
		expect(a.corrupted).toBe(true)
		expect(h.collector.takeChangelogOffsets(a)).toEqual(new Map())
	})

	for (const error of [new TimeoutError('produce', 30_000), new KafkaProtocolError(ErrorCode.NotLeaderOrFollower)]) {
		it(`${error.name} after the producer's retries is task-scoped`, async () => {
			const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
			h.producer.steps.send.always(error)
			const task = await running(h)
			send(h, task, 'sink')
			await expect(h.collector.flush(signal())).resolves.toBeUndefined()
			expect(task.corrupted).toBe(true)
		})
	}

	it("a rejected record goes to the production handler: 'continue' drops it", async () => {
		const seen: [ErrorHandlerContext, CollectedRecord, unknown, string][] = []
		const h = new TaskHarness({
			build: b => pipe(b, { processor: passThrough }),
			handlers: {
				production: (ctx, record, error, origin) => {
					seen.push([ctx, record, error, origin])
					return 'continue'
				},
			},
		})
		const tooLarge = new RecordTooLargeError('output', 0, 3_000_000, 1_000_000)
		h.producer.steps.send.always(tooLarge)
		const task = await running(h)
		send(h, task, 'sink')
		await expect(h.collector.flush(signal())).resolves.toBeUndefined()
		expect(task.corrupted).toBe(false)
		expect(seen).toHaveLength(1)
		const [ctx, record, error, origin] = seen[0]!
		expect(ctx).toMatchObject({
			taskId: task.id,
			topic: null,
			partition: -1,
			timestamp: 5,
			headers: RECORD.headers,
		})
		expect([record, error, origin]).toEqual([RECORD, tooLarge, 'send'])
		expect(h.logger.warnings).toContain('dropped a record whose send failed')
	})

	it('the production handler sees the input record the task was processing when it sent (KS)', async () => {
		const seen: ErrorHandlerContext[] = []
		const h = new TaskHarness({
			build: b => pipe(b, { processor: passThrough }),
			handlers: { production: ctx => (seen.push(ctx), 'continue') },
		})
		h.producer.steps.send.always(new RecordTooLargeError('output', 0, 2, 1))
		const task = await running(h)
		h.enqueue(task, 'input', [{ key: 'k', value: 'v', timestamp: 7 }], 0)
		await h.process(task)
		await h.collector.flush(signal())
		expect(seen).toEqual([
			expect.objectContaining({
				topic: 'input',
				partition: 0,
				offset: 0n,
				timestamp: 7,
				sourceRawKey: Buffer.from('k'),
				sourceRawValue: Buffer.from('v'),
			}),
		])
	})

	for (const [name, production, kind] of [
		["'fail'", () => 'fail', 'sink'],
		[
			'a throwing handler',
			() => {
				throw new Error('handler broke')
			},
			'sink',
		],
		["'continue' for a changelog record", () => 'continue', 'changelog'],
	] as const) {
		it(`${name} fails the run with a FailedProcessingError at the next flush, until discarded`, async () => {
			const h = new TaskHarness({
				build: b => pipe(b, { processor: passThrough }),
				handlers: { production: production as () => HandlerResponse },
			})
			h.producer.steps.send.always(new RecordTooLargeError('output', 0, 2, 1))
			const task = await running(h)
			send(h, task, kind)
			await expect(h.collector.flush(signal())).rejects.toThrow(FailedProcessingError)
			await expect(h.collector.flush(signal())).rejects.toThrow(FailedProcessingError)
			h.collector.discard()
			await expect(h.collector.flush(signal())).resolves.toBeUndefined()
		})
	}

	// KS RecordCollectorImpl.isFatalException: never handed to the production handler, under either guarantee (4.6.5).
	for (const code of [
		ErrorCode.TopicAuthorizationFailed,
		ErrorCode.GroupAuthorizationFailed,
		ErrorCode.ClusterAuthorizationFailed,
		ErrorCode.TransactionalIdAuthorizationFailed,
		ErrorCode.SaslAuthenticationFailed,
		ErrorCode.SecurityDisabled,
		ErrorCode.InvalidTopicException,
		ErrorCode.OffsetMetadataTooLarge,
	]) {
		it(`${ErrorCode[code]} is fatal whatever the production handler says (KS)`, async () => {
			let handled = 0
			const h = new TaskHarness({
				build: b => pipe(b, { processor: passThrough }),
				handlers: { production: () => (handled++, 'continue') },
			})
			const error = new KafkaProtocolError(code)
			h.producer.steps.send.always(error)
			const task = await running(h)
			send(h, task, 'sink')
			const failure = await h.collector.flush(signal()).catch((e: unknown) => e)
			expect(failure).toBeInstanceOf(StreamsError)
			expect(classify(failure, 'commit')).toBe('fatal')
			expect((failure as Error).cause).toBe(error)
			expect(handled).toBe(0)
		})
	}

	it('a fenced producer migrates the run', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		h.producer.steps.send.always(new ProducerFencedError(1n, 2))
		const task = await running(h)
		send(h, task, 'sink')
		await expect(h.collector.flush(signal())).rejects.toThrow(TaskMigratedError)
	})

	it('discard() forgets pending sends: their late acks and failures are ignored', async () => {
		const { h, pending } = manual()
		const task = await running(h)
		send(h, task, 'changelog', changelog())
		send(h, task, 'sink')
		h.collector.discard()
		const [ack, failure] = pending
		ack!.answer()
		failure!.answer(new RecordTooLargeError('output', 0, 2, 1))
		await expect(h.collector.flush(signal())).resolves.toBeUndefined()
		expect(h.collector.takeChangelogOffsets(task)).toEqual(new Map())
	})

	it('a flush whose sends were given up meanwhile (discard, close) fails: nothing may be committed for them', async () => {
		for (const giveUp of ['discard', 'close'] as const) {
			const { h, pending } = manual()
			const task = await running(h)
			send(h, task, 'sink')
			const flushed = h.collector.flush(signal())
			h.collector[giveUp]()
			pending[0]!.answer(new ConnectionError('broker', 9092))
			await expect(flushed).rejects.toThrow(TaskMigratedError)
			expect(task.corrupted).toBe(false)
		}
	})

	it('flush waits for every send, also those tracked while flushing, bounded by its signal', async () => {
		const { h, pending, ackAll } = manual()
		const task = await running(h)
		send(h, task, 'sink')
		let flushed = false
		const flush = h.collector.flush(signal()).then(() => (flushed = true))
		await Promise.resolve()
		send(h, task, 'sink')
		pending[0]!.answer()
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(flushed).toBe(false)
		ackAll()
		await flush
		send(h, task, 'sink')
		await expect(h.collector.flush(AbortSignal.timeout(10))).rejects.toThrow()
	})

	it('drops sends of a task that is neither RUNNING nor initializing, and of a lost task', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const created = h.task()
		send(h, created, 'sink')
		const lost = await running(h)
		lost.markLost()
		send(h, lost, 'sink')
		const suspended = await running(h)
		await suspended.suspend()
		send(h, suspended, 'sink')
		expect(h.producer.sent).toHaveLength(0)
		expect(h.logger.warnings).toHaveLength(3)
	})

	it('accepts sends of processors initializing; they need a commit', async () => {
		const h = new TaskHarness({
			build: b =>
				pipe(b, {
					processor: () => ({
						init: ctx => {
							;(ctx as InternalProcessorContext<string, string>).output().send('sink', RECORD)
						},
						process() {},
					}),
				}),
		})
		const task = await running(h)
		expect(h.producer.sent).toHaveLength(1)
		expect(task.needsCommit(() => undefined)).toBe(true)
	})

	it('a closed collector drops sends, ignores late acks and aborts its partition count loads', async () => {
		const { h, pending } = manual()
		let loadSignal: AbortSignal | undefined
		const collector = new RecordCollector(h.producer, {
			eos: false,
			handlers: h.handlers,
			counts: {
				get: () => undefined,
				load: (_topic, s) => {
					loadSignal = s
					return Promise.resolve(3)
				},
			},
			logger: h.logger,
		})
		h.collector = collector
		const task = await running(h)
		const output = collector.forTask(task)
		expect(output.partitionCount('output')).toBeUndefined()
		expect(await output.loadPartitionCount('output')).toBe(3)
		output.send('changelog', changelog())
		collector.close()
		expect(loadSignal!.aborted).toBe(true)
		output.send('sink', RECORD)
		expect(h.producer.sent).toHaveLength(1)
		pending[0]!.answer()
		await Promise.resolve()
		expect(collector.takeChangelogOffsets(task)).toEqual(new Map())
	})

	it('a partition count load cut short by close() is runtime control (migrate), not a processing failure', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		h.collector = new RecordCollector(h.producer, {
			eos: false,
			handlers: h.handlers,
			counts: {
				get: () => undefined,
				load: (_topic, s) =>
					new Promise((_resolve, reject) => s.addEventListener('abort', () => reject(s.reason))),
			},
			logger: h.logger,
		})
		const task = await running(h)
		const load = h.collector.forTask(task).loadPartitionCount('output')
		h.collector.close()
		const failure = await load.catch((e: unknown) => e)
		expect(failure).toBeInstanceOf(TaskMigratedError)
		expect(classify(failure, 'processing')).toBe('migrate')
	})
})

describe('RecordCollector exactly-once', () => {
	it('sends on the open transaction; a send with none open is a bug', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const task = await running(h)
		expect(h.collector.transactionOpen).toBe(true)
		send(h, task, 'sink')
		const [transaction] = h.producer.transactions
		expect(transaction!.sent).toHaveLength(1)
		await h.collector.commitTransaction([], h.assignment!, signal())
		expect(h.collector.transactionOpen).toBe(false)
		expect(() => send(h, task, 'sink')).toThrow(IllegalStateError)
	})

	it('after an abort, sends of a slice that outlived it are dropped until the next transaction (4.11 a)', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const task = await running(h)
		await h.collector.abortTransaction(signal())
		send(h, task, 'sink')
		expect(h.producer.sent).toHaveLength(0)
		expect(h.logger.warnings).toContain('dropped a record sent after its transaction was aborted')
		expect(task.needsCommit(() => undefined)).toBe(false)
		await h.collector.ensureTransaction(signal())
		send(h, task, 'sink')
		expect(h.producer.transactions[1]!.sent).toHaveLength(1)
		await h.collector.commitTransaction([], h.assignment!, signal())
		expect(() => send(h, task, 'sink')).toThrow(IllegalStateError)
	})

	it('opens one transaction for concurrent callers', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		await Promise.all([h.collector.ensureTransaction(signal()), h.collector.ensureTransaction(signal())])
		await h.collector.ensureTransaction(signal())
		expect(h.producer.transactions).toHaveLength(1)
	})

	it('commits offsets with the group metadata in the transaction; offsets alone open one', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const offsets = [{ topic: 'input', partition: 0, offset: 3n, metadata: 'm' }]
		await h.collector.commitTransaction([], h.assignment!, signal())
		expect(h.producer.transactions).toHaveLength(0)
		await h.collector.commitTransaction(offsets, h.assignment!, signal())
		const [transaction] = h.producer.transactions
		expect(transaction!.calls).toEqual(['sendOffsets', 'commit'])
		expect(transaction!.offsets).toEqual(offsets)
		expect(transaction!.group).toBe(h.assignment)
		expect(transaction!.state).toBe('committed')
	})

	it('an abort while the sends are flushed fails the flush and any commit of the offsets alone (F2)', async () => {
		const { h, pending } = manual({ eos: true })
		const task = await running(h)
		send(h, task, 'sink')
		const flushed = h.collector.flush(signal())
		const aborted = h.collector.abortTransaction(signal())
		pending[0]!.answer()
		await expect(flushed).rejects.toThrow(TaskMigratedError)
		await aborted
		const offsets = [{ topic: 'input', partition: 0, offset: 1n, metadata: 'm' }]
		for (const committed of [offsets, []]) {
			await expect(h.collector.commitTransaction(committed, h.assignment!, signal())).rejects.toThrow(
				TaskMigratedError
			)
		}
		expect(h.producer.transactions.map(t => [t.state, t.calls])).toEqual([['aborted', ['send', 'abort']]])
	})

	it('a closed collector commits no offsets', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		h.collector.close()
		const offsets = [{ topic: 'input', partition: 0, offset: 1n, metadata: 'm' }]
		await expect(h.collector.commitTransaction(offsets, h.assignment!, signal())).rejects.toThrow(TaskMigratedError)
		expect(h.producer.transactions).toHaveLength(0)
	})

	it('changelog offsets are those of the open transaction; an abort drops them', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const task = await running(h)
		send(h, task, 'changelog', changelog())
		await h.collector.flush(signal())
		await h.collector.abortTransaction(signal())
		expect(h.collector.takeChangelogOffsets(task)).toEqual(new Map())
		await h.collector.ensureTransaction(signal())
		send(h, task, 'changelog', changelog())
		await h.collector.flush(signal())
		expect(h.collector.takeChangelogOffsets(task)).toEqual(new Map([['app-s-changelog:0', 1n]]))
	})

	it('abort waits until every send was answered, then aborts; late answers are ignored', async () => {
		const { h, pending } = manual({ eos: true })
		const task = await running(h)
		send(h, task, 'changelog', changelog())
		send(h, task, 'sink')
		const [transaction] = h.producer.transactions
		const aborted = h.collector.abortTransaction(signal())
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(transaction!.calls).toEqual(['send', 'send'])
		const [ack, failure] = pending
		ack!.answer()
		failure!.answer(new TransactionAbortedError('tx'))
		await aborted
		expect(transaction!.calls).toEqual(['send', 'send', 'abort'])
		expect(h.collector.transactionOpen).toBe(false)
		expect(h.collector.takeChangelogOffsets(task)).toEqual(new Map())
		await expect(h.collector.flush(signal())).resolves.toBeUndefined()
	})

	it('abort never throws', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		await running(h)
		h.producer.transactions[0]!.abort = () => Promise.reject(new Error('broker gone'))
		await expect(h.collector.abortTransaction(signal())).resolves.toBeUndefined()
		expect(h.logger.warnings).toContain('aborting the transaction failed')
	})

	it('an abort while a transaction is opening aborts that one', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const opening = h.collector.ensureTransaction(signal())
		await h.collector.abortTransaction(signal())
		await opening
		expect(h.collector.transactionOpen).toBe(false)
		expect(h.producer.transactions[0]!.state).toBe('aborted')
	})

	it('a transaction asked for while an abort runs is a new one, begun once the abort finished', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		const begin = h.producer.beginTransaction.bind(h.producer)
		let open!: () => void
		const calls: string[] = []
		h.producer.beginTransaction = async () => {
			calls.push('begin')
			if (calls.length === 1) await new Promise<void>(resolve => (open = resolve))
			const transaction = await begin({ signal: signal() })
			const abort = transaction.abort.bind(transaction)
			transaction.abort = options => (calls.push('abort'), abort(options))
			return transaction
		}
		const first = h.collector.ensureTransaction(signal())
		const aborted = h.collector.abortTransaction(signal())
		const second = h.collector.ensureTransaction(signal())
		open()
		await Promise.all([first, aborted, second])
		expect(calls).toEqual(['begin', 'abort', 'begin'])
		expect(h.producer.transactions.map(t => t.state)).toEqual(['aborted', 'open'])
		expect(h.collector.transactionOpen).toBe(true)
		const task = await running(h)
		send(h, task, 'sink')
		expect(h.producer.transactions[1]!.sent).toHaveLength(1)
	})

	it('a transaction begins only once every abort finished, also one overlapped by a later abort', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		await h.collector.ensureTransaction(signal())
		const [first] = h.producer.transactions
		const calls: string[] = []
		let release!: () => void
		first!.abort = async () => {
			calls.push('abort')
			await new Promise<void>(resolve => (release = resolve))
			first!.state = 'aborted'
			calls.push('aborted')
		}
		const begin = h.producer.beginTransaction.bind(h.producer)
		h.producer.beginTransaction = options => (calls.push('begin'), begin(options))
		const aborting = h.collector.abortTransaction(signal())
		// A later abort finds no transaction: it is done at once, while the first one still runs.
		await h.collector.abortTransaction(signal())
		const opening = h.collector.ensureTransaction(signal())
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(calls).toEqual(['abort'])
		release()
		await Promise.all([aborting, opening])
		expect(calls).toEqual(['abort', 'aborted', 'begin'])
		expect(h.collector.transactionOpen).toBe(true)
	})

	it('a commit that an abort took over meanwhile leaves the transaction opened after the abort', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		await h.collector.ensureTransaction(signal())
		const [first] = h.producer.transactions
		let committed!: () => void
		const commitCalled = new Promise<void>(called => {
			first!.commit = () => (called(), new Promise<void>(resolve => (committed = resolve)))
		})
		const committing = h.collector.commitTransaction([], h.assignment!, signal())
		await commitCalled
		await h.collector.abortTransaction(signal())
		await h.collector.ensureTransaction(signal())
		committed()
		await committing
		expect(h.collector.transactionOpen).toBe(true)
		expect(h.producer.transactions[1]!.state).toBe('open')
	})

	for (const [name, error, expected] of [
		['a retriable failure', new ConnectionError('broker', 9092), TaskMigratedError],
		['a fenced producer', new KafkaProtocolError(ErrorCode.InvalidProducerEpoch), TaskMigratedError],
		[
			'a rejected record, whatever the handler says',
			new RecordTooLargeError('output', 0, 2, 1),
			FailedProcessingError,
		],
		['an authorization failure', new KafkaProtocolError(ErrorCode.TopicAuthorizationFailed), StreamsError],
		['an aborted transaction', new TransactionAbortedError('tx'), TransactionAbortedError],
	] as const) {
		it(`${name} fails the run: ${expected.name}`, async () => {
			let handled = 0
			const h = new TaskHarness({
				build: b => pipe(b, { processor: passThrough }),
				eos: true,
				handlers: { production: () => (handled++, 'continue') },
			})
			h.producer.steps.send.always(error)
			const task = await running(h)
			send(h, task, 'sink')
			await expect(h.collector.flush(signal())).rejects.toThrow(expected)
			expect(handled).toBe(0)
		})
	}

	it('a closed collector does not open transactions', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), eos: true })
		h.collector.close()
		await h.collector.ensureTransaction(signal())
		expect(h.producer.transactions).toHaveLength(0)
	})

	it("a task's close() that forwards and writes leaves nothing in the committed transaction and the store unchanged (CR9)", async () => {
		let store!: KeyValueStore<string, string>
		const h = new TaskHarness({
			eos: true,
			build: b =>
				pipe(b, {
					stores: [kvStore('s')],
					processor: () => {
						let context: ProcessorContext<string, string>
						return {
							init: ctx => {
								context = ctx
								store = ctx.getStateStore('s')
							},
							process: record => context.forward(record),
							close: async () => {
								await store.put('k', 'closing')
								await context.forward({
									key: 'k',
									value: 'closing',
									timestamp: 1,
									headers: {},
								} as never)
							},
						}
					},
				}),
		})
		const task = await running(h)
		h.enqueue(task, 'input', [{ key: 'k', value: 'v', timestamp: 1 }])
		await h.process(task)
		expect(await h.commit('revocation')).toBe('committed')
		await expect(task.suspend()).rejects.toThrow("failed to close processor 'processor'")
		expect(h.collector.transactionOpen).toBe(false)
		expect(await h.commit('close')).toBe('nothing')
		expect(h.producer.sent.map(r => r.message.value?.toString())).toEqual(['v'])
		expect(h.producer.committedRecords('output').map(r => r.message.value?.toString())).toEqual(['v'])
		expect(await (task.store('s') as KeyValueStore<string, string>).get('k')).toBeUndefined()
	})
})

describe('sink partitioner (runtime§6.60)', () => {
	for (const partition of [2, 1.5, -1]) {
		it(`a partitioner returning ${partition} of 2 partitions is a StreamsError`, async () => {
			const h = new TaskHarness({
				build: b => {
					b.addSource({
						name: 'source',
						topics: ['input'],
						keyCodec: string(),
						valueCodec: string(),
						internal: false,
					})
					b.addSink({
						name: 'sink',
						parents: ['source'],
						topic: 'output',
						keyCodec: string(),
						valueCodec: string(),
						partitioner: () => partition,
						internal: false,
					})
				},
			})
			h.collector = new RecordCollector(h.producer, {
				eos: false,
				handlers: h.handlers,
				counts: { get: () => 2, load: () => Promise.resolve(2) },
				logger: h.logger,
			})
			const task = await running(h)
			h.enqueue(task, 'input', [{ key: 'k', value: 'v', timestamp: 1 }])
			let error: unknown
			try {
				await task.processOne(h.clock.now())
			} catch (e) {
				error = e
			}
			expect(error).toBeInstanceOf(FailedProcessingError)
			expect((error as Error).cause).toBeInstanceOf(StreamsError)
			expect(((error as Error).cause as Error).message).toContain(`returned partition ${partition}`)
			expect(h.producer.sent).toHaveLength(0)
		})
	}
})
