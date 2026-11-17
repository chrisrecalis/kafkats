import { describe, expect, it } from 'vitest'
import {
	CommitFailedError,
	ErrorCode,
	KafkaProtocolError,
	RebalanceInProgressError,
	TimeoutError,
	TransactionAbortedError,
} from '@kafkats/client'
import { TaskCorruptedError, TaskMigratedError } from '@/api/errors.js'
import type { StreamTask } from '@/runtime/task.js'
import { passThrough, pipe, TaskHarness, type TaskHarnessOptions } from '../support/task-harness.js'

/** A harness with one RUNNING task per partition that each processed one record; every port call is logged. */
async function setup(options: Partial<TaskHarnessOptions> & { tasks?: number } = {}) {
	const count = options.tasks ?? 1
	const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }), partitions: count, ...options })
	const tasks: StreamTask[] = []
	for (let p = 0; p < count; p++) tasks.push(await h.start(h.task(0, p)))
	const collector = h.collector
	const flush = collector.flush.bind(collector)
	collector.flush = signal => {
		h.events.push('flush')
		return flush(signal)
	}
	h.consumer.steps.commit.always(() => (h.events.push('commit'), 'ok'))
	for (const task of tasks) {
		h.enqueue(task, 'input', [{ key: 'k', value: `v${task.partition}`, timestamp: 1 }])
		await h.process(task)
	}
	return { h, tasks }
}

const transactionCalls = (h: TaskHarness) => h.producer.transactions.map(t => t.calls)

describe('CommitCoordinator at-least-once (4.8.1)', () => {
	it('flushes, commits the offsets with the commit metadata, checkpoints, then reports the commit', async () => {
		const { h, tasks } = await setup({ tasks: 2 })
		h.events.length = 0
		h.clock.advance(10)
		expect(await h.commit()).toBe('committed')
		expect(h.events).toEqual([
			'flush',
			'commit',
			'checkpoint 0_0',
			'checkpoint 0_1',
			'checkpoints.flush',
			'noteCommitted',
		])
		expect(h.consumer.commits).toEqual([
			[
				expect.objectContaining({ topic: 'input', partition: 0, offset: 1n }),
				expect.objectContaining({ topic: 'input', partition: 1, offset: 1n }),
			],
		])
		expect(tasks.map(t => t.needsCommit(tp => h.consumer.nextOffset(tp)))).toEqual([false, false])
		expect(h.commits.lastCommitAt).toBe(h.clock.now())
		expect(h.commits.lastSuccessAt).toBe(h.clock.now())
		expect(h.commits.committedOnce).toBe(true)
	})

	it("nothing to commit: 'nothing', no offset commit and no checkpoint", async () => {
		const { h } = await setup()
		await h.commit()
		h.events.length = 0
		h.clock.advance(10)
		expect(await h.commit()).toBe('nothing')
		expect(h.events).toEqual(['flush'])
		expect(h.commits.lastCommitAt).toBe(h.clock.now())
	})

	it("a failed run commits nothing: 'nothing' without any flush, send or commit (runtime§6.1)", async () => {
		const { h } = await setup()
		h.events.length = 0
		h.failed = true
		for (const reason of ['interval', 'user', 'revocation', 'close'] as const) {
			expect(await h.commit(reason)).toBe('nothing')
		}
		expect(h.events).toEqual([])
		expect(h.consumer.commits).toEqual([])
	})

	it("interval and user commits are 'skipped' while a rebalance is pending; revocation and close commits are not", async () => {
		const { h } = await setup()
		h.consumer.requestRebalance()
		h.events.length = 0
		expect(await h.commit('interval')).toBe('skipped')
		expect(await h.commit('user')).toBe('skipped')
		expect(h.events).toEqual([])
		expect(await h.commit('revocation')).toBe('committed')
	})

	for (const [name, error] of [
		['a commit timeout', new TimeoutError('commit', 10_000)],
		['a rebalance', new RebalanceInProgressError('app')],
	] as const) {
		it(`${name} defers an interval commit to the next interval; failing for taskTimeoutMs migrates (KF5)`, async () => {
			const { h, tasks } = await setup({ taskTimeoutMs: 1000 })
			h.consumer.steps.commit.always(error)
			const start = h.commits.lastSuccessAt
			h.clock.advance(400)
			expect(await h.commit()).toBe('deferred')
			expect(h.commits.lastCommitAt).toBe(start + 400)
			expect(h.commits.lastSuccessAt).toBe(start)
			expect(h.state(tasks[0]!).checkpoints).toEqual([])
			expect(tasks[0]!.needsCommit(tp => h.consumer.nextOffset(tp))).toBe(true)
			h.clock.advance(700)
			const failure = await h.commit().catch((e: unknown) => e)
			expect(failure).toBeInstanceOf(TaskMigratedError)
			expect((failure as Error).cause).toBe(error)
		})
	}

	it('the commit deadline passing counts as a timeout', async () => {
		const { h } = await setup()
		h.consumer.steps.commit.always('hang')
		expect(await h.commits.commit('interval', AbortSignal.timeout(5))).toBe('deferred')
	})

	it('the commit deadline passing during the flush defers an interval commit; the sends stay pending', async () => {
		const { h, tasks } = await setup()
		h.producer.steps.send.next('hang')
		h.enqueue(tasks[0]!, 'input', [{ key: 'k', value: 'slow', timestamp: 2 }])
		await h.process(tasks[0]!)
		expect(await h.commits.commit('interval', AbortSignal.timeout(5))).toBe('deferred')
		expect(h.consumer.commits).toEqual([])
		await expect(h.commits.commit('revocation', AbortSignal.timeout(5))).rejects.toThrow()
		h.producer.steps.send.answerAll()
		expect(await h.commit()).toBe('committed')
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ offset: 2n })]])
	})

	it('a rebalance at a revocation commit leaves the offsets uncommitted but writes the checkpoints', async () => {
		const { h, tasks } = await setup()
		h.consumer.steps.commit.always(new RebalanceInProgressError('app'))
		expect(await h.commit('revocation')).toBe('committed')
		expect(h.state(tasks[0]!).checkpoints).toHaveLength(1)
		expect(tasks[0]!.lastCommitted({ topic: 'input', partition: 0 })).toBeUndefined()
		expect(h.events).not.toContain('noteCommitted')
		expect(h.logger.warnings.some(w => w.includes('next owner reprocesses'))).toBe(true)
	})

	it('a timeout at a revocation or close commit is a failure', async () => {
		const { h } = await setup()
		const timeout = new TimeoutError('commit', 10_000)
		h.consumer.steps.commit.always(timeout)
		await expect(h.commit('revocation')).rejects.toBe(timeout)
		await expect(h.commit('close')).rejects.toBe(timeout)
	})

	it("a failed commit with a task lost meanwhile is 'lost'; otherwise the error is rethrown as is", async () => {
		const { h, tasks } = await setup()
		const failed = new CommitFailedError('app', new KafkaProtocolError(ErrorCode.IllegalGeneration))
		h.consumer.steps.commit.next(() => (tasks[0]!.markLost(), failed))
		expect(await h.commit()).toBe('lost')

		const other = await setup()
		other.h.consumer.steps.commit.always(failed)
		await expect(other.h.commit()).rejects.toBe(failed)
	})

	it('a failing flush commits nothing', async () => {
		const { h } = await setup()
		h.collector.flush = () => Promise.reject(new Error('send failed'))
		await expect(h.commit()).rejects.toThrow('send failed')
		expect(h.consumer.commits).toEqual([])
	})

	it('tasks whose sends failed are left out and reported as corrupted after the others committed (KF5)', async () => {
		const { h, tasks } = await setup({ tasks: 2 })
		const [a, b] = tasks as [StreamTask, StreamTask]
		a.markCorrupted()
		const failure = await h.commit().catch((e: unknown) => e)
		expect(failure).toBeInstanceOf(TaskCorruptedError)
		expect((failure as TaskCorruptedError).tasks).toEqual([a.id])
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ partition: 1 })]])
		expect(h.state(a).checkpoints).toEqual([])
		expect(h.state(b).checkpoints).toHaveLength(1)
	})

	it('a corrupted task stays out of every later commit until revived: an op queued before the revive commits none of its offsets (F1)', async () => {
		const { h, tasks } = await setup({ tasks: 2 })
		const [a, b] = tasks as [StreamTask, StreamTask]
		a.markCorrupted()
		await expect(h.commit()).rejects.toThrow(TaskCorruptedError)
		for (const task of tasks) {
			h.enqueue(task, 'input', [{ key: 'k', value: 'again', timestamp: 2 }])
			await h.process(task)
		}
		for (const reason of ['revocation', 'user', 'close'] as const) {
			const failure = await h.commit(reason).catch((e: unknown) => e)
			expect((failure as TaskCorruptedError).tasks).toEqual([a.id])
		}
		expect(h.consumer.commits.flat().map(o => `${o.partition}@${o.offset}`)).toEqual(['1@1', '1@2'])
		expect(h.state(a).checkpoints).toEqual([])
		expect(b.needsCommit(tp => h.consumer.nextOffset(tp))).toBe(false)
	})

	it('a deferred commit reports the corrupted tasks at once', async () => {
		const { h, tasks } = await setup({ tasks: 2 })
		const [a] = tasks as [StreamTask, StreamTask]
		a.markCorrupted()
		h.consumer.steps.commit.always(new TimeoutError('commit', 10_000))
		const start = h.commits.lastSuccessAt
		h.clock.advance(10)
		const failure = await h.commit().catch((e: unknown) => e)
		expect(failure).toBeInstanceOf(TaskCorruptedError)
		expect((failure as TaskCorruptedError).tasks).toEqual([a.id])
		// The other task commits at the next interval.
		expect(h.commits.lastCommitAt).toBe(start + 10)
		expect(h.commits.lastSuccessAt).toBe(start)
		expect(tasks.map(t => h.state(t).checkpoints)).toEqual([[], []])
	})

	it('a task lost or suspended since its sends failed is not reported as corrupted', async () => {
		const { h, tasks } = await setup({ tasks: 3 })
		const [a, b, c] = tasks as [StreamTask, StreamTask, StreamTask]
		b.markCorrupted()
		c.markCorrupted()
		b.markLost()
		await c.suspend()
		expect(await h.commit()).toBe('committed')
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ partition: a.partition })]])
	})

	it('a run that fails while the sends are flushed commits nothing (F2, runtime§6.1)', async () => {
		const { h } = await setup()
		const flush = h.collector.flush.bind(h.collector)
		h.collector.flush = async signal => {
			await flush(signal)
			h.failed = true
		}
		h.events.length = 0
		expect(await h.commit('revocation')).toBe('nothing')
		expect(h.events).toEqual(['flush'])
		expect(h.consumer.commits).toEqual([])
	})

	it('a revocation commits a task whose fetch position moved past its last commit (CR13, markers)', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: passThrough }) })
		const task = await h.start(
			h.task(),
			new Map([['input:0', { committed: { offset: 4n, metadata: null }, resolved: 4n }]])
		)
		h.consumer.partition('input:0').position = 6n
		expect(await h.commit('revocation')).toBe('committed')
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ offset: 6n })]])
		expect(task.lastCommitted({ topic: 'input', partition: 0 })).toBe(6n)
	})

	it('commits only RUNNING tasks that are not lost', async () => {
		const { h, tasks } = await setup({ tasks: 3 })
		tasks[1]!.markLost()
		await tasks[2]!.suspend()
		await h.commit()
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ partition: 0 })]])
	})
})

describe('CommitCoordinator exactly-once (4.8.2)', () => {
	it('flushes, commits offsets and outputs in one transaction, applies the buffers, then checkpoints', async () => {
		const { h, tasks } = await setup({ eos: true, tasks: 2 })
		h.events.length = 0
		expect(await h.commit()).toBe('committed')
		expect(h.events).toEqual([
			'flush',
			'apply 0_0',
			'apply 0_1',
			'checkpoint 0_0',
			'checkpoint 0_1',
			'checkpoints.flush',
			'noteCommitted',
		])
		const [transaction] = h.producer.transactions
		expect(transaction!.calls).toEqual(['send', 'send', 'sendOffsets', 'commit'])
		expect(transaction!.group).toBe(h.assignment)
		expect(transaction!.offsets.map(o => [o.partition, o.offset])).toEqual([
			[0, 1n],
			[1, 1n],
		])
		expect(h.state(tasks[0]!).checkpoints[0]!.eos).toBe(true)
		expect(h.collector.transactionOpen).toBe(false)
	})

	it("nothing to commit and no transaction open: 'nothing'; an open transaction is committed", async () => {
		const { h } = await setup({ eos: true })
		await h.commit()
		expect(await h.commit()).toBe('nothing')
		await h.collector.ensureTransaction(AbortSignal.timeout(1000))
		expect(await h.commit()).toBe('committed')
		expect(transactionCalls(h)).toEqual([['send', 'sendOffsets', 'commit'], ['commit']])
	})

	it("a failed run commits nothing: 'nothing' without any flush or transaction call", async () => {
		const { h } = await setup({ eos: true })
		h.events.length = 0
		h.failed = true
		expect(await h.commit('revocation')).toBe('nothing')
		expect(h.events).toEqual([])
		expect(transactionCalls(h)).toEqual([['send']])
	})

	it('a run that fails while the sends are flushed commits no transaction; its end aborts it (F2, runtime§6.1)', async () => {
		const { h, tasks } = await setup({ eos: true })
		const flush = h.collector.flush.bind(h.collector)
		h.collector.flush = async signal => {
			await flush(signal)
			h.failed = true
		}
		expect(await h.commit('revocation')).toBe('nothing')
		expect(transactionCalls(h)).toEqual([['send']])
		expect(h.state(tasks[0]!).checkpoints).toEqual([])
	})

	it("a lost task: abort, every buffer dropped, 'lost'", async () => {
		const { h, tasks } = await setup({ eos: true, tasks: 2 })
		tasks[0]!.markLost()
		expect(await h.commit()).toBe('lost')
		expect(transactionCalls(h)).toEqual([['send', 'send', 'abort']])
		expect(tasks.map(t => h.state(t).bufferDiscards)).toEqual([1, 1])
	})

	for (const [name, change] of [
		[
			'the generation moved on',
			(h: TaskHarness) => void (h.consumer.group = { ...h.consumer.group!, generationId: 2 }),
		],
		['the consumer lost its membership', (h: TaskHarness) => void (h.consumer.group = undefined)],
		[
			'the assignment has no generation',
			(h: TaskHarness) => void (h.assignment = { ...h.assignment!, generationId: -1 }),
		],
		['no assignment was captured', (h: TaskHarness) => void (h.assignment = undefined)],
	] as const) {
		it(`offsets are fenced by the assignment's generation (CR1): ${name} -> abort, TaskMigratedError`, async () => {
			const { h, tasks } = await setup({ eos: true })
			change(h)
			await expect(h.commit()).rejects.toThrow(TaskMigratedError)
			expect(transactionCalls(h)).toEqual([['send', 'abort']])
			expect(h.state(tasks[0]!).bufferDiscards).toBe(1)
		})
	}

	it('a loss during the flush aborts: nothing of the transaction commits (CR1)', async () => {
		const { h, tasks } = await setup({ eos: true })
		const flush = h.collector.flush
		h.collector.flush = async signal => {
			tasks[0]!.markLost()
			h.consumer.group = undefined
			return flush(signal)
		}
		expect(await h.commit()).toBe('lost')
		expect(transactionCalls(h)).toEqual([['send', 'abort']])
		expect(h.producer.committedRecords('output')).toEqual([])
		expect(h.state(tasks[0]!).checkpoints).toEqual([])
	})

	for (const [name, fail] of [
		['sendOffsets', (h: TaskHarness, error: Error) => void h.producer.steps.sendOffsets.next(error)],
		['commit', (h: TaskHarness, error: Error) => void h.producer.steps.commit.next(error)],
		['flush', (h: TaskHarness, error: Error) => void (h.collector.flush = () => Promise.reject(error))],
	] as const) {
		it(`a failing ${name} aborts, drops every buffer and rethrows the original error`, async () => {
			const { h, tasks } = await setup({ eos: true, tasks: 2 })
			const error = new TransactionAbortedError('app-1', undefined, 'unknown')
			fail(h, error)
			await expect(h.commit()).rejects.toBe(error)
			expect(transactionCalls(h)[0]!.at(-1)).toBe('abort')
			expect(tasks.map(t => h.state(t).bufferDiscards)).toEqual([1, 1])
			expect(tasks.map(t => h.state(t).checkpoints)).toEqual([[], []])
			expect(h.events).not.toContain('noteCommitted')
		})
	}
})
