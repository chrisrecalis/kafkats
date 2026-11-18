import { describe, expect, it, vi } from 'vitest'
import { OffsetOutOfRangeError } from '@kafkats/client'
import { TaskCorruptedError } from '@/api/errors.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import { codec } from '@/codec.js'
import type { TaskId } from '@/processor/api.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import type { ResolvedFlowConfig, ResolvedRestorationOptions } from '@/runtime/config.js'
import { classify, type FailureSite } from '@/runtime/errors.js'
import type { RestoreConsumerPort } from '@/runtime/io.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import { RestorationPool, type RestoringTask } from '@/runtime/restoration.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import { TaskManager } from '@/runtime/task-manager.js'
import type { KeyValueStore, StateStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { InMemoryKeyValueStore, inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import type { RegisteredStore } from '@/state/state-manager.js'
import { keyValueStoreBuilder, sessionStoreBuilder, windowStoreBuilder } from '@/state/store-builders.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import type { StoreBuilder } from '@/topology/nodes.js'
import {
	deferred,
	FakeAdmin,
	FakeCluster,
	FakeGroupConsumer,
	FakeProducer,
	FakeRestoreConsumer,
	FakeStartPositions,
	SerialExclusive,
} from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { APP, passThrough, pipe, RecordingLogger } from '../support/task-harness.js'

const str = codec.string()
const DEFAULTS = { applicationId: APP }

const OPTIONS: ResolvedRestorationOptions = {
	idleTimeoutMs: 5000,
	initialIdleTimeoutMs: 5000,
	checkIntervalMs: 10,
	maxConcurrentTasks: 4,
}

/** A pool over a fake cluster; its restore consumers are kept, and a test may wrap them. */
class PoolHarness {
	readonly cluster = new FakeCluster()
	readonly admin = new FakeAdmin(this.cluster)
	readonly logger = new RecordingLogger()
	readonly checkpoints = new Checkpoints(undefined, error => this.logger.error(error.message))
	readonly consumers: FakeRestoreConsumer[] = []
	/** Applied to every restore consumer created. */
	wrap: (consumer: FakeRestoreConsumer) => RestoreConsumerPort = consumer => consumer
	readonly pool: RestorationPool

	constructor(options: Partial<ResolvedRestorationOptions> = {}) {
		this.pool = new RestorationPool({
			io: {
				admin: this.admin,
				restoreConsumers: groupId => {
					const consumer = new FakeRestoreConsumer(this.cluster, groupId)
					this.consumers.push(consumer)
					return this.wrap(consumer)
				},
			},
			checkpoints: this.checkpoints,
			options: { ...OPTIONS, ...options },
			applicationId: APP,
			commitIntervalMs: Number.POSITIVE_INFINITY,
			logger: this.logger,
		})
	}

	write(topic: string, partition: number, ...records: [key: string | Buffer, value: string | null][]): void {
		const created = this.cluster.createTopic(topic, partition + 1)
		created.grow(partition + 1)
		for (const [key, value] of records) {
			created.partitions[partition]!.append({
				key: typeof key === 'string' ? Buffer.from(key) : key,
				value: (value === null ? null : Buffer.from(value)) as Buffer,
				timestamp: 0n,
				headers: {},
			})
		}
	}
}

function taskId(partition: number): TaskId {
	return Object.freeze({ subtopology: 0, partition, key: `0_${partition}` })
}

/** A task of partition `partition` with the stores of these builders (created and initialized, raw = top). */
async function task(partition: number, ...builders: StoreBuilder[]): Promise<RestoringTask> {
	const provider = inMemory()
	const stores = await Promise.all(
		builders.map(async (builder): Promise<RegisteredStore> => {
			const raw = builder.create(provider, `${APP}.${builder.name}.p${partition}`)
			await raw.init()
			const { logging } = builder
			const changelog = logging.enabled
				? { topic: logging.topic, partition, skipRestoration: logging.skipRestoration }
				: undefined
			return { builder, raw, top: raw, changelog }
		})
	)
	return { id: taskId(partition), stores }
}

const kvBuilder = (name: string, logging?: boolean | { skipRestoration: boolean }): StoreBuilder =>
	keyValueStoreBuilder(DEFAULTS, { name, keyCodec: str, valueCodec: str, logging }) as StoreBuilder

async function entries(store: StateStore): Promise<string[]> {
	const all: string[] = []
	for await (const [key, value] of (store as KeyValueStore<unknown, unknown>).all()) {
		all.push(`${JSON.stringify(key)}=${JSON.stringify(value)}`)
	}
	return all.sort()
}

/** Holds the first batch of every restore consumer until the test releases its restore (by group id order). */
function holdFirstBatches(h: PoolHarness): { held: string[]; release: (index: number) => void } {
	const gates: (() => void)[] = []
	const held: string[] = []
	h.wrap = consumer => {
		const run = consumer.run.bind(consumer)
		const gate = deferred()
		gates.push(() => gate.resolve())
		let first = true
		consumer.run = (onBatch, onOutOfRange) =>
			run(async (tp, messages) => {
				if (first) {
					first = false
					held.push(consumer.groupId)
					await gate.promise
				}
				await onBatch(tp, messages)
			}, onOutOfRange)
		return consumer
	}
	return { held, release: index => gates[index]!() }
}

const tick = (ms = 10) => new Promise(resolve => setTimeout(resolve, ms))

async function waitFor(condition: () => boolean): Promise<void> {
	for (let i = 0; i < 300 && !condition(); i++) await tick(5)
	expect(condition()).toBe(true)
}

describe('RestorationPool (4.7.1)', () => {
	it('runs at most maxConcurrentTasks jobs, in FIFO order, each with its own consumer `<appId>-restore-<taskId>-<n>`', async () => {
		const h = new PoolHarness({ maxConcurrentTasks: 2 })
		for (let p = 0; p < 3; p++) h.write(`${APP}-s-changelog`, p, [`k${p}`, `v${p}`])
		const tasks = await Promise.all([0, 1, 2].map(p => task(p, kvBuilder('s'))))
		const gates = holdFirstBatches(h)
		const signal = new AbortController().signal

		const done: string[] = []
		const restores = tasks.map(t => h.pool.restore(t, signal).then(() => void done.push(t.id.key)))
		await waitFor(() => gates.held.length === 2)
		await tick()
		expect(h.consumers.map(c => c.groupId.replace(/-\d+$/, ''))).toEqual(['app-restore-0_0', 'app-restore-0_1'])

		gates.release(1)
		await waitFor(() => gates.held.length === 3)
		expect(done).toEqual(['0_1'])
		gates.release(0)
		gates.release(2)
		await Promise.all(restores)

		expect(done).toEqual(['0_1', '0_0', '0_2'])
		const counters = h.consumers.map(c => Number(/-(\d+)$/.exec(c.groupId)![1]))
		expect(counters[1]).toBe(counters[0]! + 1)
		expect(counters[2]).toBe(counters[1]! + 1)
		for (const [p, t] of tasks.entries()) expect(await entries(t.stores[0]!.raw)).toEqual([`"k${p}"="v${p}"`])
	})

	it('a queued job rejects at once when aborted; the others go on', async () => {
		const h = new PoolHarness({ maxConcurrentTasks: 1 })
		h.write(`${APP}-s-changelog`, 0, ['a', '1'])
		h.write(`${APP}-s-changelog`, 1, ['b', '1'])
		const [t0, t1] = await Promise.all([task(0, kvBuilder('s')), task(1, kvBuilder('s'))])
		const gates = holdFirstBatches(h)
		const queued = new AbortController()

		const running = h.pool.restore(t0!, new AbortController().signal)
		const waiting = h.pool.restore(t1!, queued.signal)
		await waitFor(() => gates.held.length === 1)
		queued.abort()

		await expect(waiting).rejects.toMatchObject({ name: 'AbortError' })
		expect(h.consumers).toHaveLength(1)
		gates.release(0)
		await running
		expect(await entries(t0!.stores[0]!.raw)).toEqual(['"a"="1"'])
		expect(h.consumers).toHaveLength(1)
	})

	it('a running job rejects only after its in-flight store write, with its progress checkpointed; its slot is released', async () => {
		const h = new PoolHarness({ maxConcurrentTasks: 1 })
		h.write(`${APP}-s-changelog`, 0, ['a', '1'], ['b', '2'])
		const t0 = await task(0, kvBuilder('s'))
		const raw = t0.stores[0]!.raw as KeyValueStore<string, string>
		const write = deferred()
		const put = raw.put.bind(raw)
		let writing = false
		raw.put = async (key, value) => {
			writing = true
			await write.promise
			await put(key, value)
		}
		const abort = new AbortController()

		const restoring = h.pool.restore(t0, abort.signal)
		let settled = false
		void restoring.catch(() => (settled = true))
		await waitFor(() => writing)
		abort.abort()
		await tick(30)
		expect(settled).toBe(false)
		write.resolve()

		await expect(restoring).rejects.toMatchObject({ name: 'AbortError' })
		expect(await h.checkpoints.get(`${APP}-s-changelog`, 0)).toBe(2n)
		raw.put = put
		h.write(`${APP}-s-changelog`, 0, ['c', '3'])
		await h.pool.restore(t0, new AbortController().signal)
		expect(await entries(raw)).toEqual(['"a"="1"', '"b"="2"', '"c"="3"'])
		expect(h.consumers.at(-1)!.calls[0]).toBe(`assign ${APP}-s-changelog:0@2`)
	})

	it('a changelog that lost the records being restored (out of range, C7) is a TaskCorruptedError of the task', async () => {
		const h = new PoolHarness()
		h.write(`${APP}-s-changelog`, 0, ['a', '1'], ['b', '2'], ['c', '3'])
		const t0 = await task(0, kvBuilder('s'))
		h.wrap = consumer => {
			h.cluster.log(`${APP}-s-changelog`, 0).deleteBefore(2n)
			return consumer
		}

		const error: unknown = await h.pool.restore(t0, new AbortController().signal).catch((e: unknown) => e)

		expect(error).toBeInstanceOf(TaskCorruptedError)
		expect((error as TaskCorruptedError).tasks).toEqual([t0.id])
		expect((error as Error).cause).toBeInstanceOf(OffsetOutOfRangeError)
		expect(classify(error, 'restore')).toBe('corrupted')
	})

	it('resolves the max key time applied to each window and session store, by store name: window ends (6.2)', async () => {
		const h = new PoolHarness()
		const windowed = windowedKeyCodec(str)
		const key = (k: string, start: number, end: number) =>
			windowed.encode({ key: k, windowStart: start, windowEnd: end })
		h.write(`${APP}-w-changelog`, 0, [key('a', 100, 110), 'x'], [key('b', 120, 130), 'y'])
		h.write(`${APP}-ses-changelog`, 0, [key('a', 5, 70), 'x'])
		h.write(`${APP}-kv-changelog`, 0, ['a', 'x'])
		const t0 = await task(
			0,
			windowStoreBuilder(DEFAULTS, {
				name: 'w',
				keyCodec: str,
				valueCodec: str,
				retentionMs: 1000,
				windowSizeMs: 10,
			}) as StoreBuilder,
			sessionStoreBuilder(DEFAULTS, {
				name: 'ses',
				keyCodec: str,
				valueCodec: str,
				retentionMs: 1000,
			}) as StoreBuilder,
			kvBuilder('kv')
		)

		const { storeTimes } = await h.pool.restore(t0, new AbortController().signal)

		expect(storeTimes).toEqual(
			new Map([
				['w', 130],
				['ses', 70],
			])
		)
		expect(await entries(t0.stores[0]!.raw)).toEqual([
			'{"key":"a","windowStart":100,"windowEnd":110}="x"',
			'{"key":"b","windowStart":120,"windowEnd":130}="y"',
		])
		expect(h.consumers).toHaveLength(1)
	})

	it('a task without stores to restore (unlogged, skipRestoration) resolves at once without any call', async () => {
		const h = new PoolHarness({ maxConcurrentTasks: 1 })
		const t0 = await task(0, kvBuilder('unlogged', false), kvBuilder('skipped', { skipRestoration: true }))
		h.write(`${APP}-skipped-changelog`, 0, ['a', '1'])

		expect(await h.pool.restore(t0, AbortSignal.abort())).toEqual({ storeTimes: new Map() })
		expect(h.admin.calls).toEqual([])
		expect(h.consumers).toEqual([])
	})
})

describe('RestorationPool in the TaskManager: a corrupted restore revives the task, which rebuilds its state (4.7.5, DL7a)', () => {
	it('out of range mid-restore -> TaskCorruptedError -> revive({ rebuild: true }) rebuilds from the new log start and runs', async () => {
		const changelog = `${APP}-s-changelog`
		const h = new PoolHarness()
		h.write(changelog, 0, ...Array.from({ length: 10 }, (_, i): [string, string] => [`k${i}`, `v${i}`]))
		// The first restore consumer reads two records, then retention deletes what it still needs.
		let first = true
		h.wrap = consumer => {
			consumer.batchSize = 2
			if (!first) return consumer
			first = false
			const run = consumer.run.bind(consumer)
			consumer.run = (onBatch, onOutOfRange) =>
				run(async (tp, messages) => {
					await onBatch(tp, messages)
					h.cluster.log(changelog, 0).deleteBefore(6n)
				}, onOutOfRange)
			return consumer
		}
		const manager = taskManager(h)
		await manager.exclusive.run(() =>
			manager.manager.handleAssignment([{ topic: 'input', partition: 0, offset: 0n, committed: null }])
		)

		await waitFor(() => manager.failures.length === 1)
		const [corrupted] = manager.manager.tasks()
		expect(manager.failures[0]!.error).toBeInstanceOf(TaskCorruptedError)
		expect(manager.failures[0]!.site).toBe('restore')
		expect(corrupted!.state).toBe('RESTORING')
		expect(await h.checkpoints.get(changelog, 0)).toBe(2n)

		// FlowRuntime.fail's reaction to a corruption (4.6.1). Inside exclusive, the revival only invalidates the
		// checkpoint: the store is cleared by the restore of the new incarnation, outside exclusive (a store of millions
		// of entries must not hold up slices, commits and rebalance hooks meanwhile).
		const remove = vi.spyOn(InMemoryKeyValueStore.prototype, 'delete')
		const scan = vi.spyOn(InMemoryKeyValueStore.prototype, 'all')
		let insideExclusive: unknown[] | undefined
		await manager.exclusive.run(async () => {
			await manager.manager.revive([corrupted!], { rebuild: true })
			insideExclusive = [remove.mock.calls.length, scan.mock.calls.length, await h.checkpoints.get(changelog, 0)]
		})
		expect(insideExclusive).toEqual([0, 0, -1n])
		await waitFor(() => manager.manager.allRunning)
		expect(remove.mock.calls.map(([key]) => key)).toEqual(['k0', 'k1'])
		remove.mockRestore()
		scan.mockRestore()

		const [revived] = manager.manager.tasks()
		expect(revived).not.toBe(corrupted)
		expect(corrupted!.state).toBe('CLOSED')
		expect(revived!.state).toBe('RUNNING')
		expect(await entries(revived!.store('s'))).toEqual(['"k6"="v6"', '"k7"="v7"', '"k8"="v8"', '"k9"="v9"'])
		expect(await h.checkpoints.get(changelog, 0)).toBe(10n)
		expect(h.consumers.map(c => c.calls[0])).toEqual([`assign ${changelog}:0@0`, `assign ${changelog}:0@6`])
		expect(manager.consumer.calls).toEqual(['pause input:0', 'pause input:0', 'seek input:0 0', 'resume input:0'])
		expect(manager.failures).toHaveLength(1)
	})
})

/** A TaskManager over a fake group consumer and producer whose restorer is the harness's pool. */
function taskManager(h: PoolHarness) {
	const logger = new RecordingLogger()
	const clock = new ManualClock(1_000)
	const builder = new InternalTopologyBuilder(APP)
	pipe(builder, { topics: ['input'], processor: passThrough, stores: [kvBuilder('s')] })
	const config: ResolvedFlowConfig = {
		applicationId: APP,
		processId: 'process',
		eos: false,
		commitIntervalMs: 100,
		taskTimeoutMs: 300_000,
		startTimeoutMs: 120_000,
		maxTaskIdleMs: 0,
		bufferedRecordsPerPartition: 1000,
		emitIntervalMs: 0,
		repartitionPurgeIntervalMs: 0,
		stateCleanupDelayMs: 600_000,
		transactionTimeoutMs: 10_000,
		rebalanceTimeoutMs: 60_000,
		defaultTimestampExtractor: TimestampExtractors.failOnInvalid,
		defaultResetPolicy: 'earliest',
		handlers: {
			deserialization: () => 'fail',
			processing: () => 'fail',
			production: () => 'fail',
			uncaught: () => 'shutdown',
		},
		restoration: OPTIONS,
	}
	const consumer = new FakeGroupConsumer({ cluster: h.cluster, groupId: APP })
	consumer.group = { groupId: APP, generationId: 1, memberId: 'member' }
	const collector = new RecordCollector(new FakeProducer(h.cluster), {
		eos: false,
		handlers: config.handlers,
		counts: { get: () => 1, load: () => Promise.resolve(1) },
		logger,
	})
	const exclusive = new SerialExclusive()
	const failures: { error: unknown; site: FailureSite }[] = []
	const stop = new AbortController()
	const manager = new TaskManager({
		topology: builder.build(),
		counts: new Map([['input', 1]]),
		ids: new TaskIdRegistry(),
		config,
		stateDirectory: new StateDirectory(APP, inMemory(), h.checkpoints, logger),
		checkpoints: h.checkpoints,
		globalStore: name => {
			throw new Error(`no global store '${name}'`)
		},
		consumer,
		outputs: {
			forTask: t => collector.forTask(t),
			initialize: signal => collector.initialize(signal),
			ensureTransaction: signal => collector.ensureTransaction(signal),
		},
		restorer: h.pool,
		startPositions: new FakeStartPositions(),
		exclusive: exclusive.run,
		commit: () => Promise.resolve('nothing'),
		resetOutput: () => Promise.resolve(),
		onWork: () => {},
		requestCommit: () => {},
		onAllRunningChanged: () => {},
		failed: () => false,
		onFailure: (error, site) => void failures.push({ error, site }),
		hardStop: stop.signal,
		signal: stop.signal,
		clock,
		logger,
	})
	return { manager, consumer, exclusive, failures }
}
