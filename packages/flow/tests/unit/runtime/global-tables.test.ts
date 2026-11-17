/**
 * GlobalStateManager over fake IO (4.12): one case per rule. Bootstrap (wipe, read to the end captured at its start,
 * completion after markers, deserialization handler, idle timeout, out of range), follow (consumer failures with
 * backoff, row failures fatal), the re-bootstrap behind the gate (FlowRuntime: commit first, processing paused, hooks
 * go on), and stop (an abort is no failure).
 */
import { describe, expect, it, vi } from 'vitest'
import { ErrorCode, KafkaProtocolError, string, TimeoutError, type Codec } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { FailedProcessingError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { AppLifecycle } from '@/app/lifecycle.js'
import { SourceTopicNotFoundError } from '@/changelog/errors.js'
import { codec } from '@/codec.js'
import { GlobalStateManager } from '@/runtime/global-tables.js'
import type { RuntimeIO } from '@/runtime/io.js'
import { FlowRuntime } from '@/runtime/runtime.js'
import type { KeyValueStore, StateStoreProvider, ValueAndTimestamp } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import { FakeIO, type FakeRestoreConsumer, type LoggedTransaction, type TestRecord } from '../support/fake-io.js'
import { RecordingLogger } from '../support/task-harness.js'

const APP = 'app'
const client = { clientId: APP, brokers: ['localhost:9092'] }
const strings = { key: string(), value: string() }
const tp = (partition: number) => ({ topic: 'users', partition })

interface HarnessOptions {
	readonly io?: FakeIO
	readonly provider?: StateStoreProvider
	readonly partitions?: number
	readonly value?: Codec<unknown>
	readonly config?: Partial<FlowConfig>
	/** Applied to each global consumer when it is created. */
	readonly onConsumer?: (consumer: FakeRestoreConsumer) => void
}

/** A GlobalStateManager of the global table `users` (store `users`) over fake IO, with a recording gate. */
class Harness {
	readonly io: FakeIO
	readonly logger = new RecordingLogger()
	readonly fatal: Error[] = []
	/** What the gate was asked, in order. */
	readonly gate: string[] = []
	readonly manager: GlobalStateManager
	readonly shutdown = new AbortController()

	constructor(options: HarnessOptions = {}) {
		this.io = options.io ?? new FakeIO({ groupId: APP })
		this.io.cluster.createTopic('users', options.partitions ?? 1)
		const app = flow({ applicationId: APP, client, ...options.config })
		app.globalTable('users', {
			key: string(),
			value: options.value ?? string(),
			materialized: { storeName: 'users' },
		})
		const checkpoints = new Checkpoints(undefined, () => {})
		const { io, onConsumer } = { ...options, io: this.io }
		const ports: Pick<RuntimeIO, 'globalConsumers' | 'admin' | 'clock'> = {
			admin: io.admin,
			clock: io.clock,
			globalConsumers: groupId => {
				const consumer = io.globalConsumers(groupId)
				onConsumer?.(consumer)
				return consumer
			},
		}
		this.manager = new GlobalStateManager({
			io: ports,
			defs: internals(app).build().globalStores,
			stateDirectory: new StateDirectory(APP, options.provider ?? inMemory(), checkpoints, this.logger),
			config: { ...internals(app).settings, processId: 'p' },
			onFatal: error => void this.fatal.push(error),
			gate: {
				pause: async () => void this.gate.push('pause'),
				resume: () => void this.gate.push('resume'),
			},
			logger: this.logger,
		})
	}

	get consumers(): FakeRestoreConsumer[] {
		return this.io.created.globalConsumers
	}

	append(partition: number, ...records: TestRecord[]): bigint[] {
		return this.io.cluster.append('users', partition, ...records)
	}

	bootstrap(): Promise<void> {
		return this.manager.bootstrap(this.shutdown.signal)
	}

	async followed(): Promise<void> {
		await this.bootstrap()
		this.manager.follow(this.shutdown.signal)
	}

	/** The table's rows: key -> value. */
	async rows(): Promise<Record<string, unknown>> {
		const store = this.manager.store('users') as KeyValueStore<string, ValueAndTimestamp<unknown>>
		const rows: Record<string, unknown> = {}
		for await (const [key, { value }] of store.all()) rows[key] = value
		return rows
	}

	stop(): Promise<void> {
		this.shutdown.abort()
		return this.manager.stop(AbortSignal.timeout(1000))
	}
}

/** A provider whose stores outlive a start, as a persistent provider's do. */
function persistent(): StateStoreProvider {
	const base = inMemory()
	const stores = new Map<string, KeyValueStore<unknown, unknown>>()
	return {
		name: 'persistent',
		createKeyValueStore: <K, V>(
			name: string,
			options: Parameters<StateStoreProvider['createKeyValueStore']>[1]
		) => {
			let store = stores.get(name)
			if (store === undefined) {
				store = base.createKeyValueStore(name, options as never)
				stores.set(name, store)
			}
			return store as KeyValueStore<K, V>
		},
		createWindowStore: (name, options) => base.createWindowStore(name, options),
		createSessionStore: (name, options) => base.createSessionStore(name, options),
		close: async () => {},
	}
}

/** Resolves to whether `promise` settled by now (after pending callbacks ran). */
async function settled(promise: Promise<unknown>): Promise<boolean> {
	let done = false
	promise.then(
		() => (done = true),
		() => (done = true)
	)
	await new Promise(resolve => setImmediate(resolve))
	return done
}

describe('GlobalStateManager: bootstrap', () => {
	it('reads every partition up to the end of its topic at the start, outside the group, from the earliest offset', async () => {
		const h = new Harness({ partitions: 2 })
		h.append(0, { key: 'k1', value: 'a' }, { key: 'k2', value: 'b' })
		h.append(1, { key: 'k3', value: 'c' })
		// An open transaction ends the stable part of partition 1: the bootstrap does not wait for it.
		const open: { state: LoggedTransaction['state'] } = { state: 'open' }
		h.io.cluster
			.log('users', 1)
			.append({ key: Buffer.from('k4'), value: Buffer.from('d'), timestamp: 0n, headers: {} }, open)

		await h.bootstrap()
		expect(await h.rows()).toEqual({ k1: 'a', k2: 'b', k3: 'c' })
		expect(h.consumers.map(c => c.groupId)).toEqual(['app-global-users-p'])
		expect(h.consumers[0]!.calls).toEqual(['assign users:0@0 users:1@0', 'run'])

		// The consumer goes on reading: what is committed later is applied.
		open.state = 'committed'
		h.io.cluster.log('users', 1).appendMarker(open)
		h.append(0, { key: 'k1', value: null })
		await vi.waitFor(async () => expect(await h.rows()).toEqual({ k2: 'b', k3: 'c', k4: 'd' }))
		await h.stop()
		expect(h.consumers[0]!.calls.at(-1)).toBe('stop')
	})

	it('wipes the store first: rows of an earlier start that its topic no longer has are gone', async () => {
		const provider = persistent()
		const io = new FakeIO({ groupId: APP })
		const first = new Harness({ io, provider })
		first.append(0, { key: 'k1', value: 'a' }, { key: 'k2', value: 'b' })
		await first.bootstrap()
		await first.stop()

		io.cluster.log('users', 0).deleteBefore(1n)
		const second = new Harness({ io, provider })
		await second.bootstrap()
		expect(await second.rows()).toEqual({ k2: 'b' })
		await second.stop()
	})

	it('completes at the next check when its end lies behind a transaction marker', async () => {
		const h = new Harness({ onConsumer: consumer => (consumer.batchSize = 1) })
		const committed: { state: LoggedTransaction['state'] } = { state: 'open' }
		const log = h.io.cluster.log('users', 0)
		log.append({ key: Buffer.from('k1'), value: Buffer.from('a'), timestamp: 0n, headers: {} }, committed)
		committed.state = 'committed'
		log.appendMarker(committed)

		const bootstrap = h.bootstrap()
		await vi.waitFor(async () => expect(await h.rows()).toEqual({ k1: 'a' }))
		// The consumer moved past the marker without a batch: only the periodic check sees it.
		expect(await settled(bootstrap)).toBe(false)
		h.io.clock.advance(1000)
		await bootstrap
		await h.stop()
	})

	it('resolves at once for an empty topic; a missing topic is SourceTopicNotFoundError', async () => {
		const empty = new Harness()
		await empty.bootstrap()
		expect(await empty.rows()).toEqual({})
		await empty.stop()

		const missing = new Harness()
		missing.io.cluster.topics.delete('users')
		await expect(missing.bootstrap()).rejects.toThrow(SourceTopicNotFoundError)
		await missing.stop()
	})

	it("skips rows the deserialization handler lets go ('continue'); of the rows of a key in a batch the last one counts", async () => {
		const h = new Harness({ value: codec.json<string>(), config: { deserializationExceptionHandler: 'continue' } })
		h.append(
			0,
			{ key: 'k1', value: '{not json' },
			{ key: 'k2', value: '"b"' },
			{ key: 'k3', value: '"c1"' },
			{ key: 'k3', value: '"c2"' },
			// Skipped: k4 keeps the value before it, as if the row were not there.
			{ key: 'k4', value: '"d"' },
			{ key: 'k4', value: '{not json' }
		)
		await h.bootstrap()
		expect(await h.rows()).toEqual({ k2: 'b', k3: 'c2', k4: 'd' })
		await h.stop()
	})

	it("fails on a row the deserialization handler does not let go ('fail', the default); its consumer stops", async () => {
		const h = new Harness({ value: codec.json<string>() })
		h.append(0, { key: 'k1', value: '"a"' }, { key: 'k2', value: '{not json' })
		await expect(h.bootstrap()).rejects.toThrow(FailedProcessingError)
		expect(h.consumers[0]!.calls.at(-1)).toBe('stop')
		expect(h.fatal).toEqual([])
		await h.stop()
	})

	it('fails after idleTimeoutMs without progress; its consumer stops', async () => {
		const h = new Harness({ onConsumer: consumer => consumer.pause([tp(0)]) })
		h.append(0, { key: 'k1', value: 'a' })
		const bootstrap = h.bootstrap()
		await vi.waitFor(() => expect(h.consumers).toHaveLength(1))
		h.io.clock.advance(29_000)
		expect(await settled(bootstrap)).toBe(false)
		h.io.clock.advance(1000)
		await expect(bootstrap).rejects.toThrow(TimeoutError)
		expect(h.consumers[0]!.calls.at(-1)).toBe('stop')
		await h.stop()
	})

	it('starts again from the new earliest offset when records were deleted before they were read', async () => {
		const h = new Harness()
		h.append(0, { key: 'k1', value: 'a' }, { key: 'k2', value: 'b' })
		// The records before offset 1 go between listing the earliest offsets and the first fetch.
		h.io.admin.steps.listOffsets.next('ok', () => {
			h.io.cluster.log('users', 0).deleteBefore(1n)
			return 'ok'
		})
		await h.bootstrap()
		expect(await h.rows()).toEqual({ k2: 'b' })
		expect(h.consumers.map(c => c.calls[0])).toEqual(['assign users:0@0', 'assign users:0@1'])
		expect(h.consumers[0]!.calls.at(-1)).toBe('stop')
		// Processing has not started: the gate is not asked.
		expect(h.gate).toEqual([])
		await h.stop()
	})

	it('a stop rejects a bootstrap in flight without reporting a failure', async () => {
		const h = new Harness()
		h.io.admin.steps.listOffsets.always('hang')
		const bootstrap = h.bootstrap()
		await vi.waitFor(() => expect(h.io.admin.steps.listOffsets.hung.length).toBeGreaterThan(0))
		await h.stop()
		await expect(bootstrap).rejects.toThrow()
		expect(h.fatal).toEqual([])
	})
})

describe('GlobalStateManager: follow', () => {
	it('replaces a failed consumer after the backoff, from the applied positions; the backoff grows', async () => {
		const h = new Harness()
		h.append(0, { key: 'k1', value: 'a' })
		await h.followed()

		h.consumers[0]!.fail(new Error('connection reset'))
		await vi.waitFor(() => expect(h.consumers[0]!.calls.at(-1)).toBe('stop'))
		h.io.clock.advance(999)
		expect(h.consumers).toHaveLength(1)
		h.io.clock.advance(1)
		await vi.waitFor(() => expect(h.consumers).toHaveLength(2))
		expect(h.consumers[1]!.calls.slice(0, 2)).toEqual(['assign users:0@1', 'run'])
		h.append(0, { key: 'k2', value: 'b' })
		await vi.waitFor(async () => expect(await h.rows()).toEqual({ k1: 'a', k2: 'b' }))

		h.consumers[1]!.fail(new Error('connection reset'))
		await vi.waitFor(() => expect(h.consumers[1]!.calls.at(-1)).toBe('stop'))
		h.io.clock.advance(1999)
		expect(h.consumers).toHaveLength(2)
		h.io.clock.advance(1)
		await vi.waitFor(() => expect(h.consumers).toHaveLength(3))
		expect(h.consumers[2]!.calls[0]).toBe('assign users:0@2')
		expect(h.logger.warnings.filter(w => w.includes('starts again after a backoff'))).toHaveLength(2)
		expect(h.fatal).toEqual([])
		await h.stop()
	})

	it('a row failure stops the app: onFatal, no new consumer', async () => {
		const h = new Harness({ value: codec.json<string>() })
		h.append(0, { key: 'k1', value: '"a"' })
		await h.followed()
		h.append(0, { key: 'k2', value: '{not json' })
		await vi.waitFor(() => expect(h.fatal).toHaveLength(1))
		expect(h.fatal[0]).toBeInstanceOf(FailedProcessingError)
		h.io.clock.advance(60_000)
		expect(h.consumers).toHaveLength(1)
		expect(h.logger.errors).toEqual(['a global table cannot be kept up to date: the app stops'])
		await h.stop()
	})

	it('a consumer failure no retry fixes stops the app', async () => {
		const h = new Harness()
		await h.followed()
		h.consumers[0]!.fail(new KafkaProtocolError(ErrorCode.TopicAuthorizationFailed))
		await vi.waitFor(() => expect(h.fatal).toHaveLength(1))
		expect(h.consumers).toHaveLength(1)
		await h.stop()
	})

	it('a stop during the backoff is no failure: no new consumer', async () => {
		const h = new Harness()
		await h.followed()
		h.consumers[0]!.fail(new Error('connection reset'))
		await vi.waitFor(() => expect(h.consumers[0]!.calls.at(-1)).toBe('stop'))
		await h.stop()
		h.io.clock.advance(60_000)
		await new Promise(resolve => setImmediate(resolve))
		expect(h.consumers).toHaveLength(1)
		expect(h.fatal).toEqual([])
	})

	it('out of range: behind the gate, the table is wiped and loaded again; a failed load is retried with backoff', async () => {
		const h = new Harness()
		h.append(0, { key: 'k1', value: 'a' }, { key: 'k2', value: 'b' })
		await h.followed()
		h.io.admin.steps.listOffsets.next(new Error('broker down'))
		// The consumer is at offset 2; retention deletes up to offset 3 before it reads the new records.
		h.append(0, { key: 'k9', value: 'x' }, { key: 'k1', value: 'c' })
		h.io.cluster.log('users', 0).deleteBefore(3n)

		await vi.waitFor(() => expect(h.consumers[0]!.calls).toContain('stop'))
		expect(h.gate).toEqual(['pause'])
		expect(h.logger.warnings).toContain(
			"a global table's position is out of range: the table is loaded again while stream processing waits"
		)
		await vi.waitFor(() => expect(h.logger.warnings.some(w => w.includes('after a backoff'))).toBe(true))
		expect(h.consumers).toHaveLength(1)
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.gate).toEqual(['pause', 'resume']))
		expect(await h.rows()).toEqual({ k1: 'c' })
		expect(h.consumers.map(c => c.calls[0])).toEqual(['assign users:0@0', 'assign users:0@3'])

		// The new consumer follows.
		h.append(0, { key: 'k2', value: 'd' })
		await vi.waitFor(async () => expect(await h.rows()).toEqual({ k1: 'c', k2: 'd' }))
		expect(h.fatal).toEqual([])
		await h.stop()
	})

	it('a stop during a load after out of range is no failure; processing stays paused', async () => {
		const h = new Harness()
		h.append(0, { key: 'k1', value: 'a' })
		await h.followed()
		h.io.admin.steps.listOffsets.always('hang')
		h.append(0, { key: 'k9', value: 'x' })
		h.io.cluster.log('users', 0).deleteBefore(2n)
		await vi.waitFor(() => expect(h.io.admin.steps.listOffsets.hung.length).toBeGreaterThan(0))
		await h.stop()
		expect(h.gate).toEqual(['pause'])
		expect(h.fatal).toEqual([])
		expect(h.consumers).toHaveLength(1)
	})

	it('the view of a store only reads', async () => {
		const h = new Harness()
		h.append(0, { key: 'k1', value: 'a' })
		await h.bootstrap()
		const view = h.manager.store('users') as KeyValueStore<string, ValueAndTimestamp<string>>
		expect((await view.get('k1'))?.value).toBe('a')
		await expect(view.put('k2', { value: 'b', timestamp: 0 })).rejects.toThrow('processors may only read it')
		expect(() => h.manager.store('other')).toThrow("no global store named 'other'")
		await h.stop()
	})
})

describe('GlobalStateManager in the FlowRuntime: the re-bootstrap gate', () => {
	/** `in` joined with the global table `users` -> `out`. */
	function joined(app: FlowApp): void {
		const users = app.globalTable('users', { ...strings, materialized: { storeName: 'users' } })
		app.stream('in', strings)
			.join(users, (value, user) => `${value}:${user}`)
			.to('out', strings)
	}

	it('commits first, processes nothing while the table loads again, keeps the rebalance hooks going, then resumes', async () => {
		const io = new FakeIO({ groupId: APP })
		const lifecycle = new AppLifecycle()
		const logger = new RecordingLogger()
		const checkpoints = new Checkpoints(undefined, () => {})
		for (const topic of ['in', 'out', 'users']) io.cluster.createTopic(topic, 1)
		io.cluster.append('users', 0, { key: 'k1', value: 'alice' })
		const app = flow({ applicationId: APP, client, commitIntervalMs: 60_000 })
		joined(app)
		const runtime = new FlowRuntime({
			topology: internals(app).build(),
			config: { ...internals(app).settings, processId: 'p' },
			io,
			connection: { connect: async () => {}, disconnect: async () => {} },
			lifecycle,
			stateDirectory: new StateDirectory(APP, inMemory(), checkpoints, logger),
			checkpoints,
			topics: {},
			logger,
		})
		const output = () =>
			io.cluster
				.log('out', 0)
				.read(0n, 'read_committed')
				.messages.map(m => m.value.toString())
		const committed = () => io.cluster.offsets(APP).get('in:0')?.offset

		const starting = lifecycle.start(() => runtime.start(runtime.shutdown.signal))
		await vi.waitFor(() => expect(io.created.consumers).toHaveLength(1))
		const consumer = io.created.consumers[0]!
		await consumer.rebalance('in:0')
		await starting
		consumer.deliver('in:0', [{ key: 'k1', value: 'a' }])
		await vi.waitFor(() => expect(output()).toEqual(['a:alice']))
		expect(committed()).toBeUndefined()

		// The global consumer is at offset 1; records up to offset 2 are deleted before it reads them.
		io.admin.steps.listOffsets.always('hang')
		io.cluster.append('users', 0, { key: 'k9', value: 'x' }, { key: 'k1', value: 'bob' })
		io.cluster.log('users', 0).deleteBefore(2n)
		await vi.waitFor(() => expect(io.admin.steps.listOffsets.hung.length).toBeGreaterThan(0))
		expect(runtime.processingPaused).toBe(true)
		// The gate committed what was processed before it paused.
		expect(committed()).toBe(1n)

		// Exclusive ops go on while processing waits: a rebalance completes.
		await consumer.rebalance('in:0')
		await vi.waitFor(() => expect(lifecycle.state).toBe('RUNNING'))
		consumer.deliver('in:0', [{ key: 'k1', value: 'b' }])
		await new Promise(resolve => setTimeout(resolve, 20))
		expect(output()).toEqual(['a:alice'])

		io.admin.steps.listOffsets.always('ok').answerAll()
		await vi.waitFor(() => expect(output()).toEqual(['a:alice', 'b:bob']))
		expect(runtime.processingPaused).toBe(false)
		expect(lifecycle.state).toBe('RUNNING')
		await lifecycle.close(() => runtime.stop(io.clock.now() + 20_000))
		expect(io.created.globalConsumers.every(c => c.calls.includes('stop'))).toBe(true)
	})

	it('two tables loaded again at once: processing waits for the last of them (F4)', async () => {
		const io = new FakeIO({ groupId: APP })
		const lifecycle = new AppLifecycle()
		const logger = new RecordingLogger()
		const checkpoints = new Checkpoints(undefined, () => {})
		for (const topic of ['in', 'out', 'users', 'orgs']) io.cluster.createTopic(topic, 1)
		io.cluster.append('users', 0, { key: 'k1', value: 'alice' })
		io.cluster.append('orgs', 0, { key: 'k1', value: 'acme' })
		const app = flow({ applicationId: APP, client, commitIntervalMs: 60_000 })
		const users = app.globalTable('users', { ...strings, materialized: { storeName: 'users' } })
		const orgs = app.globalTable('orgs', { ...strings, materialized: { storeName: 'orgs' } })
		app.stream('in', strings)
			.join(users, (value, user) => `${value}:${user}`)
			.join(orgs, (value, org) => `${value}:${org}`)
			.to('out', strings)
		const runtime = new FlowRuntime({
			topology: internals(app).build(),
			config: { ...internals(app).settings, processId: 'p' },
			io,
			connection: { connect: async () => {}, disconnect: async () => {} },
			lifecycle,
			stateDirectory: new StateDirectory(APP, inMemory(), checkpoints, logger),
			checkpoints,
			topics: {},
			logger,
		})
		const output = () =>
			io.cluster
				.log('out', 0)
				.read(0n, 'read_committed')
				.messages.map(m => m.value.toString())
		const starting = lifecycle.start(() => runtime.start(runtime.shutdown.signal))
		await vi.waitFor(() => expect(io.created.consumers).toHaveLength(1))
		const consumer = io.created.consumers[0]!
		await consumer.rebalance('in:0')
		await starting
		consumer.deliver('in:0', [{ key: 'k1', value: 'a' }])
		await vi.waitFor(() => expect(output()).toEqual(['a:alice:acme']))

		// Both followers fall behind their log start; the load of orgs hangs, the one of users completes.
		io.admin.steps.listOffsets.always(() => (io.admin.calls.at(-1)!.includes('orgs:0') ? 'hang' : 'ok'))
		io.cluster.append('orgs', 0, { key: 'k9', value: 'x' }, { key: 'k1', value: 'globex' })
		io.cluster.log('orgs', 0).deleteBefore(2n)
		await vi.waitFor(() => expect(io.admin.steps.listOffsets.hung.length).toBeGreaterThan(0))
		io.cluster.append('users', 0, { key: 'k9', value: 'x' }, { key: 'k1', value: 'bob' })
		io.cluster.log('users', 0).deleteBefore(2n)
		// users is loaded again by a new consumer of its own (the third global consumer); orgs still waits.
		await vi.waitFor(() => expect(io.created.globalConsumers).toHaveLength(3))
		await new Promise(resolve => setTimeout(resolve, 20))
		expect(runtime.processingPaused).toBe(true)
		consumer.deliver('in:0', [{ key: 'k1', value: 'b' }])
		await new Promise(resolve => setTimeout(resolve, 20))
		expect(output()).toEqual(['a:alice:acme'])

		io.admin.steps.listOffsets.always('ok').answerAll()
		await vi.waitFor(() => expect(output()).toEqual(['a:alice:acme', 'b:bob:globex']))
		expect(runtime.processingPaused).toBe(false)
		await lifecycle.close(() => runtime.stop(io.clock.now() + 20_000))
	})
})
