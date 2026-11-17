/** FlowRuntime and Run of this build (WP2.3) over fake IO: start, commits, rebalances, failures that halt, close. */
import { describe, expect, it, vi } from 'vitest'
import { string } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { FailedProcessingError, StreamsError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { AppLifecycle } from '@/app/lifecycle.js'
import { COUNT_CODEC } from '@/dsl/ops/aggregate.js'
import { FlowRuntime } from '@/runtime/runtime.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { FakeIO, type FakeGroupConsumer, type FakeProducer } from '../support/fake-io.js'
import { RecordingLogger } from '../support/task-harness.js'

const APP = 'app'
const strings = { key: string(), value: string() }

/** `in` -> mapValues (throws for 'boom') -> `out`. */
function stateless(app: FlowApp): void {
	app.stream('in', strings)
		.mapValues(value => {
			if (value === 'boom') throw new Error('boom')
			return value?.toUpperCase() ?? null
		})
		.to('out', strings)
}

function counting(app: FlowApp): void {
	app.stream('in', strings).groupByKey().count().toStream().to('out', { key: string() })
}

/** A FlowRuntime over fake IO, started through an AppLifecycle like the app's. */
class Harness {
	readonly io = new FakeIO({ groupId: APP })
	readonly lifecycle = new AppLifecycle()
	readonly logger = new RecordingLogger()
	readonly checkpoints = new Checkpoints(undefined, () => {})
	readonly topology: BuiltTopology
	readonly runtime: FlowRuntime
	readonly connection = { connected: 0, disconnected: 0 }

	constructor(build: (app: FlowApp) => void, config: Partial<FlowConfig> = {}) {
		const app = flow({ applicationId: APP, client: { clientId: APP, brokers: ['localhost:9092'] }, ...config })
		build(app)
		this.topology = internals(app).build()
		this.io.cluster.createTopic('in', 1)
		this.io.cluster.createTopic('out', 1)
		const { connection } = this
		this.runtime = new FlowRuntime({
			topology: this.topology,
			config: { ...internals(app).settings, processId: 'process' },
			io: this.io,
			connection: {
				connect: async () => void connection.connected++,
				disconnect: async () => void connection.disconnected++,
			},
			lifecycle: this.lifecycle,
			stateDirectory: new StateDirectory(APP, inMemory(), this.checkpoints, this.logger),
			checkpoints: this.checkpoints,
			topics: {},
			logger: this.logger,
		})
	}

	get consumer(): FakeGroupConsumer {
		return this.io.created.consumers[0]!
	}

	get producer(): FakeProducer {
		return this.io.created.producers.at(-1)!
	}

	start(): Promise<void> {
		return this.lifecycle.start(() => this.runtime.start(this.runtime.shutdown.signal))
	}

	/** Starts, waits for the group member, assigns `in:0` and waits until its task runs. */
	async started(): Promise<void> {
		const starting = this.start()
		await this.joined()
		await this.consumer.rebalance('in:0')
		await starting
	}

	async joined(): Promise<void> {
		await vi.waitFor(() => expect(this.io.created.consumers).toHaveLength(1))
	}

	stop(): Promise<void> {
		return this.lifecycle.close(() => this.runtime.stop(this.io.clock.now() + 20_000))
	}

	committed(key = 'in:0'): bigint | undefined {
		return this.io.cluster.offsets(APP).get(key)?.offset
	}

	/** The committed values of `out`. */
	output<T = string>(decode: (value: Buffer) => T = value => value.toString() as T): T[] {
		const { messages } = this.io.cluster.log('out', 0).read(0n, 'read_committed')
		return messages.map(m => decode(m.value))
	}
}

describe('FlowRuntime and Run', () => {
	it('starts once the first assignment runs, processes, and commits at close (at-least-once)', async () => {
		const h = new Harness(stateless)
		await h.started()
		expect(h.lifecycle.state).toBe('RUNNING')
		expect(h.connection.connected).toBe(1)
		expect(h.consumer.calls[0]).toBe('run in')

		h.consumer.deliver('in:0', [
			{ key: 'k', value: 'a' },
			{ key: 'k', value: 'b' },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['A', 'B']))
		expect(h.committed()).toBeUndefined()

		await h.stop()
		expect(h.committed()).toBe(2n)
		expect(h.consumer.calls).toContain('stop')
		expect(h.producer.closed).toBe(true)
		expect(h.connection.disconnected).toBe(1)
		expect(h.lifecycle.state).toBe('STOPPED')
	})

	it('commits the open transaction with the offsets at close (exactly-once)', async () => {
		const h = new Harness(stateless, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		expect(h.output()).toEqual([])

		await h.stop()
		expect(h.output()).toEqual(['A'])
		expect(h.committed()).toBe(1n)
		expect(h.producer.transactions.map(t => t.state)).toEqual(['committed'])
	})

	it('commits at the interval and before a rebalance, after which the task resumes', async () => {
		const h = new Harness(counting, { commitIntervalMs: 1000 })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toHaveLength(1))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed()).toBe(1n))

		h.consumer.deliver('in:0', [{ key: 'k', value: 'b' }])
		await vi.waitFor(() => expect(h.output()).toHaveLength(2))
		await h.consumer.rebalance('in:0')
		expect(h.committed()).toBe(2n)
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))

		// The resumed task kept its store: its changelog checkpoint is at the end, so nothing needs a restore.
		h.consumer.deliver('in:0', [{ key: 'k', value: 'c' }])
		await vi.waitFor(() => expect(h.output()).toHaveLength(3))
		expect(h.output(COUNT_CODEC.decode)).toEqual([1, 2, 3])
		await h.stop()
		expect(h.committed()).toBe(3n)
	})

	it('after a loss under exactly-once, aborts, closes the tasks and continues on a new producer', async () => {
		const h = new Harness(stateless, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.io.created.producers[0]!.sent).toHaveLength(1))

		await h.consumer.lose()
		expect(h.io.created.producers).toHaveLength(2)
		expect(h.io.created.producers[0]!.transactions.map(t => t.state)).toEqual(['aborted'])
		await vi.waitFor(() => expect(h.io.created.producers[0]!.closed).toBe(true))

		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'b', offset: 0n }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		await h.stop()
		expect(h.output()).toEqual(['B'])
	})

	it('halts after a failure: ERROR, the uncaught handler told, nothing committed, the run abandoned', async () => {
		const seen: Error[] = []
		const h = new Harness(stateless, {
			uncaughtExceptionHandler: error => {
				seen.push(error)
				return 'shutdown'
			},
		})
		await h.started()
		h.consumer.deliver('in:0', [
			{ key: 'k', value: 'a' },
			{ key: 'k', value: 'boom' },
			{ key: 'k', value: 'c' },
		])
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(h.lifecycle.error).toBeInstanceOf(FailedProcessingError)
		expect(seen).toEqual([h.lifecycle.error])
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		expect(h.consumer.calls).toContain('stop')
		expect(h.output()).toEqual(['A'])

		await h.stop()
		expect(h.committed()).toBeUndefined()
		expect(h.lifecycle.state).toBe('STOPPED')
	})

	it('rejects start with a failure of the run before its tasks run, and stops dirty', async () => {
		const h = new Harness(stateless)
		const starting = h.start()
		await h.joined()
		h.consumer.fail(new Error('member fenced'))
		await expect(starting).rejects.toThrow('member fenced')
		await h.runtime.stop(h.io.clock.now() + 20_000)
		expect(h.consumer.calls).toContain('stop')
		expect(h.connection.disconnected).toBe(1)
	})

	it('rejects start when no assignment arrives within startTimeoutMs', async () => {
		const h = new Harness(stateless, { startTimeoutMs: 1000 })
		const starting = h.start()
		await h.joined()
		h.io.clock.advance(1000)
		await expect(starting).rejects.toThrow('did not join the group within startTimeoutMs')
	})

	it('a close during the start fails the start and releases everything once', async () => {
		const h = new Harness(stateless)
		const starting = h.start()
		await h.joined()
		const closing = h.stop()
		await expect(starting).rejects.toThrow(StreamsError)
		await closing
		expect(h.lifecycle.state).toBe('STOPPED')
		expect(h.connection.disconnected).toBe(1)
	})

	it('restores the changelog before the task runs; a resumed task with its checkpoint at the end needs no restore consumer', async () => {
		const h = new Harness(counting)
		const changelog = [...h.topology.internalTopics.values()].find(spec => spec.kind === 'changelog')!.name
		const stored = valueAndTimestampCodec(COUNT_CODEC)
		h.io.cluster.createTopic(changelog, 1)
		h.io.cluster.log(changelog, 0).append({
			key: Buffer.from('k'),
			value: stored.encode({ value: 5, timestamp: 0 }),
			timestamp: 0n,
			headers: {},
		})

		await h.started()
		expect(h.io.created.restoreConsumers.map(c => c.groupId)).toEqual([
			expect.stringMatching(/^app-restore-0_0-\d+$/),
		])
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([6]))

		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		expect(h.io.created.restoreConsumers).toHaveLength(1)
		await h.stop()
	})
})

describe('start positions and repartition purge', () => {
	it("commits the start position of a 'latest' source before its task runs; without a commit it stays pinned", async () => {
		const h = new Harness(app => app.stream('in', { ...strings, offsetReset: 'latest' }).to('out', strings))
		const starting = h.start()
		await h.joined()
		h.consumer.steps.commit.next(new Error('coordinator loading'))
		// The client resolved 'latest' to the end of the log.
		await h.consumer.rebalance({ topic: 'in', partition: 0, offset: 4n, committed: null })
		await starting
		expect(h.committed()).toBeUndefined()

		// Assigned again without a commit, now at a newer end: back to where the start began, before the task resumes.
		h.consumer.calls.length = 0
		await h.consumer.rebalance({ topic: 'in', partition: 0, offset: 9n, committed: null })
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		expect(h.consumer.calls.filter(call => !call.startsWith('pause'))).toEqual([
			'seek in:0 4',
			'commit in:0@4',
			'resume in:0',
		])
		expect(h.committed()).toBe(4n)
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['a']))
		await h.stop()
		expect(h.committed()).toBe(5n)
	})

	it('deletes the records of a repartition topic below its committed offset after commits, once per interval', async () => {
		const h = new Harness(
			app =>
				app
					.stream('in', strings)
					.groupBy((_key, value) => value, strings)
					.count()
					.toStream()
					.to('out'),
			{ commitIntervalMs: 1000, repartitionPurgeIntervalMs: 2000 }
		)
		const repartition = [...h.topology.internalTopics.values()].find(spec => spec.kind === 'repartition')!.name
		const starting = h.start()
		await h.joined()
		await h.consumer.rebalance('in:0', `${repartition}:0`)
		await starting
		/** Records the upstream task wrote, as the consumer fetches them. */
		const fetched = (...values: string[]) => {
			const records = values.map(value => ({ key: value, value }))
			h.io.cluster.append(repartition, 0, ...records)
			h.consumer.deliver(`${repartition}:0`, records)
		}
		const purges = () => h.io.admin.calls.filter(call => call.startsWith('deleteRecords'))

		fetched('x', 'y', 'x')
		await vi.waitFor(() => expect(h.output()).toHaveLength(3))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed(`${repartition}:0`)).toBe(3n))
		expect(purges()).toEqual([])

		fetched('y')
		await vi.waitFor(() => expect(h.output()).toHaveLength(4))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.io.cluster.log(repartition, 0).start).toBe(4n))
		expect(purges()).toEqual([`deleteRecords ${repartition}:0@4`])
		await h.stop()
	})
})
