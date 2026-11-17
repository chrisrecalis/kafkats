/**
 * A FlowRuntime over fake IO, started and closed through an AppLifecycle like the app's (runtime, run and handler
 * tests). The topology is built by `flow()` from the test's DSL calls; `in` and `out` exist with one partition unless
 * `partitions` says otherwise.
 */
import { expect, vi } from 'vitest'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { AppLifecycle } from '@/app/lifecycle.js'
import { FlowRuntime } from '@/runtime/runtime.js'
import type { StateStoreProvider } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { FakeIO, type FakeGroupConsumer, type FakeProducer } from './fake-io.js'
import { RecordingLogger } from './task-harness.js'

export const APP = 'app'

export interface RuntimeHarnessOptions {
	/** Partition counts of the topics created before the start (default: `in` and `out` with one each). */
	readonly partitions?: Record<string, number>
	readonly provider?: StateStoreProvider
}

export class RuntimeHarness {
	readonly io = new FakeIO({ groupId: APP })
	readonly lifecycle = new AppLifecycle()
	readonly logger = new RecordingLogger()
	readonly checkpoints = new Checkpoints(undefined, () => {})
	readonly stateDirectory: StateDirectory
	readonly topology: BuiltTopology
	readonly runtime: FlowRuntime
	readonly connection = { connected: 0, disconnected: 0 }

	constructor(build: (app: FlowApp) => void, config: Partial<FlowConfig> = {}, options: RuntimeHarnessOptions = {}) {
		const app = flow({ applicationId: APP, client: { clientId: APP, brokers: ['localhost:9092'] }, ...config })
		build(app)
		this.topology = internals(app).build()
		for (const [topic, count] of Object.entries(options.partitions ?? { in: 1, out: 1 })) {
			this.io.cluster.createTopic(topic, count)
		}
		const { connection } = this
		this.stateDirectory = new StateDirectory(APP, options.provider ?? inMemory(), this.checkpoints, this.logger)
		this.runtime = new FlowRuntime({
			topology: this.topology,
			config: { ...internals(app).settings, processId: 'process' },
			io: this.io,
			connection: {
				connect: async () => void connection.connected++,
				disconnect: async () => void connection.disconnected++,
			},
			lifecycle: this.lifecycle,
			stateDirectory: this.stateDirectory,
			checkpoints: this.checkpoints,
			topics: {},
			logger: this.logger,
		})
	}

	/** The group consumer of the current run (the latest one created). */
	get consumer(): FakeGroupConsumer {
		return this.io.created.consumers.at(-1)!
	}

	/** The producer of the current output (the latest one created). */
	get producer(): FakeProducer {
		return this.io.created.producers.at(-1)!
	}

	/** As the app's start(): a failed start releases what it started (the stop close() shares, CR14). */
	start(): Promise<void> {
		return this.lifecycle.start(async () => {
			try {
				await this.runtime.start(this.runtime.shutdown.signal)
			} catch (error) {
				await this.runtime.stop(this.io.clock.now() + 20_000)
				throw error
			}
		})
	}

	/** Starts, waits for the group member, assigns `assignment` (default `in:0`) and waits until its tasks run. */
	async started(...assignment: string[]): Promise<void> {
		const starting = this.start()
		await this.joined()
		await this.consumer.rebalance(...(assignment.length > 0 ? assignment : ['in:0']))
		await starting
	}

	/** Waits for the `count`th group consumer (the member of the `count`th run). */
	async joined(count = 1): Promise<void> {
		await vi.waitFor(() => expect(this.io.created.consumers).toHaveLength(count))
	}

	stop(timeoutMs = 20_000): Promise<void> {
		return this.lifecycle.close(() => this.runtime.stop(this.io.clock.now() + timeoutMs))
	}

	committed(key = 'in:0'): bigint | undefined {
		return this.io.cluster.offsets(APP).get(key)?.offset
	}

	/** The committed values of `topic` partition 0 (default `out`). */
	output<T = string>(decode: (value: Buffer) => T = value => value.toString() as T, topic = 'out'): T[] {
		const { messages } = this.io.cluster.log(topic, 0).read(0n, 'read_committed')
		return messages.map(m => decode(m.value))
	}
}
