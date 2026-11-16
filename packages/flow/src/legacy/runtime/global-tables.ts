import type { Consumer, KafkaClient, Logger } from '@kafkats/client'
import { SourceTopicNotFoundError } from '@/legacy/changelog.js'
import { clearStore, latestPerKey, writeChanges } from '@/state/bulk.js'
import type { FlowConfig } from '@/legacy/types.js'
import {
	RESTART_INITIAL_BACKOFF_MS,
	RESTART_MAX_BACKOFF_MS,
	isInfrastructureRecoverable,
	isRecoverable,
	toError,
} from '@/legacy/runtime/errors.js'
import type { AppStatus } from '@/legacy/runtime/status.js'
import type { StoreRegistry } from '@/legacy/runtime/store-registry.js'
import type { GlobalTableDef, Topology } from '@/legacy/runtime/topology.js'

export type GlobalTablesDeps = {
	client: KafkaClient
	config: FlowConfig
	/** Identity of this instance: every instance reads global tables with its own consumer group. */
	processId: string
	topology: Topology
	stores: StoreRegistry
	status: AppStatus
	logger: Logger
	/**
	 * A table can no longer be kept up to date. Stream processing must not continue: joins would
	 * keep answering from a table that no longer advances.
	 */
	onFatal: (error: Error) => void
}

/** The background consumer that keeps one global table up to date. */
type Runner = { consumer: Consumer | null; run: Promise<void> | null }

/** Loads every global table before processing starts and follows its topic afterwards. */
export class GlobalTables {
	private readonly runners = new Map<GlobalTableDef, Runner>()

	constructor(private readonly deps: GlobalTablesDeps) {}

	/** Load all global tables; resolves once each has caught up to the end of its topic. */
	async start(): Promise<void> {
		await Promise.all(this.deps.topology.globalTables.map(table => this.startTable(table)))
	}

	stopConsumers(): void {
		for (const runner of this.runners.values()) {
			try {
				runner.consumer?.stop()
			} catch {
				// not running
			}
		}
	}

	/** Settles once every table consumer has stopped. */
	async settled(): Promise<void> {
		await Promise.allSettled([...this.runners.values()].map(runner => runner.run ?? Promise.resolve()))
	}

	clear(): void {
		this.runners.clear()
	}

	/**
	 * Load a global table completely, then keep it up to date in the background.
	 * Resolves once the table has caught up to the end of its topic as of startup.
	 */
	private async startTable(table: GlobalTableDef): Promise<void> {
		const { client, config, status, logger } = this.deps
		const runner: Runner = { consumer: null, run: null }
		this.runners.set(table, runner)

		const store = this.deps.stores.openGlobalStore(table)
		await store.init()
		// The topic is the source of truth: rebuild from scratch so removed rows don't survive.
		await clearStore(store)

		const metadata = await client.getMetadata([table.topic])
		const partitions = [...(metadata.topics.get(table.topic)?.partitions.keys() ?? [])]
		if (partitions.length === 0) {
			throw new SourceTopicNotFoundError(table.topic, table.storeName)
		}

		const admin = client.admin()
		const [startOffsets, endOffsets] = await Promise.all([
			admin.fetchTopicOffsets(table.topic, partitions, 'earliest', { isolationLevel: 'read_committed' }),
			admin.fetchTopicOffsets(table.topic, partitions, 'latest', { isolationLevel: 'read_committed' }),
		])

		type Progress = { end: bigint; position: bigint; fetched: number; handled: number }
		const progress = new Map<number, Progress>()
		const pending = new Set<number>()
		for (const partition of partitions) {
			const start = startOffsets.get(partition) ?? 0n
			const end = endOffsets.get(partition) ?? 0n
			progress.set(partition, { end, position: start, fetched: 0, handled: 0 })
			if (start < end) pending.add(partition)
		}

		let resolveBootstrap!: () => void
		let rejectBootstrap!: (err: Error) => void
		const bootstrapped = new Promise<void>((resolve, reject) => {
			resolveBootstrap = resolve
			rejectBootstrap = reject
		})
		bootstrapped.catch(() => {})
		const check = (partition: number) => {
			const state = progress.get(partition)
			if (!state || !pending.has(partition)) return
			if (state.position >= state.end && state.handled >= state.fetched) {
				pending.delete(partition)
				if (pending.size === 0) resolveBootstrap()
			}
		}
		if (pending.size === 0) resolveBootstrap()

		const positions = new Map<number, bigint>()
		for (const partition of partitions) positions.set(partition, startOffsets.get(partition) ?? 0n)

		const run = async () => {
			let attempt = 0
			/** What applying rows threw, as opposed to a failure of the consumer itself. */
			let rowFailure: Error | undefined
			while (!status.closing) {
				const consumer = client.consumer({
					groupId: `${config.applicationId}-global-${table.storeName}-${this.deps.processId}`,
					autoOffsetReset: 'earliest',
					isolationLevel: 'read_committed',
				})
				runner.consumer = consumer
				consumer.on('error', error => {
					status.lastError = error
				})
				consumer.on('fetchPosition', (position, recordCount) => {
					if (position.topic !== table.topic) return
					const state = progress.get(position.partition)
					if (!state) return
					state.fetched += recordCount
					if (position.offset > state.position) state.position = position.offset
					check(position.partition)
				})
				try {
					await consumer.runBatch(
						[table.topic],
						// A batch holds records of one partition, in offset order.
						async messages => {
							const { partition } = messages[0]!
							try {
								await writeChanges(store, table.keyCodec, table.valueCodec, latestPerKey(messages))
								positions.set(partition, messages.at(-1)!.offset + 1n)
							} catch (err) {
								rowFailure = toError(err)
								throw rowFailure
							}
							// Only rows that were applied count as loaded: a batch that failed must
							// not complete the bootstrap.
							const state = progress.get(partition)
							if (state) state.handled += messages.length
							check(partition)
						},
						{
							commitOffsets: false,
							assignment: partitions.map(partition => ({
								topic: table.topic,
								partition,
								offset: positions.get(partition) ?? 0n,
							})),
						}
					)
					if (status.closing) return
				} catch (err) {
					if (status.closing) return
					const error = toError(err)
					status.lastError = error
					if (pending.size > 0) {
						// Still bootstrapping: fail startup rather than serve a partial table.
						rejectBootstrap(error)
						return
					}
					// Applying rows (codecs, the store) is like user code in a stream worker: it would
					// fail on the same rows again, and retrying forever would serve a stale table.
					const recoverable = error === rowFailure ? isRecoverable(error) : isInfrastructureRecoverable(error)
					if (!recoverable) {
						logger.error('global table consumer failed', { topic: table.topic, error: error.message })
						this.deps.onFatal(error)
						return
					}
					attempt += 1
					logger.warn('global table consumer failed; restarting', {
						topic: table.topic,
						attempt,
						error: error.message,
					})
				}
				// Resume from the last applied positions; fetch accounting restarts with them.
				for (const state of progress.values()) {
					state.fetched = 0
					state.handled = 0
				}
				await status.sleepUnlessClosing(
					Math.min(RESTART_MAX_BACKOFF_MS, RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(attempt, 10))
				)
			}
		}

		runner.run = run()

		const idleTimeoutMs = config.changelog?.restoration?.idleTimeoutMs ?? 30_000
		let lastSnapshot = ''
		let lastChange = Date.now()
		const watchdog = setInterval(() => {
			const snapshot = [...progress.values()].map(s => `${s.position}:${s.handled}`).join(',')
			if (snapshot !== lastSnapshot) {
				lastSnapshot = snapshot
				lastChange = Date.now()
			} else if (Date.now() - lastChange > idleTimeoutMs) {
				rejectBootstrap(
					new Error(`Timed out loading global table "${table.topic}": no progress for ${idleTimeoutMs}ms`)
				)
			}
		}, 1000)
		const onClose = () => rejectBootstrap(new Error('Flow was closed while starting'))
		const signal = status.shutdownSignal
		signal.addEventListener('abort', onClose, { once: true })
		try {
			await bootstrapped
		} finally {
			clearInterval(watchdog)
			signal.removeEventListener('abort', onClose)
		}
	}
}
