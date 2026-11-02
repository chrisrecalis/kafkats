import { EventEmitter } from 'node:events'
import { noopLogger, type Logger } from '@kafkats/client'
import { LagCollector } from './collector.js'
import { emptyStats, type LagMonitorConfig, type LagMonitorEvents, type LagSnapshot } from './types.js'

const DEFAULT_INTERVAL_MS = 30_000

/**
 * Periodically measures consumer group lag and emits each result as a `snapshot` event.
 *
 * ```ts
 * const monitor = new LagMonitor({ client, groups: /^orders-/ })
 * monitor.on('snapshot', snapshot => metrics.update(snapshot, monitor.lastSuccessAt))
 * await monitor.start()
 * ```
 */
export class LagMonitor extends EventEmitter<LagMonitorEvents> {
	private readonly collector: LagCollector
	private readonly intervalMs: number
	private readonly client: LagMonitorConfig['client']
	private readonly logger: Logger
	private timer: NodeJS.Timeout | null = null
	private running = false
	private inFlight: Promise<LagSnapshot> | null = null
	private latestSnapshot: LagSnapshot | null = null
	private lastSuccessAtMs: number | null = null

	constructor(config: LagMonitorConfig) {
		super()
		this.client = config.client
		this.logger = config.logger ?? noopLogger
		this.intervalMs = config.intervalMs ?? DEFAULT_INTERVAL_MS
		this.collector = new LagCollector(config)
	}

	/** Most recent snapshot (possibly with errors), or `null` before the first collection completes */
	get latest(): LagSnapshot | null {
		return this.latestSnapshot
	}

	/** Epoch ms when the most recent error-free collection finished, or `null` if none has */
	get lastSuccessAt(): number | null {
		return this.lastSuccessAtMs
	}

	/**
	 * Connect the client if needed, run one collection, then keep collecting every `intervalMs`.
	 * Resolves after the first collection so metrics are populated when this returns.
	 */
	async start(): Promise<void> {
		if (this.running) return
		if (!this.client.isConnected) {
			await this.client.connect()
		}
		this.running = true
		await this.collect()
		this.schedule()
	}

	/** Stop the periodic loop. Waits for an in-flight collection. Does not disconnect the client. */
	async stop(): Promise<void> {
		this.running = false
		if (this.timer) {
			clearTimeout(this.timer)
			this.timer = null
		}
		await this.inFlight
	}

	/**
	 * Run one collection now. Concurrent calls share the same in-flight collection. Never rejects: a
	 * collector failure becomes a snapshot with a single `collect` error, and a throwing `snapshot`
	 * listener is logged rather than propagated.
	 */
	collect(): Promise<LagSnapshot> {
		if (this.inFlight) return this.inFlight
		const startedAt = Date.now()
		this.inFlight = this.collector
			.collect()
			.catch((error: unknown): LagSnapshot => ({
				collectedAt: startedAt,
				durationMs: Date.now() - startedAt,
				groups: [],
				errors: [{ scope: 'collect', message: error instanceof Error ? error.message : String(error) }],
				stats: emptyStats(),
			}))
			.then(snapshot => {
				this.latestSnapshot = snapshot
				if (snapshot.errors.length === 0) this.lastSuccessAtMs = snapshot.collectedAt + snapshot.durationMs
				try {
					this.emit('snapshot', snapshot)
				} catch (error) {
					this.logger.error('lag: snapshot listener threw', {
						error: error instanceof Error ? error.message : String(error),
					})
				}
				return snapshot
			})
			.finally(() => {
				this.inFlight = null
			})
		return this.inFlight
	}

	private schedule(): void {
		if (!this.running) return
		this.timer = setTimeout(() => {
			this.timer = null
			void this.collect().finally(() => this.schedule())
		}, this.intervalMs)
		this.timer.unref()
	}
}
