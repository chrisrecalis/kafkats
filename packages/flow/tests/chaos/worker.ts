/**
 * Chaos worker: a single flow app instance run as a child process.
 *
 * Not run directly; spawned by kill-chaos.ts / broker-chaos.ts / close-chaos.ts.
 * Manual run (from packages/flow):
 *   CHAOS_BROKERS=127.0.0.1:29092 CHAOS_APP_ID=x CHAOS_IN=in CHAOS_OUT=out \
 *   CHAOS_MODE=count CHAOS_GUARANTEE=exactly_once pnpm exec tsx tests/chaos/worker.ts
 *
NOTE: CHAOS_TRACE_RESTORE monkeypatch targets the OLD ChangelogRestorer.restore signature and is broken on new code; do not set it.
 * Env: CHAOS_BROKERS, CHAOS_APP_ID, CHAOS_IN, CHAOS_OUT, CHAOS_MODE=stateless|count|regroup,
 *      CHAOS_GUARANTEE=at_least_once|exactly_once, CHAOS_THREADS (default 1),
 *      CHAOS_SESSION_MS (default 6000), CHAOS_TXN_TIMEOUT_MS (default 10000),
 *      CHAOS_RESTORE_IDLE_MS (changelog restoration idleTimeoutMs, default 300 as in the repo's integration tests),
 *      CHAOS_RESTORE_DEFAULT=1 (use library default restoration options),
 *      CHAOS_NO_EXIT_AFTER_CLOSE=1 (stay alive after close() resolves),
 *      CHAOS_CLOSE_DURING_START_DELAY_MS=N (with CHAOS_CLOSE_DURING_START),
 *      CHAOS_CLOSE_DURING_START=1 (call close() without awaiting start()).
 * Behaviour: logs one JSON line per event on stdout (type=...). SIGTERM => graceful close().
 * Logs every unhandledRejection / uncaughtException (scenario E) but does NOT exit on them.
 */
import { codec, flow } from '../../src/index.js'
import { ChangelogRestorer } from '../../src/changelog.js'

const env = (k: string, d?: string): string => {
	const v = process.env[k] ?? d
	if (v === undefined) throw new Error(`missing env ${k}`)
	return v
}
const t0 = Date.now()
const log = (type: string, extra: Record<string, unknown> = {}) =>
	process.stdout.write(JSON.stringify({ t: Date.now() - t0, pid: process.pid, type, ...extra }) + '\n')

process.on('unhandledRejection', (reason: unknown) => {
	const r = reason as Error
	log('UNHANDLED_REJECTION', {
		message: r?.message ?? String(reason),
		stack: r?.stack?.split('\n').slice(0, 6).join(' | '),
	})
})
process.on('uncaughtException', (err: Error) => {
	log('UNCAUGHT_EXCEPTION', { message: err.message, stack: err.stack?.split('\n').slice(0, 6).join(' | ') })
})

// CHAOS_TRACE_RESTORE=1: wrap ChangelogRestorer.restore (runtime monkeypatch, src untouched) to log what each restoration did.
if (process.env.CHAOS_TRACE_RESTORE) {
	const orig = (ChangelogRestorer.prototype as any).restore
	;(ChangelogRestorer.prototype as any).restore = async function (
		this: any,
		client: any,
		opts: any,
		partitions: any,
		cp: any
	) {
		const st = Date.now()
		const admin = client.admin()
		const lso = await admin.fetchTopicOffsets(this.topicName, partitions, 'latest', {
			isolationLevel: 'read_committed',
		})
		const hw = await admin.fetchTopicOffsets(this.topicName, partitions, 'latest', {
			isolationLevel: 'read_uncommitted',
		})
		log('restore_begin', {
			topic: this.topicName,
			partitions,
			lso: [...lso].map(([p, o]) => `${p}:${o}`),
			hw: [...hw].map(([p, o]) => `${p}:${o}`),
		})
		try {
			const n = await orig.call(this, client, opts, partitions, cp)
			const vals: unknown[] = []
			for (let i = 0; i < 12; i++) vals.push(await this.store.get(`k${i}`).catch(() => '?'))
			log('restore_end', { restored: n, ms: Date.now() - st, restoredValues: vals.join(',') })
			return n
		} catch (e) {
			log('restore_error', { message: (e as Error).message, ms: Date.now() - st })
			throw e
		}
	}
}

const guarantee = env('CHAOS_GUARANTEE', 'at_least_once') as 'at_least_once' | 'exactly_once'
const mode = env('CHAOS_MODE', 'stateless')
const str = codec.string()
const num = codec.json<number>()
let processed = 0

const app = flow({
	applicationId: env('CHAOS_APP_ID'),
	client: {
		clientId: `chaos-${process.pid}`,
		brokers: env('CHAOS_BROKERS').split(','),
		...(process.env.CHAOS_LOG_LEVEL ? { logLevel: process.env.CHAOS_LOG_LEVEL as any } : {}),
	},
	numStreamThreads: Number(env('CHAOS_THREADS', '1')),
	processingGuarantee: guarantee,
	commitIntervalMs: 100,
	consumer: {
		autoOffsetReset: 'earliest',
		maxWaitMs: 100,
		sessionTimeoutMs: Number(env('CHAOS_SESSION_MS', '6000')),
		rebalanceTimeoutMs: 20000,
	},
	producer: { lingerMs: 0, retries: 10, transactionTimeoutMs: Number(env('CHAOS_TXN_TIMEOUT_MS', '10000')) },
	changelog: {
		restoration: process.env.CHAOS_RESTORE_DEFAULT
			? undefined
			: {
					idleTimeoutMs: Number(env('CHAOS_RESTORE_IDLE_MS', '300')),
					initialIdleTimeoutMs: 5000,
					checkIntervalMs: 50,
					consumerMaxWaitMs: 50,
				},
	},
})

if (mode === 'stateless') {
	app.stream(env('CHAOS_IN'), { key: str, value: str })
		.mapValues(v => {
			processed++
			return v === null ? null : `m:${v}`
		})
		.to(env('CHAOS_OUT'), { key: str, value: str })
} else if (mode === 'regroup') {
	// groupBy(newKey) => internal repartition topic in the middle; new key = 'n'+key (different partitioning)
	app.stream(env('CHAOS_IN'), { key: str, value: str })
		.peek(() => {
			processed++
		})
		.groupBy((k, _v) => `n${k}`, { key: str, value: str })
		.count({ storeName: 'counts', key: str, value: num })
		.toStream()
		.to(env('CHAOS_OUT'), { key: str, value: num })
} else {
	app.stream(env('CHAOS_IN'), { key: str, value: str })
		.peek(() => {
			processed++
		})
		.groupByKey()
		.count({ storeName: 'counts', key: str, value: num })
		.toStream()
		.to(env('CHAOS_OUT'), { key: str, value: num })
}

let closing = false
const shutdown = async (sig: string) => {
	if (closing) return
	closing = true
	const s = Date.now()
	log('close_begin', { sig, processed, state: app.state() })
	try {
		await app.close()
		log('close_done', { ms: Date.now() - s, state: app.state() })
		if (process.env.CHAOS_NO_EXIT_AFTER_CLOSE) return // keep process alive so the harness can look for leaked consumers
		process.exit(0)
	} catch (e) {
		log('close_error', { ms: Date.now() - s, message: (e as Error).message })
		process.exit(3)
	}
}
process.on('SIGTERM', () => void shutdown('SIGTERM'))

setInterval(() => {
	const err = app.getError()
	log('status', { state: app.state(), processed, error: err ? err.message : null })
}, 1000).unref()

const s = Date.now()
log('start_begin')
const startP = app.start().then(
	() => log('started', { ms: Date.now() - s, state: app.state() }),
	e =>
		log('start_failed', {
			ms: Date.now() - s,
			message: (e as Error).message,
			stack: (e as Error).stack?.split('\n').slice(0, 5).join(' | '),
		})
)
if (process.env.CHAOS_CLOSE_DURING_START) {
	setTimeout(() => void shutdown('close-during-start'), Number(process.env.CHAOS_CLOSE_DURING_START_DELAY_MS ?? 0))
}
void startP
// keep alive
setInterval(() => {}, 1 << 30)
