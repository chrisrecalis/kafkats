/**
 * Chaos worker: a single flow app instance run as a child process.
 *
 * Not run directly; spawned by kill-chaos.ts / broker-chaos.ts / close-chaos.ts.
 * Manual run (from packages/flow):
 *   CHAOS_BROKERS=127.0.0.1:29092 CHAOS_APP_ID=x CHAOS_IN=in CHAOS_OUT=out \
 *   CHAOS_MODE=count CHAOS_GUARANTEE=exactly_once pnpm exec tsx tests/chaos/worker.ts
 *
 * Env: CHAOS_BROKERS, CHAOS_APP_ID, CHAOS_IN, CHAOS_OUT,
 *      CHAOS_MODE=stateless|count|regroup|table-join|global-join|window|join,
 *      CHAOS_TABLE (the table topic of the table join modes), CHAOS_RIGHT (the right stream of the join mode),
 *      CHAOS_GUARANTEE=at_least_once|exactly_once,
 *      CHAOS_BUFFERED_RECORDS (bufferedRecordsPerPartition, default the library's),
 *      CHAOS_SESSION_MS (default 6000), CHAOS_TXN_TIMEOUT_MS (default 10000),
 *      CHAOS_RESTORE_IDLE_MS (changelog restoration idleTimeoutMs, default 300 as in the repo's integration tests),
 *      CHAOS_RESTORE_DEFAULT=1 (use library default restoration options),
 *      CHAOS_NO_EXIT_AFTER_CLOSE=1 (stay alive after close() resolves),
 *      CHAOS_CLOSE_DURING_START_DELAY_MS=N (with CHAOS_CLOSE_DURING_START),
 *      CHAOS_CLOSE_DURING_START=1 (call close() without awaiting start()).
 * Behaviour: logs one JSON line per event on stdout (type=...). SIGTERM => graceful close().
 * Logs every unhandledRejection / uncaughtException (scenario E) but does NOT exit on them.
 */
import { codec, flow, JoinWindows, TimeWindows } from '../../src/index.js'

const env = (k: string, d?: string): string => {
	const v = process.env[k] ?? d
	if (v === undefined) throw new Error(`missing env ${k}`)
	return v
}
const t0 = Date.now()
const log = (type: string, extra: Record<string, unknown> = {}) =>
	process.stdout.write(JSON.stringify({ t: Date.now() - t0, pid: process.pid, type, ...extra }) + '\n')

// The harness reads stdout: once it is gone (EPIPE) nobody watches this worker, which stops like a crash (logging the
// write error as an uncaught exception would fail again, forever).
process.stdout.on('error', () => process.exit(1))
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
	processingGuarantee: guarantee,
	commitIntervalMs: 100,
	// KIP-695: a task of the join mode waits up to 2 s for an input partition that is caught up, so the right stream,
	// written in full before the input (common.ts produceRight), is joined in event-time order with the live input.
	...(mode === 'join' ? { maxTaskIdleMs: 2_000 } : {}),
	...(process.env.CHAOS_BUFFERED_RECORDS
		? { bufferedRecordsPerPartition: Number(process.env.CHAOS_BUFFERED_RECORDS) }
		: {}),
	consumer: {
		autoOffsetReset: 'earliest',
		maxWaitMs: 100,
		sessionTimeoutMs: Number(env('CHAOS_SESSION_MS', '6000')),
		rebalanceTimeoutMs: 20000,
	},
	producer: { lingerMs: 0, retries: 10, transactionTimeoutMs: Number(env('CHAOS_TXN_TIMEOUT_MS', '10000')) },
	changelog: {
		replicationFactor: Number(env('CHAOS_RF', '1')),
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

const input = () =>
	app.stream(env('CHAOS_IN'), { key: str, value: str }).peek(() => {
		processed++
	})
if (mode === 'stateless') {
	input()
		.mapValues(v => `m:${v}`)
		.to(env('CHAOS_OUT'), { key: str, value: str })
} else if (mode === 'regroup') {
	// groupBy(newKey) => internal repartition topic in the middle; new key = 'n'+key (different partitioning)
	input()
		.groupBy(k => `n${k}`, { key: str, value: str })
		.count({ storeName: 'counts', key: str, value: num })
		.toStream()
		.to(env('CHAOS_OUT'), { key: str, value: num })
} else if (mode === 'count') {
	input()
		.groupByKey()
		.count({ storeName: 'counts', key: str, value: num })
		.toStream()
		.to(env('CHAOS_OUT'), { key: str, value: num })
} else if (mode === 'table-join') {
	// stream-table inner join, both topics co-partitioned; the table rows are versions
	const versions = app.table(env('CHAOS_TABLE'), { key: str, value: str })
	input()
		.join(versions, (v, version) => `m:${v}|${version}`)
		.to(env('CHAOS_OUT'), { key: str, value: str })
} else if (mode === 'global-join') {
	const versions = app.globalTable(env('CHAOS_TABLE'), { key: str, value: str })
	input()
		.join(
			versions,
			k => k,
			(v, version) => `m:${v}|${version}`
		)
		.to(env('CHAOS_OUT'), { key: str, value: str })
} else if (mode === 'window') {
	// counts per key and 1 s window; no grace, but every partition is in event-time order (common.ts genInput)
	input()
		.groupByKey()
		.windowedBy(TimeWindows.ofSizeWithNoGrace(1_000))
		.count({ storeName: 'counts', value: num })
		.toStream()
		.selectKey(key => `${key!.key}@${key!.window.start}`)
		.to(env('CHAOS_OUT'), { key: str, value: num })
} else if (mode === 'join') {
	// left join within 1 s, no grace: matched at once, unmatched joined with null once its window closed
	const right = app.stream(env('CHAOS_RIGHT'), { key: str, value: str })
	input()
		.leftJoin(right, (v, r) => `m:${v}|${r ?? 'null'}`, JoinWindows.ofTimeDifferenceWithNoGrace(1_000))
		.to(env('CHAOS_OUT'), { key: str, value: str })
} else {
	throw new Error(`unknown CHAOS_MODE ${mode}`)
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
