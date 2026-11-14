/**
 * Child worker for round3 LMDB crash tests (spawned, never run directly by vitest).
 * Env: R3_BROKERS, R3_APP, R3_IN, R3_OUT, R3_STATE, R3_MODE=count|window, R3_GUARANTEE, R3_RESTORE_DELAY_MS (slow restore)
 */
import { codec, flow, TimeWindows } from '@kafkats/flow'
import { lmdb } from '../src/lmdb.js'

const env = (k: string, d?: string): string => {
	const v = process.env[k] ?? d
	if (v === undefined) throw new Error(`missing env ${k}`)
	return v
}
const str = codec.string()
const num = codec.json<number>()
const log = (type: string, extra: Record<string, unknown> = {}) =>
	process.stdout.write(JSON.stringify({ type, ...extra }) + '\n')
process.on('unhandledRejection', r => log('UNHANDLED_REJECTION', { message: (r as Error)?.message ?? String(r) }))

const app = flow({
	applicationId: env('R3_APP'),
	client: { clientId: `r3-${process.pid}`, brokers: env('R3_BROKERS').split(',') },
	processingGuarantee: env('R3_GUARANTEE') as 'at_least_once' | 'exactly_once',
	commitIntervalMs: 100,
	consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100, sessionTimeoutMs: 6000 },
	producer: { lingerMs: 0, transactionTimeoutMs: 10000 },
	stateStoreProvider: lmdb({ stateDir: env('R3_STATE') }),
	changelog: {
		restoration: { idleTimeoutMs: 1000, initialIdleTimeoutMs: 5000, checkIntervalMs: 50, consumerMaxWaitMs: 50 },
	},
})
const grouped = app.stream(env('R3_IN'), { key: str, value: str }).groupByKey()
if (env('R3_MODE') === 'count') {
	grouped.count({ storeName: 'counts', key: str, value: num }).toStream().to(env('R3_OUT'), { key: str, value: num })
} else {
	grouped
		.windowedBy(TimeWindows.of(1_000))
		.count({ storeName: 'wcounts', value: num })
		.toStream()
		.selectKey((_v, k) => {
			const w = k as unknown as { key: string; window: { start: number } }
			return `${w.key}@${w.window.start}`
		})
		.to(env('R3_OUT'), { key: str, value: num })
}
process.on('SIGTERM', () => {
	app.close().then(
		() => process.exit(0),
		() => process.exit(3)
	)
})
app.start().then(
	() => log('started'),
	e => log('start_failed', { message: (e as Error).message })
)
setInterval(() => {}, 1 << 30)
