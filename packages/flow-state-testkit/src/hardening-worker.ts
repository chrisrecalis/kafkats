/**
 * Child worker for the crash tests (spawned, never run directly by vitest).
 * Env: R3_BROKERS, R3_APP, R3_IN, R3_OUT, R3_STATE, R3_MODE=count|window, R3_GUARANTEE, R3_RESTORE_DELAY_MS (slow restore),
 *      R3_PROVIDER_MODULE + R3_PROVIDER_FACTORY (the provider under test, see EngineUnderTest.module)
 */
import { codec, flow, SessionWindows, TimeWindows } from '@kafkats/flow'
import type { StateStoreProvider } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

const env = (k: string, d?: string): string => {
	const v = process.env[k] ?? d
	if (v === undefined) throw new Error(`missing env ${k}`)
	return v
}
const provider = (await import(env('R3_PROVIDER_MODULE'))) as Record<string, unknown>
const createProvider = provider[env('R3_PROVIDER_FACTORY')] as (options: { stateDir: string }) => StateStoreProvider
const str = codec.string()
const num = codec.json<number>()
const log = (type: string, extra: Record<string, unknown> = {}) =>
	process.stdout.write(JSON.stringify({ type, ...extra }) + '\n')
process.on('unhandledRejection', r => log('UNHANDLED_REJECTION', { message: (r as Error)?.message ?? String(r) }))

const t0 = Date.now()
const client = new KafkaClient({ clientId: `r3-${process.pid}`, brokers: env('R3_BROKERS').split(',') })
if (process.env.R3_TRACE) {
	// Trace every producer transaction (no src changes): begin / sendOffsets / outcome.
	const original = client.producer.bind(client)
	let n = 0
	;(client as any).producer = (config: any) => {
		const producer = original(config)
		const tx0 = producer.transaction.bind(producer)
		;(producer as any).transaction = async (fn: any, ...rest: any[]) => {
			const id = ++n
			log('tx_begin', { id, t: Date.now() - t0 })
			try {
				const r = await tx0(
					async (tx: any) => {
						const so = tx.sendOffsets.bind(tx)
						tx.sendOffsets = async (...a: any[]) => {
							log('tx_sendOffsets', {
								id,
								t: Date.now() - t0,
								offsets: JSON.stringify(a[1], (_k, v) => (typeof v === 'bigint' ? String(v) : v)),
							})
							return so(...a)
						}
						const send = tx.send.bind(tx)
						tx.send = async (topic: string, ...a: any[]) => {
							const res = await send(topic, ...a)
							log('tx_send', {
								id,
								topic,
								res: JSON.stringify(res, (_k, v) => (typeof v === 'bigint' ? String(v) : v)),
							})
							return res
						}
						return fn(tx)
					},
					...rest
				)
				log('tx_committed', { id, t: Date.now() - t0 })
				return r
			} catch (e) {
				log('tx_failed', { id, t: Date.now() - t0, message: (e as Error).message })
				throw e
			}
		}
		return producer
	}
}
const app = flow({
	applicationId: env('R3_APP'),
	client,
	processingGuarantee: env('R3_GUARANTEE') as 'at_least_once' | 'exactly_once',
	commitIntervalMs: 100,
	consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100, sessionTimeoutMs: 6000 },
	producer: { lingerMs: 0, transactionTimeoutMs: 10000 },
	stateStoreProvider: createProvider({ stateDir: env('R3_STATE') }),
	changelog: {
		restoration: { idleTimeoutMs: 1000, initialIdleTimeoutMs: 5000, checkIntervalMs: 50, consumerMaxWaitMs: 50 },
	},
})
const grouped = app.stream(env('R3_IN'), { key: str, value: str }).groupByKey()
if (env('R3_MODE') === 'count') {
	grouped.count({ storeName: 'counts', key: str, value: num }).toStream().to(env('R3_OUT'), { key: str, value: num })
} else if (env('R3_MODE') === 'session') {
	grouped
		.windowedBy(SessionWindows.withInactivityGap(1_000))
		.count({ storeName: 'scounts', value: num })
		.toStream()
		.selectKey(k => `${k!.key}@${k!.window.start}-${k!.window.end}`)
		.to(env('R3_OUT'), { key: str, value: num })
} else {
	grouped
		.windowedBy(TimeWindows.of(1_000))
		.count({ storeName: 'wcounts', value: num })
		.toStream()
		.selectKey(k => `${k!.key}@${k!.window.start}`)
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
	e => {
		log('start_failed', { message: (e as Error).message })
		process.exit(2) // let the harness respawn (start() fails when the group join takes > 10s, see hardening-restart-start test)
	}
)
setInterval(() => {}, 1 << 30)
