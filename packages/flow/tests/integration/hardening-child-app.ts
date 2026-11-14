/** Child process app for hardening tests (spawned, not a test). Env: R3_BROKERS, R3_APP, R3_IN, R3_OUT, R3_SESSION_MS */
import { codec, flow } from '../../src/index.js'

const str = codec.string()
const app = flow({
	applicationId: process.env.R3_APP!,
	client: { clientId: `r3-child-${process.pid}`, brokers: process.env.R3_BROKERS!.split(',') },
	processingGuarantee: 'at_least_once',
	consumer: {
		autoOffsetReset: 'earliest',
		maxWaitMs: 100,
		sessionTimeoutMs: Number(process.env.R3_SESSION_MS ?? 30_000),
	},
	producer: { lingerMs: 0 },
})
app.stream(process.env.R3_IN!, { key: str, value: str })
	.mapValues(v => (v === null ? null : `m:${v}`))
	.to(process.env.R3_OUT!, { key: str, value: str })
app.start().then(
	() => process.stdout.write('started\n'),
	e => process.stdout.write(`start_failed ${(e as Error).message}\n`)
)
setInterval(() => {}, 1 << 30)
