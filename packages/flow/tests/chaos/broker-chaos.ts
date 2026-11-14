/**
 * Scenario C: broker restart / pause chaos (and E: unhandled rejections are logged by worker).
 *
 * Prereq: docker container `kafka-chaos` on 127.0.0.1:29092 (start-kafka.sh kafka-chaos 29092). Uses docker restart/pause on it ONLY.
 * Run (from packages/flow):
 *   pnpm exec tsx tests/chaos/broker-chaos.ts --guarantee eos --mode stateless --fault restart
 *   pnpm exec tsx tests/chaos/broker-chaos.ts --guarantee alo --mode count --fault pause --pause-s 25
 * Flags: --guarantee alo|eos --mode stateless|count --fault restart|pause --pause-s 25 --keys 12 --per-key 100
 *        --recover-ms 120000 (how long to wait for self-recovery before declaring HANG)
 * Timeline: produce; once worker is processing inject fault while producer keeps going; then watch the worker:
 *   state, processed counter progress, errors. Reports precise phases: stall start, broker back, resume (or never).
 * Exit 0 PASS / 1 FAIL.
 */
import { execSync } from 'node:child_process'
import {
	Worker,
	workerEnv,
	makeClient,
	createTopics,
	genInput,
	readAll,
	uid,
	sleep,
	CONTAINER,
	summarizeStateless,
	summarizeCount,
	type Rec,
} from './common.js'

const arg = (n: string, d: string) => {
	const i = process.argv.indexOf(`--${n}`)
	return i >= 0 ? process.argv[i + 1]! : d
}
const guarantee = arg('guarantee', 'alo') === 'eos' ? 'exactly_once' : 'at_least_once'
const mode = arg('mode', 'stateless') as 'stateless' | 'count'
const fault = arg('fault', 'restart')
const pauseS = Number(arg('pause-s', '25'))
const keys = Number(arg('keys', '12'))
const perKey = Number(arg('per-key', '100'))
const recoverMs = Number(arg('recover-ms', '120000'))
const isolation = guarantee === 'exactly_once' ? 'read_committed' : 'read_uncommitted'
const label = `${guarantee}/${mode}/${fault}${fault === 'pause' ? pauseS + 's' : ''}`
const sh = (c: string) => execSync(c, { stdio: 'pipe' }).toString().trim()
const T0 = Date.now()
const ev = (m: string) => console.log(`  [+${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`)

let client = await makeClient()
const input = uid('chaos-in')
const output = uid('chaos-out')
const appId = uid('chaos-app')
await createTopics(client, [input, output], 4)
const inputs = genInput(keys, perKey)
console.log(`== ${label}: ${inputs.length} records`)

// producer that survives broker outages: retries each batch on a fresh producer until it succeeds
let sentCount = 0
let producerDone = false
const sendLoop = (async () => {
	const batch = 12
	for (let i = 0; i < inputs.length; i += batch) {
		const slice: Rec[] = inputs.slice(i, i + batch)
		for (;;) {
			try {
				const c = (globalThis as any).__pc ?? client
				const p = c.producer({ lingerMs: 0 })
				await p.send(
					input,
					slice.map(r => ({ key: r.key, value: Buffer.from(r.value) }))
				)
				await p.disconnect()
				break
			} catch {
				await sleep(500)
			}
		}
		sentCount = i + slice.length
		await sleep(200)
	}
	producerDone = true
})()

const w = new Worker('w0', workerEnv({ appId, input, output, mode, guarantee })).start()
const sample = () => {
	const s = w.last('status')
	return {
		state: s?.state as string | undefined,
		processed: (s?.processed as number) ?? 0,
		error: s?.error as string | null,
	}
}
// wait for processing to begin
for (let i = 0; i < 60 && sample().processed < 40; i++) await sleep(500)
ev(`worker processing: ${JSON.stringify(sample())} sent=${sentCount}`)

// inject fault
const faultAt = Date.now()
if (fault === 'restart') {
	ev('docker restart ...')
	sh(`docker restart ${CONTAINER}`)
	ev('docker restart returned')
} else {
	sh(`docker pause ${CONTAINER}`)
	ev(`docker paused for ${pauseS}s`)
	await sleep(pauseS * 1000)
	sh(`docker unpause ${CONTAINER}`)
	ev('docker unpaused')
}
const brokerBackAt = Date.now()

// observe recovery
const timeline: string[] = []
let lastState = ''
let lastProcessed = sample().processed
let resumedAt: number | null = null
let producerDoneAt: number | null = null
const obsStart = Date.now()
while (Date.now() - obsStart < recoverMs) {
	await sleep(1000)
	const s = sample()
	const tag = `${s.state}${s.error ? ' err=' + s.error.slice(0, 120) : ''}`
	if (tag !== lastState) {
		timeline.push(`+${((Date.now() - T0) / 1000).toFixed(1)}s state=${tag}`)
		lastState = tag
	}
	if (s.processed > lastProcessed && resumedAt === null && Date.now() > faultAt + 500) {
		// first progress after the fault
		resumedAt = Date.now()
		ev(
			`processing progressed after fault (processed ${lastProcessed} -> ${s.processed}) ${((resumedAt - brokerBackAt) / 1000).toFixed(1)}s after broker back`
		)
	}
	lastProcessed = s.processed
	if (!w.alive) {
		ev('WORKER PROCESS DIED: ' + JSON.stringify(w.lines.slice(-3)))
		break
	}
	if (producerDone && producerDoneAt === null) producerDoneAt = Date.now()
	if (s.state === 'ERROR') {
		ev(`worker app.state()=ERROR (${s.error})`)
		break
	}
	// if the producer is done and processing has been quiet for 20s, stop observing
	if (producerDoneAt && Date.now() - producerDoneAt > 25000) break
}
await sendLoop.catch(() => {})
const final = sample()
const unhandled = w.lines.filter(l => /UNHANDLED|UNCAUGHT/.test(l.type))
const stderr = w.lines
	.filter(l => l.type === 'stderr')
	.map(l => l.line.trim())
	.slice(0, 5)

// verify with a fresh client (old one may hold dead connections)
await client.disconnect().catch(() => {})
client = await makeClient()
const actualInput = await readAll(client, input, 'read_uncommitted', { quietMs: 4000, maxMs: 30000 })
const actualRecs: Rec[] = actualInput.map(o => ({ key: o.key, value: o.value }))
const expectedByKey = new Map<string, number>()
for (const r of actualRecs) expectedByKey.set(r.key, (expectedByKey.get(r.key) ?? 0) + 1)
const dedupInput = new Set(actualRecs.map(r => r.value)).size !== actualRecs.length
let outs = await readAll(client, output, isolation, { quietMs: 8000, maxMs: 60000 })
const sum =
	mode === 'stateless' ? summarizeStateless(actualRecs, outs) : summarizeCount(keys, perKey, outs, expectedByKey)

// Is the app actually dead? Produce 1 sentinel per key and see if it still processes.
let stillAlive = 'n/a'
if (w.alive) {
	const before = outs.length
	const p = client.producer({ lingerMs: 0 })
	await p.send(input, [{ key: 'sentinel', value: Buffer.from('sentinel#1') }])
	await p.disconnect()
	await sleep(15000)
	const after = await readAll(client, output, isolation, { quietMs: 5000, maxMs: 30000 })
	stillAlive = after.length > before ? 'yes (processed sentinel)' : 'NO (sentinel not processed in 15s)'
}

let pass: boolean
if (guarantee === 'exactly_once') {
	pass =
		mode === 'stateless' ? (sum as any).missing === 0 && (sum as any).dupes === 0 : (sum as any).exactKeys === keys
} else {
	pass =
		mode === 'stateless'
			? (sum as any).missing === 0
			: (sum as any).belowKeys === 0 && (sum as any).lastBelowKeys === 0
}
if (final.state === 'ERROR' || stillAlive.startsWith('NO')) pass = false
console.log(`RESULT ${label}: ${pass ? 'PASS' : 'FAIL'}`)
console.log(
	'  fault->broker back:',
	((brokerBackAt - faultAt) / 1000).toFixed(1) + 's; resumed after broker back:',
	resumedAt ? ((resumedAt - brokerBackAt) / 1000).toFixed(1) + 's' : 'NEVER (within observation)'
)
console.log('  state timeline:', timeline)
console.log(
	'  final worker state:',
	JSON.stringify(final),
	'process alive:',
	w.alive,
	'processes sentinel:',
	stillAlive
)
console.log('  input harness dup:', dedupInput, 'actual input records:', actualRecs.length)
console.log(
	'  summary:',
	JSON.stringify(sum, (_, v) => (typeof v === 'bigint' ? v.toString() : v))
)
console.log(
	'  unhandled:',
	unhandled.length,
	unhandled.slice(0, 4).map(u => `${u.message} @ ${u.stack}`)
)
console.log('  stderr:', stderr)
console.log(
	'  worker events (non-status):',
	w.lines
		.filter(l => !['status', 'stderr'].includes(l.type))
		.slice(0, 12)
		.map(l => JSON.stringify(l))
)
await w.kill()
await client.disconnect().catch(() => {})
process.exit(pass ? 0 : 1)
