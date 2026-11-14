/**
 * Scenarios A + B: SIGKILL chaos for stateless and count pipelines, ALO and EOS.
 *
 * Prereq:  start-kafka.sh kafka-chaos 29092   (broker at 127.0.0.1:29092; override with CHAOS_BROKERS)
 * Run (from packages/flow):
 *   pnpm exec tsx tests/chaos/kill-chaos.ts --guarantee eos --mode count --instances 2 --threads 1 --kills 8
 * Flags: --guarantee alo|eos  --mode stateless|count|regroup  --instances 1|2  --threads N  --kills N
 *        --keys 12 --per-key 100  --batch 12 --delay-ms 250 (producer pacing)  --drain-ms 120000
 * Set CHAOS_DUMP=/path/file.json to dump all output records + worker event logs for analysis.
 * Exit code 0 = PASS, 1 = FAIL. Prints evidence (missing/dup/gaps, unhandled rejections, worker error states).
 */
import {
	Worker, workerEnv, makeClient, createTopics, genInput, produceBatches, readAll, uid, sleep, rnd,
	summarizeStateless, summarizeCount, type Out,
} from './common.js'

const arg = (n: string, d: string) => {
	const i = process.argv.indexOf(`--${n}`)
	return i >= 0 ? process.argv[i + 1]! : d
}
const guarantee = arg('guarantee', 'alo') === 'eos' ? 'exactly_once' : 'at_least_once'
const mode = arg('mode', 'stateless') as 'stateless' | 'count' | 'regroup'
const instances = Number(arg('instances', '1'))
const threads = Number(arg('threads', '1'))
const kills = Number(arg('kills', '7'))
const keys = Number(arg('keys', '12'))
const perKey = Number(arg('per-key', '100'))
const batch = Number(arg('batch', '12'))
const delayMs = Number(arg('delay-ms', '250'))
const drainMs = Number(arg('drain-ms', '120000'))
const isolation = guarantee === 'exactly_once' ? 'read_committed' : 'read_uncommitted'

const label = `${guarantee}/${mode}/inst=${instances}/thr=${threads}`
const client = await makeClient()
const input = uid('chaos-in')
const output = uid('chaos-out')
const appId = uid('chaos-app')
await createTopics(client, [input, output], 4)
const inputs = genInput(keys, perKey)
console.log(`== ${label}: ${inputs.length} records, ${kills} kills; in=${input} out=${output}`)

let produced = false
const producing = produceBatches(client, input, inputs, batch, delayMs).then(() => {
	produced = true
})

const env = workerEnv({ appId, input, output, mode, guarantee, threads })
const all: Worker[] = []
const slots: Worker[] = []
const spawnWorker = (slot: number) => {
	const w = new Worker(`w${slot}.${all.length}`, env).start()
	all.push(w)
	slots[slot] = w
	return w
}
for (let i = 0; i < instances; i++) {
	spawnWorker(i)
	await sleep(500)
}

for (let k = 0; k < kills; k++) {
	await sleep(rnd(2000, 6000))
	const slot = k % instances
	const victim = slots[slot]!
	// prefer to kill a worker that is actively processing (wait up to 15s for its counter to be > 0)
	for (let i = 0; i < 30 && !((victim.last('status')?.processed ?? 0) > 0); i++) await sleep(500)
	await victim.kill()
	console.log(`  kill #${k + 1} ${victim.label} (processed=${victim.last('status')?.processed}, state=${victim.last('status')?.state})`)
	await sleep(rnd(0, 1500))
	spawnWorker(slot)
}
await producing
console.log('  producer finished, waiting for drain...')

const t0 = Date.now()
let outs: Out[] = []
let ok = false
let sum: any
while (Date.now() - t0 < drainMs) {
	outs = await readAll(client, output, isolation, { quietMs: 3000, maxMs: 20000 })
	if (mode === 'stateless') {
		sum = summarizeStateless(inputs, outs)
		ok = sum.missing === 0
	} else {
		sum = summarizeCount(keys, perKey, outs, undefined, mode === 'regroup' ? 'nk' : 'k')
		ok = sum.belowKeys === 0 && sum.lastBelowKeys === 0
	}
	if (ok) break
	console.log(`  not drained yet (${Date.now() - t0}ms):`, JSON.stringify({ ...sum, problems: undefined }))
}
const drainTook = Date.now() - t0
// let it settle, then final re-read (catches late dupes)
await sleep(5000)
outs = await readAll(client, output, isolation, { quietMs: 6000, maxMs: 40000 })
sum = mode === 'stateless' ? summarizeStateless(inputs, outs) : summarizeCount(keys, perKey, outs, undefined, mode === 'regroup' ? 'nk' : 'k')

if (process.env.CHAOS_DUMP) {
	const { writeFileSync } = await import('node:fs')
	writeFileSync(process.env.CHAOS_DUMP, JSON.stringify({ outs, workers: all.map(w => ({ label: w.label, lines: w.lines.filter(l => l.type !== 'status') })) }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)))
}
const unhandled = all.flatMap(w => w.lines.filter(l => /UNHANDLED|UNCAUGHT/.test(l.type)))
const errStates = all.flatMap(w => w.lines.filter(l => l.type === 'status' && (l.state === 'ERROR' || l.error)).slice(0, 1).map(l => ({ w: l.label, state: l.state, error: l.error })))
const startFails = all.flatMap(w => w.lines.filter(l => l.type === 'start_failed'))

let pass: boolean
if (guarantee === 'exactly_once') {
	pass = mode === 'stateless' ? sum.missing === 0 && sum.dupes === 0 && sum.unexpected === 0 : sum.exactKeys === keys
} else {
	pass = mode === 'stateless' ? sum.missing === 0 : sum.belowKeys === 0 && sum.lastBelowKeys === 0
}
console.log(`RESULT ${label}: ${pass ? 'PASS' : 'FAIL'} (drain ${drainTook}ms)`)
console.log('  summary:', JSON.stringify(sum, (_, v) => (typeof v === 'bigint' ? v.toString() : v)))
console.log('  unhandled rejections/exceptions:', unhandled.length, unhandled.slice(0, 3).map(u => u.message))
console.log('  worker ERROR states/errors:', JSON.stringify(errStates))
console.log('  start failures:', JSON.stringify(startFails.map(s => s.message)))

for (const w of slots) {
	if (w.alive) {
		const s = Date.now()
		const r = await Promise.race([w.term(), sleep(30000).then(() => 'TIMEOUT')])
		console.log(`  graceful close of ${w.label}: ${r === 'TIMEOUT' ? 'HUNG >30s (killing)' : Date.now() - s + 'ms'}`)
		if (r === 'TIMEOUT') await w.kill()
	}
}
await client.disconnect()
process.exit(pass ? 0 : 1)
