/**
 * Scenarios A + B: SIGKILL chaos for every worker mode, ALO and EOS.
 *
 * Prereq: a broker at 127.0.0.1:29092 (override with CHAOS_BROKERS); `tests/chaos/brokers.sh single` starts one.
 * Run (from packages/flow):
 *   pnpm exec tsx tests/chaos/kill-chaos.ts --guarantee eos --mode count --instances 2 --kills 8
 * Flags: --guarantee alo|eos  --mode stateless|count|regroup|table-join|global-join  --instances N  --kills N
 *        --partitions 4  --keys 12 --per-key 100  --batch 12 --delay-ms 250 (producer pacing)  --drain-ms 120000
 *        --max-dupes N (ALO stateless/join: fail above N duplicates)  --max-count N (ALO count: fail above a count of N)
 * One worker process runs one app (there is no --threads: every app has one scheduler; use --instances).
 * Set CHAOS_DUMP=/path/file.json to dump all output records + worker event logs for analysis.
 * Exit code 0 = PASS, 1 = FAIL. Prints evidence (missing/dup/gaps, unhandled rejections, workers in ERROR).
 */
import {
	Worker,
	workerEnv,
	makeClient,
	scenarioTopics,
	genInput,
	produceBatches,
	readAll,
	sleep,
	rnd,
	flags,
	guaranteeOf,
	modeOf,
	json,
	type Out,
} from './common.js'
import { TableFeed } from './table-feed.js'
import { drained, passes, printFaults, summarize, workerFaults, type Summary } from './verdict.js'

const opts = flags({
	guarantee: 'alo',
	mode: 'stateless',
	instances: '1',
	kills: '7',
	partitions: '4',
	keys: '12',
	'per-key': '100',
	batch: '12',
	'delay-ms': '250',
	'drain-ms': '120000',
	'max-dupes': '',
	'max-count': '',
})
const guarantee = guaranteeOf(opts.guarantee)
const mode = modeOf(opts.mode)
const instances = Number(opts.instances)
const kills = Number(opts.kills)
const partitions = Number(opts.partitions)
const keys = Number(opts.keys)
const perKey = Number(opts['per-key'])
const drainMs = Number(opts['drain-ms'])
const isolation = guarantee === 'exactly_once' ? 'read_committed' : 'read_uncommitted'

const label = `${guarantee}/${mode}/inst=${instances}/p=${partitions}`
const client = await makeClient()
const topics = await scenarioTopics(client, mode, partitions)
const inputs = genInput(keys, perKey)
console.log(`== ${label}: ${inputs.length} records, ${kills} kills; in=${topics.input} out=${topics.output}`)

const feed = topics.table ? await TableFeed.start(client, topics.table, keys) : null
const producing = produceBatches(client, topics.input, inputs, Number(opts.batch), Number(opts['delay-ms']))

const env = workerEnv({ ...topics, mode, guarantee })
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
	console.log(
		`  kill #${k + 1} ${victim.label} (processed=${victim.last('status')?.processed}, state=${victim.last('status')?.state})`
	)
	await sleep(rnd(0, 1500))
	spawnWorker(slot)
}
await producing
await feed?.stop()
console.log(`  producer finished${feed ? ` (table at version ${feed.version})` : ''}, waiting for drain...`)

const sizes = { keys, perKey }
const t0 = Date.now()
let outs: Out[] = []
let sum: Summary
while (Date.now() - t0 < drainMs) {
	outs = await readAll(client, topics.output, isolation, { quietMs: 3000, maxMs: 20000 })
	sum = summarize(mode, inputs, outs, sizes)
	if (drained(sum)) break
	console.log(`  not drained yet (${Date.now() - t0}ms):`, json({ ...sum, problems: undefined }))
}
const drainTook = Date.now() - t0
// let it settle, then final re-read (catches late dupes)
await sleep(5000)
outs = await readAll(client, topics.output, isolation, { quietMs: 6000, maxMs: 40000 })
sum = summarize(mode, inputs, outs, sizes)

if (process.env.CHAOS_DUMP) {
	const { writeFileSync } = await import('node:fs')
	writeFileSync(
		process.env.CHAOS_DUMP,
		json({ outs, workers: all.map(w => ({ label: w.label, lines: w.lines.filter(l => l.type !== 'status') })) })
	)
}
const faults = workerFaults(all)
const startFails = all.flatMap(w => w.lines.filter(l => l.type === 'start_failed'))
const gate = aloGate(sum)

const pass = passes(guarantee, sum) && faults.ok && gate.ok
console.log(`RESULT ${label}: ${pass ? 'PASS' : 'FAIL'} (drain ${drainTook}ms)`)
console.log('  summary:', json(sum))
if (gate.message) console.log(`  ALO duplicate gate: ${gate.message}`)
printFaults(faults)
console.log('  start failures:', JSON.stringify(startFails.map(s => s.message)))

for (const w of slots) {
	if (w.alive) {
		const s = Date.now()
		const r = await Promise.race([w.term(), sleep(30000).then(() => 'TIMEOUT')])
		console.log(
			`  graceful close of ${w.label}: ${r === 'TIMEOUT' ? 'HUNG >30s (killing)' : Date.now() - s + 'ms'}`
		)
		if (r === 'TIMEOUT') await w.kill()
	}
}
await client.disconnect()
process.exit(pass ? 0 : 1)

/** --max-dupes / --max-count: duplicates under at-least-once must stay within the recorded baseline (spec 9.4). */
function aloGate(sum: Summary): { ok: boolean; message?: string } {
	if ('exactKeys' in sum) {
		if (!opts['max-count']) return { ok: true }
		const max = Number(opts['max-count'])
		return { ok: sum.maxCount <= max, message: `max count ${sum.maxCount} (limit ${max})` }
	}
	if (!opts['max-dupes']) return { ok: true }
	const max = Number(opts['max-dupes'])
	return { ok: sum.dupes <= max, message: `${sum.dupes} duplicates (limit ${max})` }
}
