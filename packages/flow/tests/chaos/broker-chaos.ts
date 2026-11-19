/**
 * Scenario C: broker restart / pause chaos (and E: unhandled rejections are logged by worker).
 *
 * Prereq: docker container `kafka-chaos` on 127.0.0.1:29092 (`tests/chaos/brokers.sh single`). Uses docker restart/pause on it ONLY.
 * Run (from packages/flow):
 *   pnpm exec tsx tests/chaos/broker-chaos.ts --guarantee eos --mode stateless --fault restart
 *   pnpm exec tsx tests/chaos/broker-chaos.ts --guarantee alo --mode count --fault pause --pause-s 25
 * Flags: --guarantee alo|eos --mode stateless|count|regroup|table-join|global-join|window|join
 *        --fault restart|pause|kill|rolling (multi-broker: CHAOS_RF=3 CHAOS_CONTAINER=<one> CHAOS_CONTAINERS=<all>)
 *        --pause-s 25 --partitions 4 --keys 12 --per-key 100
 *        --recover-ms 120000 (how long to wait for self-recovery before declaring HANG)
 * The join modes keep writing table versions before and after the fault (none in flight across it).
 * Timeline: produce; once worker is processing inject fault while producer keeps going; then watch the worker:
 *   state, processed counter progress, errors. Reports precise phases: stall start, broker back, resume (or never).
 * CHAOS_DEBUG=1 (+ KAFKA_TS_LOG_LEVEL=debug) prints the worker's raw log lines at the end.
 * Exit 0 PASS / 1 FAIL.
 */
import { execSync } from 'node:child_process'
import {
	Worker,
	workerEnv,
	makeClient,
	scenarioTopics,
	genInput,
	produceRight,
	readAll,
	sleep,
	tickStreamTime,
	lastEventTime,
	TICK_KEY,
	flags,
	guaranteeOf,
	modeOf,
	json,
	CONTAINER,
	CONTAINERS,
	type Rec,
} from './common.js'
import { TableFeed } from './table-feed.js'
import { passes, printFaults, summarize, workerFaults } from './verdict.js'

const opts = flags({
	guarantee: 'alo',
	mode: 'stateless',
	fault: 'restart',
	'pause-s': '25',
	partitions: '4',
	keys: '12',
	'per-key': '100',
	'recover-ms': '120000',
})
const guarantee = guaranteeOf(opts.guarantee)
const mode = modeOf(opts.mode)
const fault = opts.fault
const pauseS = Number(opts['pause-s'])
const keys = Number(opts.keys)
const perKey = Number(opts['per-key'])
const recoverMs = Number(opts['recover-ms'])
const isolation = guarantee === 'exactly_once' ? 'read_committed' : 'read_uncommitted'
const label = `${guarantee}/${mode}/${fault}${fault === 'pause' ? pauseS + 's' : ''}`
const sh = (c: string) => execSync(c, { stdio: 'pipe' }).toString().trim()
const T0 = Date.now()
const ev = (m: string) => console.log(`  [+${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`)

let client = await makeClient()
const topics = await scenarioTopics(client, mode, Number(opts.partitions))
const { input, output } = topics
const inputs = genInput(keys, perKey, mode)
console.log(`== ${label}: ${inputs.length} records`)
const feed = topics.table ? await TableFeed.start(client, topics.table, keys) : null
await produceRight(client, topics, keys, perKey)

// producer that survives broker outages: retries each batch on a fresh producer until it succeeds
let sentCount = 0
let producerDone = false
const sendLoop = (async () => {
	// own client + producer, recreated whenever a send fails, so a harness-side dead connection can't mask the app's behaviour
	let pc = await makeClient()
	let p = pc.producer({ lingerMs: 0 })
	const batch = 12
	for (let i = 0; i < inputs.length; i += batch) {
		const slice: Rec[] = inputs.slice(i, i + batch)
		for (;;) {
			try {
				await p.send(
					input,
					slice.map(r => ({
						key: r.key,
						value: Buffer.from(r.value),
						...(r.timestamp === undefined ? {} : { timestamp: new Date(r.timestamp) }),
					}))
				)
				break
			} catch {
				await sleep(500)
				try {
					await pc.disconnect()
				} catch {}
				try {
					pc = await makeClient()
					p = pc.producer({ lingerMs: 0 })
				} catch {}
			}
		}
		sentCount = i + slice.length
		await sleep(200)
	}
	producerDone = true
	await pc.disconnect().catch(() => {})
})()

const w = new Worker('w0', workerEnv({ ...topics, mode, guarantee })).start()
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
await feed?.hold()
const faultAt = Date.now()
if (fault === 'restart') {
	ev('docker restart ...')
	sh(`docker restart ${CONTAINER}`)
	ev('docker restart returned')
} else if (fault === 'kill') {
	// Hard-kill one broker and keep it down: leaders and coordinators must fail over.
	sh(`docker kill ${CONTAINER}`)
	ev(`docker killed ${CONTAINER}, down for ${pauseS}s`)
	await sleep(pauseS * 1000)
	sh(`docker start ${CONTAINER}`)
	ev('docker started')
} else if (fault === 'rolling') {
	// Restart every broker of the cluster, one after the other.
	for (const container of CONTAINERS) {
		sh(`docker restart ${container}`)
		ev(`docker restarted ${container}`)
		await sleep(pauseS * 1000)
	}
} else {
	sh(`docker pause ${CONTAINER}`)
	ev(`docker paused for ${pauseS}s`)
	await sleep(pauseS * 1000)
	sh(`docker unpause ${CONTAINER}`)
	ev('docker unpaused')
}
const brokerBackAt = Date.now()
feed?.release()

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
await feed?.stop()
const final = sample()
const stderr = w.lines
	.filter(l => l.type === 'stderr')
	.map(l => l.line.trim())
	.slice(0, 5)

// verify with a fresh client (old one may hold dead connections)
await client.disconnect().catch(() => {})
client = await makeClient()
const actualInput = await readAll(client, input, 'read_uncommitted', { quietMs: 4000, maxMs: 30000 })
const actualRecs: Rec[] = actualInput
	.filter(o => o.key !== TICK_KEY)
	.map(o => ({ key: o.key, value: o.value, timestamp: o.timestamp }))
const dedupInput = new Set(actualRecs.map(r => r.value)).size !== actualRecs.length
await tickStreamTime(client, topics, mode, Number(opts.partitions), lastEventTime(actualRecs), 3)
const outs = await readAll(client, output, isolation, { quietMs: 8000, maxMs: 60000 })
const sum = summarize(mode, actualRecs, outs, { keys, perKey })

// Is the app actually dead? Produce a sentinel (on a key the join modes' tables hold) and see if it still processes.
let stillAlive = 'n/a'
if (w.alive) {
	const before = outs.length
	const p = client.producer({ lingerMs: 0 })
	await p.send(input, [{ key: 'k0', value: Buffer.from('sentinel#1') }])
	await p.disconnect()
	// The join mode emits the (unmatched) sentinel once stream time passed its window.
	await tickStreamTime(client, topics, mode, Number(opts.partitions), Date.now(), 3)
	await sleep(15000)
	const after = await readAll(client, output, isolation, { quietMs: 5000, maxMs: 30000 })
	stillAlive = after.length > before ? 'yes (processed sentinel)' : 'NO (sentinel not processed in 15s)'
}

const faults = workerFaults([w])
const pass = passes(guarantee, sum) && faults.ok && final.state !== 'ERROR' && !stillAlive.startsWith('NO')
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
if (feed) console.log(`  table versions written: ${feed.version}`)
console.log('  summary:', json(sum))
printFaults(faults)
console.log('  stderr:', stderr)
console.log(
	'  worker events (non-status):',
	w.lines
		.filter(l => !['status', 'stderr'].includes(l.type))
		.slice(0, 12)
		.map(l => JSON.stringify(l))
)
if (process.env.CHAOS_DEBUG) {
	const raw = w.lines.filter(l => l.type === 'raw' || l.type === 'stderr').map(l => String(l.line ?? '').trim())
	const counts = new Map<string, number>()
	for (const r of raw) {
		let key = r.slice(0, 200)
		try {
			const j = JSON.parse(r)
			key = `${j.level} ${j.component ?? ''}: ${j.message} | ${String(j.error ?? '').slice(0, 100)}`
		} catch {}
		counts.set(key, (counts.get(key) ?? 0) + 1)
	}
	console.log('  worker client log summary (count x message):')
	for (const [k, v] of [...counts].sort((a, b) => b[1] - a[1]).slice(0, 25)) console.log(`    ${v} x ${k}`)
}
await w.kill()
await client.disconnect().catch(() => {})
process.exit(pass ? 0 : 1)
