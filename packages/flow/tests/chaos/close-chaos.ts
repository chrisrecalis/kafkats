/**
 * Scenario D: graceful close() behaviour.
 *
 * Prereq: docker container `kafka-chaos` on 127.0.0.1:29092 (`tests/chaos/brokers.sh single`).
 * Run (from packages/flow):
 *   pnpm exec tsx tests/chaos/close-chaos.ts --scenario drain --guarantee eos --mode count   # SIGTERM -> close() under load, restart, verify
 *   pnpm exec tsx tests/chaos/close-chaos.ts --scenario start --delay-ms 300                 # close() while start() in progress
 *   pnpm exec tsx tests/chaos/close-chaos.ts --scenario broker-down --down pause              # close() while broker unreachable (pause|kill)
 * Flags: --guarantee alo|eos --mode stateless|count|regroup|table-join|global-join|window|join --cycles 4 --partitions 4
 *        --keys 12 --per-key 100 --delay-ms 0 (start) --down pause|kill (broker-down)
 * Exit 0 PASS / 1 FAIL. For `start`/`broker-down` PASS means close() returned within 30s and no member leaked.
 * Every scenario also fails on an unhandled rejection or uncaught exception in a worker, or a worker left in ERROR.
 */
import { execSync } from 'node:child_process'
import {
	Worker,
	workerEnv,
	makeClient,
	scenarioTopics,
	genInput,
	produceBatches,
	produceRight,
	tickStreamTime,
	lastEventTime,
	readAll,
	sleep,
	flags,
	guaranteeOf,
	modeOf,
	json,
	CONTAINER,
	type Out,
} from './common.js'
import { TableFeed } from './table-feed.js'
import { drained, passes, printFaults, summarize, workerFaults, type Summary } from './verdict.js'

const opts = flags({
	scenario: 'drain',
	guarantee: 'eos',
	mode: 'count',
	cycles: '4',
	partitions: '4',
	keys: '12',
	'per-key': '100',
	'delay-ms': '0',
	down: 'pause',
})
const scenario = opts.scenario
const guarantee = guaranteeOf(opts.guarantee)
const mode = modeOf(opts.mode)
const cycles = Number(opts.cycles)
const keys = Number(opts.keys)
const perKey = Number(opts['per-key'])
const isolation = guarantee === 'exactly_once' ? 'read_committed' : 'read_uncommitted'
const sh = (c: string) => execSync(c, { stdio: 'pipe' }).toString().trim()
const T0 = Date.now()
const ev = (m: string) => console.log(`  [+${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`)

const client = await makeClient()
const topics = await scenarioTopics(client, mode, Number(opts.partitions))
const { appId, input, output } = topics
const env = workerEnv({ ...topics, mode, guarantee })
const feed = topics.table ? await TableFeed.start(client, topics.table, keys) : null
await produceRight(client, topics, keys, perKey)
const workers: Worker[] = []
const startWorker = (label: string, extra: Record<string, string> = {}) => {
	const w = new Worker(label, { ...env, ...extra }).start()
	workers.push(w)
	return w
}
let pass = true

async function waitFor(w: Worker, type: string, ms: number) {
	const s = Date.now()
	while (Date.now() - s < ms) {
		const l = w.last(type)
		if (l) return l
		await sleep(100)
	}
	return null
}

async function membersOf(group: string): Promise<string> {
	try {
		const admin = client.admin()
		const [d] = await admin.describeGroups([group])
		return `state=${(d as any)?.state} members=${(d as any)?.members?.length}`
	} catch (e) {
		return 'describe failed: ' + (e as Error).message
	}
}

if (scenario === 'drain') {
	const inputs = genInput(keys, perKey, mode)
	console.log(
		`== close/drain ${guarantee}/${mode}: ${inputs.length} records, ${cycles} graceful close+restart cycles`
	)
	const producing = produceBatches(client, input, inputs, 12, 200)
	for (let c = 0; c < cycles; c++) {
		const w = startWorker(`c${c}`)
		for (let i = 0; i < 40 && (w.last('status')?.processed ?? 0) < 30; i++) await sleep(250)
		await sleep(500)
		const s = Date.now()
		const r = await Promise.race([w.term(), sleep(30000).then(() => 'HUNG')])
		const ce = w.last('close_error')
		const cd = w.last('close_done')
		ev(
			`cycle ${c}: processed=${w.last('status')?.processed} close ${r === 'HUNG' ? 'HUNG >30s' : cd ? cd.ms + 'ms' : ce ? 'error ' + ce.message : 'exit without close_done'} (total ${Date.now() - s}ms) exit=${JSON.stringify(r)}`
		)
		if (r === 'HUNG') {
			await w.kill()
			pass = false
		}
	}
	await producing
	const fin = startWorker('final')
	const sizes = { keys, perKey }
	let outs: Out[] = []
	let sum: Summary
	const t0 = Date.now()
	do {
		await tickStreamTime(client, topics, mode, Number(opts.partitions), lastEventTime(inputs))
		outs = await readAll(client, output, isolation, { quietMs: 3000, maxMs: 20000 })
		sum = summarize(mode, inputs, outs, sizes)
	} while (!drained(sum) && Date.now() - t0 < 90000)
	await sleep(4000)
	outs = await readAll(client, output, isolation, { quietMs: 5000, maxMs: 30000 })
	sum = summarize(mode, inputs, outs, sizes)
	pass = pass && passes(guarantee, sum)
	console.log(`  summary: ${json(sum)}`)
	await fin.kill()
} else if (scenario === 'start') {
	const delay = Number(opts['delay-ms'])
	console.log(`== close during start(): close() invoked ${delay}ms after start() begins (${guarantee}/${mode})`)
	await produceBatches(client, input, genInput(keys, 20), 50, 0)
	const w = startWorker('w', {
		CHAOS_CLOSE_DURING_START: '1',
		CHAOS_CLOSE_DURING_START_DELAY_MS: String(delay),
		CHAOS_NO_EXIT_AFTER_CLOSE: '1',
	})
	const cd = await waitFor(w, 'close_done', 40000)
	ev(
		`close_done: ${JSON.stringify(cd)} | started: ${JSON.stringify(w.last('started'))} | start_failed: ${JSON.stringify(w.last('start_failed'))}`
	)
	if (!cd) {
		console.log('  close() HUNG (>40s)')
		pass = false
	}
	await sleep(12000)
	ev(`12s after close: group ${await membersOf(appId)}; app state per worker: ${JSON.stringify(w.last('status'))}`)
	const members = await membersOf(appId)
	if (/members=[1-9]/.test(members)) {
		console.log('  LEAK: consumer still a group member after close() resolved')
		pass = false
	}
	const processedAfter = w.last('status')?.processed
	await sleep(3000)
	if ((w.last('status')?.processed ?? 0) > (processedAfter ?? 0)) {
		console.log('  LEAK: records still being processed after close()')
		pass = false
	}
	console.log(
		'  worker events:',
		w.lines
			.filter(l => !['status'].includes(l.type))
			.map(l => JSON.stringify(l))
			.slice(0, 10)
	)
	await w.kill()
} else if (scenario === 'broker-down') {
	const down = opts.down
	console.log(`== close while broker down (${down}) ${guarantee}/${mode}`)
	const inputs = genInput(keys, perKey, mode)
	const producing = produceBatches(client, input, inputs, 12, 100).catch(() => {})
	const w = startWorker('w')
	for (let i = 0; i < 60 && (w.last('status')?.processed ?? 0) < 40; i++) await sleep(500)
	ev(`processing: ${w.last('status')?.processed}`)
	await feed?.hold()
	if (down === 'pause') sh(`docker pause ${CONTAINER}`)
	else sh(`docker kill ${CONTAINER}`)
	ev('broker down; waiting 3s then SIGTERM')
	await sleep(3000)
	const s = Date.now()
	const r = await Promise.race([w.term(), sleep(90000).then(() => 'HUNG')])
	const took = Date.now() - s
	ev(
		`close: ${r === 'HUNG' ? 'HUNG >90s' : took + 'ms'} close_done=${JSON.stringify(w.last('close_done'))} close_error=${JSON.stringify(w.last('close_error'))}`
	)
	if (r === 'HUNG') {
		pass = false
		ev('process still alive: ' + w.alive)
		await w.kill()
	} else if (took > 30000) pass = false
	console.log(
		'  stderr:',
		w.lines
			.filter(l => l.type === 'stderr')
			.map(l => l.line.trim())
			.slice(0, 4)
	)
	if (down === 'pause') sh(`docker unpause ${CONTAINER}`)
	else {
		sh(`docker start ${CONTAINER}`)
		await sleep(15000)
	}
	await producing.catch(() => {})
}
await feed?.stop()
const faults = workerFaults(workers)
pass = pass && faults.ok
console.log(`RESULT ${scenario} ${guarantee}/${mode}: ${pass ? 'PASS' : 'FAIL'}`)
printFaults(faults)
await client.disconnect().catch(() => {})
process.exit(pass ? 0 : 1)
