/**
 * Micro-benchmark of the task core (spec 9.6): records through `StreamTask.processOne` for a stateless topology and
 * a count topology (in-memory store), at-least-once commits every 10000 records, over the test ports of the task
 * harness, with nothing subscribed to the tracing channel. A measurement is the best of 7 runs, in ns per record. More
 * than 10% slower than `bench/baseline.json` in three measurements in a row fails. `--record` writes the baseline
 * instead (the median of three measurements).
 *
 *     pnpm -C packages/flow bench:task-core [--record]
 */
import { readFileSync, writeFileSync } from 'node:fs'
import type { Message } from '@kafkats/client'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { KeyValueStore } from '@/state.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { isThenable } from '@/processor/node.js'
import { flowDiagnostics } from '@/runtime/tracing.js'
import { message } from '../tests/unit/support/fake-io.js'
import { kvStore, passThrough, pipe, TaskHarness } from '../tests/unit/support/task-harness.js'

const RECORDS = 1_000_000
const WARMUP = 200_000
const BATCH = 500
const COMMIT_EVERY = 10_000
const KEYS = 1000
const RUNS = 7
const ATTEMPTS = 3
const TOLERANCE = 0.1
const BASELINE = new URL('./baseline.json', import.meta.url)

const count: ProcessorSupplier<string, string, string, string> = () => {
	let context: ProcessorContext<string, string>
	let store: KeyValueStore<string, string>
	return {
		init: ctx => {
			context = ctx
			store = ctx.getStateStore('counts')
		},
		process: async record => {
			const counted = Number((await store.get(record.key)) ?? '0') + 1
			await store.put(record.key, String(counted))
			return context.forward(record.withValue(String(counted)))
		},
	}
}

const scenarios: Record<string, (b: InternalTopologyBuilder) => void> = {
	stateless: b => pipe(b, { processor: passThrough }),
	count: b => pipe(b, { processor: count, stores: [kvStore('counts')] }),
}

/** ns per record over RECORDS records, after a warm-up. */
async function run(build: (b: InternalTopologyBuilder) => void): Promise<number> {
	const h = new TaskHarness({ build })
	const task = await h.start(h.task())
	const tp = { topic: 'input', partition: 0 }
	const batch: Message<Buffer, Buffer>[] = Array.from({ length: BATCH }, (_, i) =>
		message('input', 0, BigInt(i), { key: `key-${i % KEYS}`, value: 'value', timestamp: 1_000 + i })
	)
	const feed = async (records: number): Promise<void> => {
		const now = h.clock.now()
		for (let done = 0; done < records; done += BATCH) {
			task.addRecords(tp, batch)
			// Asked once per slice, as the scheduler does.
			const traced = flowDiagnostics.process.hasSubscribers
			for (;;) {
				let processed = task.processOne(now, traced)
				if (isThenable(processed)) {
					processed = await processed
				}
				if (!processed) {
					break
				}
			}
			if ((done + BATCH) % COMMIT_EVERY === 0) {
				await h.commit()
				h.producer.sent.length = 0
			}
		}
	}
	await feed(WARMUP)
	const start = process.hrtime.bigint()
	await feed(RECORDS)
	return Number(process.hrtime.bigint() - start) / RECORDS
}

/** The best of RUNS runs, each after a full GC: the machine's interference only ever adds time. */
async function measure(name: string, build: (b: InternalTopologyBuilder) => void): Promise<number> {
	const runs: number[] = []
	for (let i = 0; i < RUNS; i++) {
		globalThis.gc?.()
		runs.push(await run(build))
	}
	runs.sort((a, b) => a - b)
	console.log(`${name}: ${Math.round(runs[0]!)} ns/record (runs: ${runs.map(Math.round).join(', ')})`)
	return runs[0]!
}

/** Records the median of ATTEMPTS measurements per scenario: a typical value, not the luckiest one. */
async function record(): Promise<void> {
	const nsPerRecord: Record<string, number> = {}
	for (const [name, build] of Object.entries(scenarios)) {
		const measured: number[] = []
		for (let i = 0; i < ATTEMPTS; i++) measured.push(await measure(name, build))
		measured.sort((a, b) => a - b)
		nsPerRecord[name] = Math.round(measured[Math.floor(ATTEMPTS / 2)]!)
	}
	const baseline = { node: process.version, records: RECORDS, nsPerRecord }
	writeFileSync(BASELINE, `${JSON.stringify(baseline, null, '\t')}\n`)
	console.log(`baseline written to ${BASELINE.pathname}: ${JSON.stringify(nsPerRecord)}`)
}

/** A regression shows in every measurement; the machine's noise rarely does ATTEMPTS times in a row. */
async function check(): Promise<void> {
	const baseline = JSON.parse(readFileSync(BASELINE, 'utf8')) as { nsPerRecord: Record<string, number> }
	let failed = false
	for (const [name, build] of Object.entries(scenarios)) {
		const limit = baseline.nsPerRecord[name]! * (1 + TOLERANCE)
		let best = await measure(name, build)
		for (let attempt = 1; best > limit && attempt < ATTEMPTS; attempt++) {
			best = Math.min(best, await measure(name, build))
		}
		const ns = Math.round(best)
		failed ||= ns > limit
		console.log(
			`${name}: ${ns} ns/record, baseline ${baseline.nsPerRecord[name]} (limit ${Math.round(limit)}): ` +
				(ns <= limit ? 'ok' : 'REGRESSION')
		)
	}
	process.exitCode = failed ? 1 : 0
}

await (process.argv.includes('--record') ? record() : check())
