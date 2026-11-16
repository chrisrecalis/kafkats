/**
 * Micro-benchmark of the task core (spec 9.6): records through `StreamTask.processOne` for a stateless topology and
 * a count topology (in-memory store), at-least-once commits every 10000 records, over the test ports of the task
 * harness. Prints ns per record (median of 5 runs) and compares with `bench/baseline.json`: more than 10% slower
 * fails. `--record` writes the baseline instead.
 *
 *     pnpm -C packages/flow bench:task-core [--record]
 */
import { readFileSync, writeFileSync } from 'node:fs'
import type { Message } from '@kafkats/client'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { KeyValueStore } from '@/state.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { isThenable } from '@/processor/node.js'
import { kvStore, message, passThrough, pipe, TaskHarness } from '../tests/unit/support/task-harness.js'

const RECORDS = 1_000_000
const WARMUP = 200_000
const BATCH = 500
const COMMIT_EVERY = 10_000
const KEYS = 1000
const RUNS = 5
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
			for (;;) {
				let processed = task.processOne(now)
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

async function main(): Promise<void> {
	const results: Record<string, number> = {}
	for (const [name, build] of Object.entries(scenarios)) {
		const runs: number[] = []
		for (let i = 0; i < RUNS; i++) runs.push(await run(build))
		runs.sort((a, b) => a - b)
		results[name] = Math.round(runs[Math.floor(RUNS / 2)]!)
		console.log(`${name}: ${results[name]} ns/record (runs: ${runs.map(Math.round).join(', ')})`)
	}
	if (process.argv.includes('--record')) {
		const baseline = { node: process.version, records: RECORDS, nsPerRecord: results }
		writeFileSync(BASELINE, `${JSON.stringify(baseline, null, '\t')}\n`)
		console.log(`baseline written to ${BASELINE.pathname}`)
		return
	}
	const baseline = JSON.parse(readFileSync(BASELINE, 'utf8')) as { nsPerRecord: Record<string, number> }
	let failed = false
	for (const [name, ns] of Object.entries(results)) {
		const limit = baseline.nsPerRecord[name]! * (1 + TOLERANCE)
		const verdict = ns <= limit ? 'ok' : 'REGRESSION'
		failed ||= ns > limit
		console.log(
			`${name}: ${ns} ns/record, baseline ${baseline.nsPerRecord[name]} (limit ${Math.round(limit)}): ${verdict}`
		)
	}
	process.exitCode = failed ? 1 : 0
}

await main()
