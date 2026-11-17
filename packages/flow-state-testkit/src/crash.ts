/**
 * Round 3 / attack 1: SIGKILL a flow app (LMDB state) many times while input flows, restart on the SAME state dir.
 * Requires built dists (pnpm -C packages/client build && pnpm -C packages/flow build) and KAFKA_BROKERS.
 * Run: KAFKA_BROKERS=127.0.0.1:19092 pnpm exec vitest run tests/hardening-crash.test.ts
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
	ChildWorker,
	workerEnv,
	createTopic,
	makeClient,
	readCommitted,
	sleep,
	uniq,
	type CommittedRow,
} from './kafka-util.js'
import type { EngineUnderTest } from './engine.js'

export function crashSuite(engine: EngineUnderTest): void {
	const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
	const KEYS = 8
	const PER_KEY = 60
	// NOTE: timestamps must be near wall-clock: ancient record timestamps make the broker's time-based retention delete the output log
	const BASE = Math.floor(Date.now() / 1000) * 1000
	/** Key of the join mode's records that only move stream time. */
	const TICK = 'tick'
	/**
	 * The join mode's right stream: for two thirds of the keys, a record every 2.5 s from 1.25 s on, up to the last input
	 * record. The input records are 300 ms apart from BASE: each lies within 1 s of at most one of them, and none exactly
	 * 1 s from one, so it joins that one or none.
	 */
	const RIGHT: Array<{ key: string; value: string; timestamp: number }> = []
	for (let j = 0, t = 1_250; t <= (PER_KEY - 1) * 300; j++, t += 2_500) {
		for (let k = 0; k < KEYS; k++) {
			if (k % 3 !== 2) RIGHT.push({ key: `k${k}`, value: `r${j}`, timestamp: BASE + t })
		}
	}

	describe.skipIf(!brokers)(`${engine.name}: hardening: SIGKILL +`, () => {
		let client: any
		let lastOut = ''
		let lastWorkers: ChildWorker[] = []
		const out_ = (_r: unknown) => lastOut
		beforeAll(async () => {
			client = await makeClient(brokers!)
		})
		afterAll(async () => {
			await client?.disconnect()
		})

		/** Runs the mode's app through `kills` SIGKILLs; the output values are JSON of type `V`. */
		async function scenario<V = number>(
			guarantee: 'exactly_once' | 'at_least_once',
			mode: 'count' | 'window' | 'session' | 'join',
			kills: number
		) {
			const input = uniq('r3c-in')
			const out = uniq('r3c-out')
			const right = uniq('r3c-right')
			const app = uniq('r3c-app')
			const stateDir = mkdtempSync(join(tmpdir(), 'r3-crash-'))
			await createTopic(client, input, 3)
			await createTopic(client, out, 3)
			const env = {
				R3_BROKERS: brokers!.join(','),
				R3_APP: app,
				R3_IN: input,
				R3_OUT: out,
				R3_STATE: stateDir,
				R3_MODE: mode,
				R3_GUARANTEE: guarantee,
				...(mode === 'join' ? { R3_RIGHT: right } : {}),
				...(process.env.R3_TRACE ? { R3_TRACE: '1' } : {}),
			}
			const producer = client.producer({ lingerMs: 0, idempotent: true })
			if (mode === 'join') {
				// All of the right side first: each task reads its two partitions in timestamp order and waits for the
				// live input (KIP-695, the worker's maxTaskIdleMs).
				await createTopic(client, right, 3)
				await producer.send(
					right,
					RIGHT.map(r => ({ key: r.key, value: Buffer.from(r.value), timestamp: new Date(r.timestamp) }))
				)
			}
			let produced = false
			// per-key sequence, round-robin over keys; timestamps advance 300ms per record so windows of 1s hold 3-4 records
			const producing = (async () => {
				for (let s = 0; s < PER_KEY; s++) {
					await producer.send(
						input,
						Array.from({ length: KEYS }, (_, k) => ({
							key: `k${k}`,
							value: Buffer.from(`k${k}#${s}`),
							timestamp: new Date(BASE + s * 300),
						}))
					)
					await sleep(120)
				}
				produced = true
			})()
			const allWorkers: ChildWorker[] = []
			const spawn = () => {
				const x = new ChildWorker({ ...env, ...workerEnv(engine) }).start()
				allWorkers.push(x)
				return x
			}
			let w = spawn()
			try {
				for (let i = 0; i < kills; i++) {
					await sleep(1_500 + Math.random() * 2_500)
					// phase variety: sometimes kill very soon after spawn (during startup / restoration)
					await w.kill()
					w = spawn()
					if (i % 3 === 2) await sleep(300 + Math.random() * 900)
				}
				await producing
				// final: let it drain
				const total = KEYS * PER_KEY
				const deadline = Date.now() + 60_000
				let rows: CommittedRow<V>[] = []
				for (let tick = 0; Date.now() < deadline; tick++) {
					if (mode === 'join') {
						// The last records of each partition are joined with null once stream time passed their window.
						await producer.send(
							input,
							[0, 1, 2].map(partition => ({
								key: TICK,
								value: Buffer.from('t'),
								partition,
								timestamp: new Date(BASE + PER_KEY * 300 + 5_000 + tick),
							}))
						)
					}
					await sleep(2_000)
					if (w.proc.exitCode !== null) w = spawn()
					rows = await readCommitted<V>(client, out, 1_500)
					if (mode === 'join') {
						if (rows.filter(r => r.key !== TICK).length >= total) break
					} else if (mode === 'count') {
						const finals = new Map<string, number>()
						for (const r of rows) finals.set(r.key, Math.max(finals.get(r.key) ?? 0, Number(r.value)))
						if ([...finals.values()].filter(v => v >= PER_KEY).length === KEYS) break
					} else if (mode === 'window') {
						const sum = new Map<string, number>()
						for (const r of rows) sum.set(r.key, Math.max(sum.get(r.key) ?? 0, Number(r.value)))
						if ([...sum.values()].reduce((a, b) => a + b, 0) >= total) break
					} else if (rows.filter(r => r.value === PER_KEY).length === KEYS) {
						break // session: every key's single session has absorbed all records
					}
				}
				void total
				lastOut = out
				lastWorkers = allWorkers
				return { rows, lines: w.lines }
			} finally {
				await w.kill().catch(() => {})
				await producer.disconnect().catch(() => {})
				rmSync(stateDir, { recursive: true, force: true })
				void produced
			}
		}

		it('exactly_once count: every key emits exactly 1..N in commit order (partition order) after >=8 SIGKILLs', async () => {
			const { rows, lines } = await scenario('exactly_once', 'count', 8)
			const per = new Map<string, number[]>()
			for (const r of rows) (per.get(r.key) ?? per.set(r.key, []).get(r.key)!).push(r.value!)
			const bad: string[] = []
			for (let k = 0; k < KEYS; k++) {
				const seq = per.get(`k${k}`) ?? []
				const want = Array.from({ length: PER_KEY }, (_, i) => i + 1)
				if (JSON.stringify(seq) !== JSON.stringify(want)) bad.push(`k${k}: ${JSON.stringify(seq)}`)
			}
			if (bad.length) {
				const again = await readCommitted(client, out_(rows), 2_500)
				console.log('OUTTOPIC', lastOut)
				console.log(
					'RAW partition of k0',
					JSON.stringify(
						again
							.filter(r => r.partition === (rows.find(x => x.key === 'k0')?.partition ?? 0))
							.slice(0, 60)
							.map(r => `${String(r.offset)}:${r.key}=${r.value}`)
					)
				)
				console.log(
					'RAW k0 first read',
					JSON.stringify(rows.filter(r => r.key === 'k0').map(r => [r.partition, String(r.offset), r.value]))
				)
				console.log(
					'RAW k0 re-read',
					JSON.stringify(again.filter(r => r.key === 'k0').map(r => [r.partition, String(r.offset), r.value]))
				)
				for (const x of lastWorkers)
					console.log(
						`--- worker pid ${x.pid}\n` +
							x.lines
								.filter(l => !l.includes('tx_send'))
								.slice(0, 60)
								.join('\n')
					)
				void lines
			}
			expect(bad).toEqual([])
		}, 180_000)

		it('exactly_once windowed count: per window final == truth, no repeats/gaps in per-window sequence', async () => {
			const { rows } = await scenario('exactly_once', 'window', 8)
			// truth: window w (start = floor((BASE+s*300)/1000)*1000) per key has n records
			const truth = new Map<string, number>()
			for (let k = 0; k < KEYS; k++)
				for (let s = 0; s < PER_KEY; s++) {
					const ws = Math.floor((BASE + s * 300) / 1000) * 1000
					truth.set(`k${k}@${ws}`, (truth.get(`k${k}@${ws}`) ?? 0) + 1)
				}
			const seqs = new Map<string, number[]>()
			for (const r of rows) (seqs.get(r.key) ?? seqs.set(r.key, []).get(r.key)!).push(r.value!)
			const bad: string[] = []
			for (const [k, n] of truth) {
				const got = seqs.get(k) ?? []
				const want = Array.from({ length: n }, (_, i) => i + 1)
				if (JSON.stringify(got) !== JSON.stringify(want)) bad.push(`${k}: got ${JSON.stringify(got)} want ${n}`)
			}
			expect(bad).toEqual([])
		}, 180_000)

		it('exactly_once session windows: live sessions after >=8 SIGKILLs equal ground truth', async () => {
			// records are 300ms apart with a 1s inactivity gap => exactly one session per key spanning all of them;
			// every extension re-emits the grown session and retracts the previous one
			const { rows } = await scenario('exactly_once', 'session', 8)
			const view = new Map<string, number | null>()
			for (const r of rows) view.set(r.key, r.value)
			const live = Object.fromEntries([...view].filter(([, v]) => v !== null).sort())
			const truth: Record<string, number> = {}
			for (let k = 0; k < KEYS; k++) {
				const start = BASE
				let end = BASE
				let n = 1
				for (let s = 1; s < PER_KEY; s++) {
					end = BASE + s * 300
					n++
				}
				truth[`k${k}@${start}-${end}`] = n
			}
			expect(live).toEqual(truth)
		}, 180_000)

		it('exactly_once stream-stream left join: every input record is joined exactly once after >=8 SIGKILLs', async () => {
			const { rows } = await scenario<string>('exactly_once', 'join', 8)
			const want: string[] = []
			for (let s = 0; s < PER_KEY; s++) {
				for (let k = 0; k < KEYS; k++) {
					const timestamp = BASE + s * 300
					const partner = RIGHT.find(r => r.key === `k${k}` && Math.abs(r.timestamp - timestamp) <= 1_000)
					want.push(`k${k}=k${k}#${s}|${partner?.value ?? 'null'}`)
				}
			}
			const got = rows.filter(r => r.key !== TICK).map(r => `${r.key}=${r.value}`)
			expect(want.filter(row => !row.endsWith('|null')).length, 'matched records').toBeGreaterThan(KEYS * 20)
			expect(got.sort()).toEqual(want.sort())
		}, 180_000)

		it('at_least_once count: no record lost (final >= N per key) and final is never below truth', async () => {
			const { rows } = await scenario('at_least_once', 'count', 8)
			const finals = new Map<string, number>()
			for (const r of rows) finals.set(r.key, Math.max(finals.get(r.key) ?? 0, r.value!))
			for (let k = 0; k < KEYS; k++) expect(finals.get(`k${k}`) ?? 0).toBeGreaterThanOrEqual(PER_KEY)
		}, 180_000)
	})
}
