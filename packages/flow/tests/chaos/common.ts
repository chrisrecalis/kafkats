/**
 * Shared helpers for chaos harnesses (not runnable on its own).
 * Requires a broker at CHAOS_BROKERS (default 127.0.0.1:29092) - see top of kill-chaos.ts.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { KafkaClient } from '@kafkats/client'

export const BROKERS = (process.env.CHAOS_BROKERS ?? '127.0.0.1:29092').split(',')
export const CONTAINER = process.env.CHAOS_CONTAINER ?? 'kafka-chaos'
const here = path.dirname(fileURLToPath(import.meta.url))
export const WORKER = path.join(here, 'worker.ts')
export const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms))
export const rnd = (a: number, b: number) => a + Math.random() * (b - a)

export function uid(prefix: string) {
	return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
}

export async function makeClient(): Promise<KafkaClient> {
	const c = new KafkaClient({ clientId: uid('chaos-harness'), brokers: BROKERS })
	await c.connect()
	return c
}

export async function createTopics(client: KafkaClient, names: string[], partitions = 4) {
	await client.createTopics(names.map(name => ({ name, numPartitions: partitions, replicationFactor: 1 })))
	const deadline = Date.now() + 20000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata(names)
		if (names.every(n => {
			const t = meta.topics.get(n)
			return t && t.partitions.size === partitions && [...t.partitions.values()].every(p => p.leaderId >= 0)
		})) return
		await sleep(100)
	}
	throw new Error('topics not ready')
}

export interface Rec { key: string; value: string }
export interface Out { partition: number; offset: bigint; key: string; value: string }

/** Read all of `topic` with a fresh group until `done(msgs)` is true, or `quietMs` passes with no new message, or maxMs. */
export async function readAll(
	client: KafkaClient,
	topic: string,
	isolation: 'read_committed' | 'read_uncommitted',
	opts: { done?: (n: number) => boolean; quietMs?: number; maxMs?: number } = {}
): Promise<Out[]> {
	const out: Out[] = []
	const consumer = client.consumer({
		groupId: uid('chaos-verify'),
		autoOffsetReset: 'earliest',
		isolationLevel: isolation,
		maxWaitMs: 100,
	})
	let last = Date.now()
	const run = consumer.runEach([topic], async m => {
		out.push({
			partition: m.partition,
			offset: m.offset,
			key: m.key ? m.key.toString() : '',
			value: m.value ? m.value.toString() : '',
		})
		last = Date.now()
	})
	run.catch(() => {})
	const start = Date.now()
	const quietMs = opts.quietMs ?? 8000
	const maxMs = opts.maxMs ?? 120000
	while (Date.now() - start < maxMs) {
		await sleep(200)
		if (opts.done?.(out.length)) break
		if (Date.now() - last > quietMs && Date.now() - start > quietMs) break
	}
	consumer.stop()
	await run.catch(() => {})
	return out
}

export class Worker {
	proc: ChildProcess | null = null
	lines: any[] = []
	exited: Promise<{ code: number | null; signal: string | null }> | null = null
	constructor(
		readonly label: string,
		readonly env: Record<string, string>
	) {}
	start() {
		// `node --import tsx` so that SIGKILL hits the actual worker process (the `tsx` CLI wrapper would orphan it).
		const p = spawn(process.execPath, ['--import', 'tsx', WORKER], {
			env: { ...process.env, ...this.env },
			stdio: ['ignore', 'pipe', 'pipe'],
		})
		this.proc = p
		let buf = ''
		p.stdout!.on('data', d => {
			buf += d.toString()
			let i
			while ((i = buf.indexOf('\n')) >= 0) {
				const line = buf.slice(0, i)
				buf = buf.slice(i + 1)
				try {
					const j = JSON.parse(line)
					j.label = this.label
					this.lines.push(j)
					if (/UNHANDLED|UNCAUGHT|start_failed|close_error/.test(j.type)) console.log(`  [${this.label}]`, line)
				} catch {
					this.lines.push({ type: 'raw', line, label: this.label })
				}
			}
		})
		p.stderr!.on('data', d => {
			const s = d.toString()
			this.lines.push({ type: 'stderr', line: s, label: this.label })
		})
		this.exited = new Promise(res => p.on('exit', (code, signal) => res({ code, signal })))
		return this
	}
	kill() {
		this.proc?.kill('SIGKILL')
		return this.exited
	}
	term() {
		this.proc?.kill('SIGTERM')
		return this.exited
	}
	get alive() {
		return !!this.proc && this.proc.exitCode === null && this.proc.signalCode === null
	}
	last(type: string) {
		for (let i = this.lines.length - 1; i >= 0; i--) if (this.lines[i].type === type) return this.lines[i]
		return null
	}
	count(type: string) {
		return this.lines.filter(l => l.type === type).length
	}
}

export function workerEnv(p: {
	appId: string
	input: string
	output: string
	mode: 'stateless' | 'count' | 'regroup'
	guarantee: 'at_least_once' | 'exactly_once'
	threads?: number
}): Record<string, string> {
	return {
		CHAOS_BROKERS: BROKERS.join(','),
		CHAOS_APP_ID: p.appId,
		CHAOS_IN: p.input,
		CHAOS_OUT: p.output,
		CHAOS_MODE: p.mode,
		CHAOS_GUARANTEE: p.guarantee,
		CHAOS_THREADS: String(p.threads ?? 1),
	}
}

/** Input generator: KEYS keys x PER_KEY records; value = `${key}#${seq}` (seq is per-key 1..PER_KEY). Interleaved round-robin. */
export function genInput(keys: number, perKey: number): Rec[] {
	const recs: Rec[] = []
	for (let s = 1; s <= perKey; s++) for (let k = 0; k < keys; k++) recs.push({ key: `k${k}`, value: `k${k}#${s}` })
	return recs
}

export async function produceBatches(
	client: KafkaClient,
	topic: string,
	recs: Rec[],
	batch: number,
	delayMs: number,
	stop?: () => boolean
) {
	const producer = client.producer({ lingerMs: 0, idempotent: true } as any)
	for (let i = 0; i < recs.length; i += batch) {
		if (stop?.()) break
		await producer.send(
			topic,
			recs.slice(i, i + batch).map(r => ({ key: r.key, value: Buffer.from(r.value) }))
		)
		if (delayMs) await sleep(delayMs)
	}
	await producer.disconnect()
}

export function summarizeStateless(inputs: Rec[], outs: Out[]) {
	const want = new Map<string, number>()
	for (const r of inputs) want.set(`m:${r.value}`, (want.get(`m:${r.value}`) ?? 0) + 1)
	const seen = new Map<string, number>()
	for (const o of outs) seen.set(o.value, (seen.get(o.value) ?? 0) + 1)
	let missing = 0
	let dupes = 0
	let unexpected = 0
	const missingSample: string[] = []
	const dupSample: string[] = []
	for (const [v, w] of want) {
		const c = seen.get(v) ?? 0
		if (c < w) {
			missing += w - c
			if (missingSample.length < 5) missingSample.push(v)
		} else if (c > w) {
			dupes += c - w
			if (dupSample.length < 5) dupSample.push(`${v}x${c}`)
		}
	}
	for (const v of seen.keys()) if (!want.has(v)) unexpected++
	return { inputs: inputs.length, outputs: outs.length, missing, missingSample, dupes, dupSample, unexpected }
}

/** counts per key in output order. returns {gaps, repeats, maxByKey, finalByKey} relative to expected perKey. */
export function summarizeCount(keys: number, perKeyDefault: number, outs: Out[], expectedByKey?: Map<string, number>, keyPrefix = 'k') {
	const byKey = new Map<string, number[]>()
	for (const o of outs) {
		const a = byKey.get(o.key) ?? []
		a.push(Number(o.value))
		byKey.set(o.key, a)
	}
	let exactKeys = 0
	let belowKeys = 0
	let lastBelowKeys = 0
	let nonMonotonicKeys = 0
	const problems: string[] = []
	let maxBelow = 0
	for (let k = 0; k < keys; k++) {
		const a = byKey.get(`${keyPrefix}${k}`) ?? []
		const perKey = expectedByKey?.get(`k${k}`) ?? perKeyDefault
		const max = a.length ? Math.max(...a) : 0
		const last = a.length ? a[a.length - 1] : 0
		const exact = a.length === perKey && a.every((v, i) => v === i + 1)
		if (exact) exactKeys++
		if (last < perKey) lastBelowKeys++
		if (a.some((v, i) => i > 0 && v < a[i - 1])) nonMonotonicKeys++
		if (max < perKey) {
			belowKeys++
			maxBelow = Math.max(maxBelow, perKey - max)
		}
		if (!exact && problems.length < 6) problems.push(`k${k}: n=${a.length} max=${max} last=${last} seq=${compress(a)}`)
	}
	return { keys, perKey: perKeyDefault, exactKeys, belowKeys, lastBelowKeys, nonMonotonicKeys, maxBelow, problems }
}

function compress(a: number[]): string {
	if (a.length <= 30) return a.join(',')
	return a.slice(0, 12).join(',') + ',...,' + a.slice(-12).join(',')
}
