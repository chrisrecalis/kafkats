/**
 * Shared helpers for chaos harnesses (not runnable on its own).
 * Requires a broker at CHAOS_BROKERS (default 127.0.0.1:29092); run-all.sh starts it (brokers.sh).
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'
import path from 'node:path'
import { KafkaClient } from '@kafkats/client'

export const BROKERS = (process.env.CHAOS_BROKERS ?? '127.0.0.1:29092').split(',')
export const CONTAINER = process.env.CHAOS_CONTAINER ?? 'kafka-chaos'
/** Replication factor for harness and application topics (set to 3 on a multi-broker cluster). */
export const REPLICATION_FACTOR = Number(process.env.CHAOS_RF ?? '1')
/** All broker containers of the cluster, for rolling restarts (CHAOS_CONTAINERS=a,b,c). */
export const CONTAINERS = (process.env.CHAOS_CONTAINERS ?? CONTAINER).split(',')
const here = path.dirname(fileURLToPath(import.meta.url))
const WORKER = path.join(here, 'worker.ts')
export const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms))
export const rnd = (a: number, b: number) => a + Math.random() * (b - a)

/**
 * The pipelines of worker.ts. The join modes join the input with a table (`table-join`, co-partitioned) or a global
 * table (`global-join`) whose rows are versions written by a TableFeed; their outputs are `m:<value>|<version>`.
 */
export type Mode = 'stateless' | 'count' | 'regroup' | 'table-join' | 'global-join'
export type Guarantee = 'at_least_once' | 'exactly_once'
const MODES: readonly string[] = ['stateless', 'count', 'regroup', 'table-join', 'global-join']

/** `--name value` flags with these defaults; an unknown flag (such as the removed `--threads`) fails fast. */
export function flags<const T extends Record<string, string>>(defaults: T): { [K in keyof T]: string } {
	const options = Object.fromEntries(
		Object.entries(defaults).map(([name, value]) => [name, { type: 'string' as const, default: value }])
	)
	return parseArgs({ options, strict: true }).values as { [K in keyof T]: string }
}

export function guaranteeOf(flag: string): Guarantee {
	if (flag !== 'alo' && flag !== 'eos') throw new Error(`--guarantee must be alo or eos, not ${flag}`)
	return flag === 'eos' ? 'exactly_once' : 'at_least_once'
}

export function modeOf(flag: string): Mode {
	if (!MODES.includes(flag)) throw new Error(`--mode must be one of ${MODES.join('|')}, not ${flag}`)
	return flag as Mode
}

export const hasTable = (mode: Mode) => mode === 'table-join' || mode === 'global-join'

export function uid(prefix: string) {
	return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
}

export async function makeClient(): Promise<KafkaClient> {
	const c = new KafkaClient({ clientId: uid('chaos-harness'), brokers: BROKERS })
	await c.connect()
	return c
}

export async function createTopics(client: KafkaClient, names: string[], partitions = 4) {
	await client.createTopics(
		names.map(name => ({ name, numPartitions: partitions, replicationFactor: REPLICATION_FACTOR }))
	)
	const deadline = Date.now() + 20000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata(names)
		if (
			names.every(n => {
				const t = meta.topics.get(n)
				return t && t.partitions.size === partitions && [...t.partitions.values()].every(p => p.leaderId >= 0)
			})
		)
			return
		await sleep(100)
	}
	throw new Error('topics not ready')
}

/** The topics of one scenario (each with the scenario's partition count) and its application id. */
export interface Topics {
	appId: string
	input: string
	output: string
	/** The table (table-join) or global table (global-join) topic. */
	table?: string
}

export async function scenarioTopics(client: KafkaClient, mode: Mode, partitions: number): Promise<Topics> {
	const topics: Topics = { appId: uid('chaos-app'), input: uid('chaos-in'), output: uid('chaos-out') }
	if (hasTable(mode)) topics.table = uid('chaos-table')
	await createTopics(client, [topics.input, topics.output, ...(topics.table ? [topics.table] : [])], partitions)
	return topics
}

export interface Rec {
	key: string
	value: string
}
export interface Out {
	partition: number
	offset: bigint
	key: string
	value: string
}

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
					if (/UNHANDLED|UNCAUGHT|start_failed|close_error/.test(j.type))
						console.log(`  [${this.label}]`, line)
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

/** Worker env of a scenario; CHAOS_BUFFERED_RECORDS and the other optional knobs pass through the inherited env. */
export function workerEnv(p: Topics & { mode: Mode; guarantee: Guarantee }): Record<string, string> {
	return {
		CHAOS_BROKERS: BROKERS.join(','),
		CHAOS_RF: String(REPLICATION_FACTOR),
		CHAOS_APP_ID: p.appId,
		CHAOS_IN: p.input,
		CHAOS_OUT: p.output,
		...(p.table ? { CHAOS_TABLE: p.table } : {}),
		CHAOS_MODE: p.mode,
		CHAOS_GUARANTEE: p.guarantee,
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

export const json = (v: unknown) => JSON.stringify(v, (_, x) => (typeof x === 'bigint' ? x.toString() : x))
