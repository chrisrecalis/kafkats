/**
 * Helpers for the broker-backed tests: client/topic setup, a read_committed reader and the
 * `hardening-worker.ts` child process.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import type { EngineUnderTest } from './engine.js'
import { KafkaClient } from '@kafkats/client'

export const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms))
export const uniq = (p: string) => `${p}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
const here = path.dirname(fileURLToPath(import.meta.url))

/** Environment that tells `hardening-worker.ts` which provider to create. */
export const workerEnv = (engine: EngineUnderTest): Record<string, string> => ({
	R3_PROVIDER_MODULE: engine.module.url,
	R3_PROVIDER_FACTORY: engine.module.factory,
})

export class ChildWorker {
	proc!: ChildProcess
	lines: string[] = []
	exited!: Promise<void>
	pid = 0
	constructor(readonly env: Record<string, string>) {}
	start(): this {
		this.proc = spawn(process.execPath, ['--import', 'tsx', path.join(here, 'hardening-worker.ts')], {
			cwd: path.resolve(here, '..'),
			env: { ...process.env, ...this.env },
			stdio: ['ignore', 'pipe', 'pipe'],
		})
		this.pid = this.proc.pid ?? 0
		this.proc.stdout!.on('data', d => this.lines.push(...d.toString().split('\n').filter(Boolean)))
		this.proc.stderr!.on('data', d => this.lines.push(`STDERR ${d}`))
		this.exited = new Promise(res => this.proc.on('exit', () => res()))
		return this
	}
	get started() {
		return this.lines.some(l => l.includes('"started"'))
	}
	async kill() {
		this.proc.kill('SIGKILL')
		await this.exited
	}
	async term() {
		this.proc.kill('SIGTERM')
		await this.exited
	}
}

export async function makeClient(brokers: string[]) {
	const c = new KafkaClient({ clientId: uniq('r3-harness'), brokers })
	await c.connect()
	return c
}
export async function createTopic(client: any, name: string, partitions: number) {
	await client.createTopics([{ name, numPartitions: partitions, replicationFactor: 1 }])
	const deadline = Date.now() + 20_000
	while (Date.now() < deadline) {
		const t = (await client.getMetadata([name])).topics.get(name)
		if (t && t.partitions.size === partitions && [...t.partitions.values()].every((p: any) => p.leaderId >= 0))
			return
		await sleep(100)
	}
	throw new Error('topic not ready')
}
/** A committed record of an output topic; values are JSON (numbers unless `V` says else). */
export interface CommittedRow<V = number> {
	key: string
	value: V | null
	partition: number
	offset: bigint
}

/** Everything committed on `topic` until it was quiet for `quietMs`. */
export async function readCommitted<V = number>(
	client: any,
	topic: string,
	quietMs = 2_000
): Promise<CommittedRow<V>[]> {
	const consumer = client.consumer({
		groupId: uniq('r3-reader'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		maxWaitMs: 100,
	})
	const out: any[] = []
	let last = Date.now()
	const run = consumer.runEach(topic, async (m: any) => {
		out.push({
			key: m.key.toString(),
			value: m.value === null ? null : JSON.parse(m.value.toString()),
			partition: m.partition,
			offset: m.offset,
		})
		last = Date.now()
	})
	run.catch(() => {})
	await sleep(800)
	while (Date.now() - last < quietMs) await sleep(100)
	consumer.stop()
	await run.catch(() => {})
	return out
}
