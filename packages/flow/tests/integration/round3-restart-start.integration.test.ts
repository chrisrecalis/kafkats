/**
 * Round 3 / attack 1 (startup): an instance that is SIGKILLed leaves a dead member in the consumer group until its
 * session times out. A replacement instance's start() must wait for the join (JoinGroup blocks until the dead member's
 * session expires) instead of failing after the hard-coded 10s WORKER_START_TIMEOUT_MS.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { newClient, readAllCommitted, sleep } from './eos-test-util.js'

const here = path.dirname(fileURLToPath(import.meta.url))
const str = codec.string()

describe('round3: replacement instance after SIGKILL', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-rs-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	it('start() of a replacement succeeds while the SIGKILLed member (sessionTimeoutMs 20s) has not expired yet', async () => {
		const input = uniqueTopicName('r3-rs-in')
		const output = uniqueTopicName('r3-rs-out')
		const applicationId = uniqueTopicName('r3-rs-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: output }])
		const child = spawn(process.execPath, ['--import', 'tsx', path.join(here, 'round3-child-app.ts')], {
			cwd: path.resolve(here, '../..'),
			env: { ...process.env, R3_BROKERS: process.env.KAFKA_BROKERS!, R3_APP: applicationId, R3_IN: input, R3_OUT: output, R3_SESSION_MS: '20000' },
			stdio: ['ignore', 'pipe', 'inherit'],
		})
		let out = ''
		child.stdout.on('data', d => (out += d.toString()))
		const exited = new Promise<void>(res => child.on('exit', () => res()))
		const app = flow({
			applicationId,
			client: newClient('r3-rs-app'),
			processingGuarantee: 'at_least_once',
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100, sessionTimeoutMs: 20_000 },
			producer: { lingerMs: 0 },
		})
		app.stream(input, { key: str, value: str })
			.mapValues(v => (v === null ? null : `m:${v}`))
			.to(output, { key: str, value: str })
		try {
			const deadline = Date.now() + 30_000
			while (!out.includes('started') && Date.now() < deadline) await sleep(100)
			expect(out).toContain('started')
			child.kill('SIGKILL')
			await exited
			const t0 = Date.now()
			await app.start() // must not reject with "Consumer did not start within 10000ms"
			expect(Date.now() - t0).toBeGreaterThan(0)
			await producer.send(input, { key: 'a', value: Buffer.from('1') })
			const end = Date.now() + 15_000
			let rows = await readAllCommitted(admin, output, 1_000)
			while (rows.length < 1 && Date.now() < end) rows = await readAllCommitted(admin, output, 1_000)
			expect(rows.map(r => r.value!.toString())).toEqual(['m:1'])
		} finally {
			child.kill('SIGKILL')
			await app.close().catch(() => {})
		}
	}, 80_000)
})
