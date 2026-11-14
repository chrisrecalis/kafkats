import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, TimeWindows, type FlowApp } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { injectCommitFailures, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

describe('stream time across aborted transactions', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-st-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	// BUG (F1): StreamTimeTracker lives on the processor graph and is advanced by records of a
	// transaction that is later aborted. After the worker restarts and re-reads the same records,
	// the old record is judged "late" (its window is older than streamTime - 24 windows) and dropped,
	// so the committed output differs from a failure-free run.
	const scenario = async (inject: boolean) => {
		const input = uniqueTopicName('r2-st-in')
		const output = uniqueTopicName('r2-st-out')
		const applicationId = uniqueTopicName('r2-st-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const base = 1_700_000_000_000 // multiple of 1s
		await producer.send(input, [
			{ key: 'a', value: numberCodec.encode(1), timestamp: new Date(base) },
			{ key: 'a', value: numberCodec.encode(1), timestamp: new Date(base + 100_000) },
		])

		const client = newClient('r2-st-app')
		let failures = 0
		if (inject) injectCommitFailures(client, () => failures++ < 1)
		const app: FlowApp = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 1_000,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.windowedBy(TimeWindows.of(1_000))
			.count({ storeName: 'w', value: numberCodec })
			.toStream()
			.selectKey(
				(_v, k) =>
					`${(k as unknown as { key: string }).key}@${(k as unknown as { window: { start: number } }).window.start}`
			)
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			await sleep(9_000)
			const out = (await readAllCommitted(admin, output)).map(
				m => `${m.key!.toString()}=${numberCodec.decode(m.value!)}`
			)
			if (inject) expect(failures).toBeGreaterThanOrEqual(2) // failed once, then succeeded
			expect(out.sort()).toEqual([`a@${base}=1`, `a@${base + 100_000}=1`].sort())
		} finally {
			await app.close()
		}
	}

	it('control: failure-free pass', () => scenario(false), 60_000)
	it('exactly_once windowed count after an aborted commit equals a failure-free pass', () => scenario(true), 60_000)
})
