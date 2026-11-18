import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, TimeWindows, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	eventTimeBase,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

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

	// BUG (F1): the late-record clock lived on the processor graph and was advanced by records of a transaction that was
	// later aborted, so the records re-read after the abort were judged against a clock ahead of the committed one. The
	// clock is now committed with the offsets and seeded from them (D5). The late record (99.7 s behind stream time, its
	// window closed 76 s ago) is dropped by the failure-free run and by the run whose first commit fails.
	const scenario = async (inject: boolean) => {
		const input = uniqueTopicName('r2-st-in')
		const output = uniqueTopicName('r2-st-out')
		const applicationId = uniqueTopicName('r2-st-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const base = eventTimeBase() // near the wall clock (retention), a multiple of 1s
		await producer.send(input, [
			{ key: 'a', value: numberCodec.encode(1), timestamp: new Date(base) },
			{ key: 'a', value: numberCodec.encode(1), timestamp: new Date(base + 100_000) },
			{ key: 'a', value: numberCodec.encode(1), timestamp: new Date(base + 300) },
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
			.windowedBy(TimeWindows.ofSizeAndGrace('1s', '23s'))
			.count({ storeName: 'w', value: numberCodec })
			.toStream()
			.selectKey(k => `${k!.key}@${k!.window.start}`)
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			await sleep(9_000)
			const out = (await readAllCommitted(admin, output)).map(
				m => `${m.key!.toString()}=${numberCodec.decode(m.value!)}`
			)
			if (inject) expect(failures).toBeGreaterThanOrEqual(2) // failed once, then succeeded
			// Both on-time records counted once; the late one dropped (no `a@${base}=2`).
			expect(out.sort()).toEqual([`a@${base}=1`, `a@${base + 100_000}=1`].sort())
		} finally {
			await app.close()
		}
	}

	it('control: failure-free pass', () => scenario(false), 60_000)
	it('exactly_once windowed count after an aborted commit equals a failure-free pass', () => scenario(true), 60_000)
})
