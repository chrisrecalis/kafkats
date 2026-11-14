/**
 * Round 3 / attack 1+8: restoration while a (crashed instance's) transaction is still open on the changelog partition.
 *
 * docs/flow/fault-tolerance.md says: "Restoration reads to the end of the changelog. If an instance crashed with a
 * transaction open, the restore waits until the broker aborts that transaction ... instead of stopping at an older
 * position". The restorer instead stops at the last stable offset (changelog.ts: fetchTopicOffsets(..., 'latest',
 * { isolationLevel: 'read_committed' }) is the LSO, i.e. the start of the open transaction), so everything that was
 * committed AFTER the open transaction is missing from the restored state while the input offsets are past it.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()

describe('round3: restore with an open transaction on the changelog', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-ot-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	it('a successor restoring after a rebalance sees everything committed after the dead instance\'s open transaction', async () => {
		const input = uniqueTopicName('r3-ot-in')
		const output = uniqueTopicName('r3-ot-out')
		const applicationId = uniqueTopicName('r3-ot-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const mk = (): FlowApp => {
			const app = flow({
				applicationId,
				client: newClient('r3-ot-app'),
				processingGuarantee: 'exactly_once',
				commitIntervalMs: 100,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
				changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 3_000, checkIntervalMs: 50 } },
			})
			app.stream(input, { key: str, value: str })
				.groupByKey()
				.count({ storeName: 'counts', key: str, value: num })
				.toStream()
				.to(output, { key: str, value: num })
			return app
		}
		const send = (n: number) =>
			producer.send(
				input,
				Array.from({ length: n }, () => ({ key: 'k', value: Buffer.from('x') }))
			)
		const vals = async () => (await readAllCommitted(admin, output, 1_200)).map(r => num.decode(r.value!))

		const b = mk()
		await b.start()
		await send(3)
		for (let i = 0; i < 30 && (await vals()).length < 3; i++) await sleep(300)
		expect(await vals()).toEqual([1, 2, 3])

		// a "crashed instance" leaves a transaction open on changelog partition 0 (the broker aborts it after 12s)
		const changelog = `${applicationId}-counts-changelog`
		const rogueClient = newClient('r3-ot-rogue')
		await rogueClient.connect()
		const rogue = rogueClient.producer({ transactionalId: `rogue-${applicationId}`, transactionTimeoutMs: 12_000, lingerMs: 0 })
		let release: () => void = () => {}
		const hold = new Promise<void>(res => (release = res))
		const rogueTx = rogue
			.transaction(async tx => {
				await tx.send(changelog, { key: Buffer.from('zz'), value: Buffer.from('99'), partition: 0 })
				await hold
			})
			.catch(() => {})
		try {
			await sleep(500)
			// B keeps committing (its changelog records now sit behind the open transaction)
			await send(3)
			for (let i = 0; i < 30 && (await vals()).length < 6; i++) await sleep(300)
			expect(await vals()).toEqual([1, 2, 3, 4, 5, 6])
			await b.close()

			// C takes over (fresh in-memory state => it must restore from the changelog)
			const c = mk()
			try {
				await c.start()
				await send(1)
				let seen: number[] = []
				for (let i = 0; i < 60 && seen.length < 7; i++) {
					await sleep(500)
					seen = await vals()
				}
				expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7])
			} finally {
				await c.close().catch(() => {})
			}
		} finally {
			release()
			await rogueTx
			await rogue.disconnect().catch(() => {})
			await rogueClient.disconnect().catch(() => {})
		}
	}, 85_000)
})
