/**
 * A global table whose consumer falls behind records that are deleted before it reads them (WP2.5, spec 4.12 step 3,
 * CR10): its position turns out of range, the app commits what it processed, pauses stream processing, wipes the
 * table and loads it again from the new log start, then resumes. Stream records that arrive meanwhile are joined
 * with the reloaded table, never with a half-loaded one, and the app stays RUNNING.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { string, type Consumer, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowConfig } from '../../src/api/app.js'
import { flow, inMemory, type KeyValueStore, type StateStoreProvider } from '../../src/index.js'
import {
	collectUnhandledRejections,
	createTopics,
	newClient,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const strings = { key: string(), value: string() }
/** The global consumer fetches at most this much per partition, and buffers ten times as much. */
const MAX_BYTES_PER_PARTITION = 16 * 1024

let client: KafkaClient
let producer: Producer

/** Holds the calls that pass it while closed. */
class Valve {
	waiting = 0
	private opened: Promise<void> = Promise.resolve()
	private release = (): void => {}

	close(): void {
		this.opened = new Promise(resolve => (this.release = resolve))
	}

	open(): void {
		this.release()
		this.opened = Promise.resolve()
	}

	async pass(): Promise<void> {
		this.waiting++
		await this.opened
		this.waiting--
	}
}

/** In-memory stores; puts and deletes of the provider store `name` pass a valve first. */
function heldStores(name: string, valves: { put: Valve; delete: Valve }): StateStoreProvider {
	const inner = inMemory()
	return {
		name: 'held',
		createKeyValueStore<K, V>(
			storeName: string,
			options: Parameters<StateStoreProvider['createKeyValueStore']>[1]
		) {
			const store = inner.createKeyValueStore<K, V>(storeName, options as never)
			if (storeName !== name) return store
			const held: Partial<KeyValueStore<K, V>> = {
				put: async (key, value) => {
					await valves.put.pass()
					return store.put(key, value)
				},
				delete: async key => {
					await valves.delete.pass()
					return store.delete(key)
				},
			}
			return new Proxy(store, {
				get: (target, prop) => {
					const value = (prop in held ? held : target)[prop as keyof KeyValueStore<K, V>]
					return typeof value === 'function' && !(prop in held) ? value.bind(target) : value
				},
			})
		},
		createWindowStore: (storeName, options) => inner.createWindowStore(storeName, options),
		createSessionStore: (storeName, options) => inner.createSessionStore(storeName, options),
		close: () => inner.close(),
	}
}

/** What a read_committed consumer of `topic` saw so far. */
class Output {
	readonly values: string[] = []
	private readonly consumer: Consumer
	private running: Promise<void> | undefined

	constructor(private readonly topic: string) {
		this.consumer = client.consumer({
			groupId: uniqueTopicName('global-rebootstrap-reader'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
			maxWaitMs: 100,
		})
	}

	async start(): Promise<void> {
		const ready = new Promise<void>(resolve => this.consumer.once('running', () => resolve()))
		this.running = this.consumer.runEach(this.topic, async m => void this.values.push(m.value.toString()))
		await ready
	}

	async stop(): Promise<void> {
		this.consumer.stop()
		await this.running
	}
}

/** The offset the group `groupId` committed for partition 0 of `topic` (read with a consumer outside the group). */
async function committedOffset(groupId: string, topic: string): Promise<bigint | undefined> {
	const probe = client.consumer({ groupId })
	const ready = new Promise<void>(resolve => probe.once('running', () => resolve()))
	const running = probe.runBatch([topic], async () => {}, {
		assignment: [{ topic, partition: 0, offset: 0n }],
		autoCommit: false,
		commitOffsets: false,
	})
	try {
		await ready
		const committed = await probe.committed([{ topic, partition: 0 }])
		return committed.get(`${topic}:0`)?.offset
	} finally {
		probe.stop()
		await running
	}
}

async function produce(topic: string, records: readonly { key: string; value: string }[]): Promise<void> {
	await producer.send(
		topic,
		records.map(({ key, value }) => ({ key: Buffer.from(key), value: Buffer.from(value) }))
	)
}

const CONFIGS: readonly [string, Partial<FlowConfig>][] = [
	// No interval commit during the test: only the gate commits what was processed before the pause.
	['at_least_once', { processingGuarantee: 'at_least_once', commitIntervalMs: 60_000 }],
	['exactly_once', { processingGuarantee: 'exactly_once' }],
]

describe('global table re-bootstrap', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('global-rebootstrap')
		await client.connect()
		producer = client.producer()
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	it.each(CONFIGS)(
		'%s: an out-of-range global position reloads the table while stream processing waits',
		async (_name, config) => {
			const users = uniqueTopicName('global-rebootstrap-users')
			const input = uniqueTopicName('global-rebootstrap-in')
			const outputTopic = uniqueTopicName('global-rebootstrap-out')
			const applicationId = uniqueTopicName('global-rebootstrap')
			await createTopics(client, [{ name: users }, { name: input }, { name: outputTopic }])
			const output = new Output(outputTopic)
			await output.start()
			const valves = { put: new Valve(), delete: new Valve() }

			const app = flow({
				applicationId,
				client,
				consumer: { maxWaitMs: 100, maxBytesPerPartition: MAX_BYTES_PER_PARTITION },
				stateStoreProvider: heldStores(`${applicationId}.users`, valves),
				...config,
			})
			const table = app.globalTable(users, { ...strings, materialized: { storeName: 'users' } })
			app.stream(input, strings)
				.leftJoin(table, (value, user) => `${value}:${user}`)
				.to(outputTopic, strings)

			await produce(users, [{ key: 'k1', value: 'alice' }])
			await app.start()
			await produce(input, [{ key: 'k1', value: 'a' }])
			await waitUntil('a:alice', () => output.values.includes('a:alice'), 30_000)

			// The global consumer takes a batch of many records and is held in its first put, far behind the end.
			valves.put.close()
			const filler = 'x'.repeat(1024)
			for (let chunk = 0; chunk < 15; chunk++) {
				await produce(
					users,
					Array.from({ length: 100 }, (_, i) => ({ key: `g${chunk * 100 + i}`, value: filler }))
				)
			}
			await waitUntil('a held put', () => valves.put.waiting > 0, 30_000)

			// Everything up to the end is deleted before it is read; the topic goes on after it.
			const end = (await client.admin().fetchTopicOffsets(users, [0], 'latest')).get(0)!
			const [deleted] = await client.admin().deleteRecords([{ topic: users, partition: 0, offset: end }])
			expect(deleted?.error).toBeUndefined()
			await produce(users, [{ key: 'k1', value: 'bob' }])

			// The consumer goes on, meets the deleted range, and the table is wiped: hold the wipe.
			valves.delete.close()
			valves.put.open()
			await waitUntil('a held delete of the wipe', () => valves.delete.waiting > 0, 30_000)

			// The gate committed what was processed before processing paused.
			expect(await committedOffset(applicationId, input)).toBe(1n)
			await produce(input, [{ key: 'k1', value: 'b' }])
			await sleep(1500)
			expect(output.values).toEqual(['a:alice'])
			expect(app.state()).toBe('RUNNING')

			valves.delete.open()
			await waitUntil('b:bob', () => output.values.includes('b:bob'), 30_000)
			// The reloaded table has only what the topic has now.
			await produce(input, [{ key: 'g5', value: 'c' }])
			await waitUntil('c:null', () => output.values.includes('c:null'), 30_000)
			expect(output.values).toEqual(['a:alice', 'b:bob', 'c:null'])
			expect(app.state()).toBe('RUNNING')

			await app.close()
			await output.stop()
			expect(app.getError()).toBeNull()
			expect(app.state()).toBe('STOPPED')
		},
		120_000
	)
})
