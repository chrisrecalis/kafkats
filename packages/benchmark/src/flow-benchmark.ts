import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { KafkaClient } from '@kafkats/client'
import { SessionWindows, TimeWindows, codec, flow, type FlowApp, type FlowConfig, type KStream } from '@kafkats/flow'
import { lmdb } from '@kafkats/flow-state-lmdb'
import { rocksdb } from '@kafkats/flow-state-rocksdb'

import { uniqueName } from './utils.js'

/**
 * End-to-end throughput of @kafkats/flow topologies against a real broker.
 *
 * Each scenario preloads its input topic, starts one application and measures the time from
 * `start()` until the last record has passed through the whole topology (records per second of
 * input). The `restore` scenario instead times how fast a fresh instance restores the state of
 * `count` from its changelog. The broker is taken from KAFKA_BROKERS.
 *
 *   KAFKA_BROKERS=localhost:9092 pnpm -C packages/benchmark bench:flow
 *   ... bench:flow -- --records 200000 --scenario count,restore --guarantee exactly_once --runs 3
 *   ... bench:flow -- --scenario count,session-count --provider lmdb   (or rocksdb)
 */

type Guarantee = NonNullable<FlowConfig['processingGuarantee']>

interface Options {
	records: number
	keys: number
	partitions: number
	valueBytes: number
	runs: number
	scenarios: string[]
	guarantees: Guarantee[]
	/** State store provider: 'memory' (the default provider), 'lmdb' or 'rocksdb'. */
	provider: 'memory' | 'lmdb' | 'rocksdb'
}

interface Event {
	user: string
	amount: number
	pad: string
}

interface Topics {
	input: string
	table: string
	output: string
}

interface Scenario {
	name: string
	/** Builds the topology; `done` must be called once per record that reaches the end of it. */
	build(app: FlowApp, topics: Topics, done: () => void): void
	/** Whether the scenario reads a lookup table that must be loaded first. */
	usesTable?: boolean
}

const str = codec.string()
const json = <T>() => codec.json<T>()
const event = json<Event>()
const num = json<number>()

const SCENARIOS: Scenario[] = [
	{
		name: 'stateless',
		build(app, t, done) {
			sink(
				app
					.stream(t.input, { key: str, value: event })
					.filter((_, v) => v !== null && v.amount >= 0)
					.mapValues(v => ({ ...v!, amount: v!.amount * 2 })),
				t.output,
				event,
				done
			)
		},
	},
	{
		name: 'count',
		build(app, t, done) {
			sink(
				app
					.stream(t.input, { key: str, value: event })
					.groupByKey()
					.count({ storeName: 'counts', key: str, value: num })
					.toStream(),
				t.output,
				num,
				done
			)
		},
	},
	{
		name: 'windowed-count',
		build(app, t, done) {
			sink(
				app
					.stream(t.input, { key: str, value: event })
					.groupByKey()
					.windowedBy(TimeWindows.of('1m'))
					.count({ storeName: 'windowed-counts', value: num })
					.toStream()
					.map(
						(windowed, count) =>
							[`${windowed!.key}@${windowed!.window.start}`, count] as [string, number | null]
					),
				t.output,
				num,
				done
			)
		},
	},
	{
		name: 'session-count',
		build(app, t, done) {
			sink(
				app
					.stream(t.input, { key: str, value: event })
					.groupByKey()
					.windowedBy(SessionWindows.withInactivityGap('1m'))
					.count({ storeName: 'session-counts', value: num })
					.toStream()
					// Merging a record into a session retracts the previous session (a null value).
					.filter((_, count) => count !== null)
					.map(
						(windowed, count) =>
							[`${windowed!.key}@${windowed!.window.start}`, count] as [string, number | null]
					),
				t.output,
				num,
				done
			)
		},
	},
	{
		name: 'table-join',
		usesTable: true,
		build(app, t, done) {
			const users = app.table(t.table, { key: str, value: str })
			sink(
				app.stream(t.input, { key: str, value: event }).join(users, (e, name) => `${name}:${e.amount}`),
				t.output,
				str,
				done
			)
		},
	},
	{
		name: 'global-table-join',
		usesTable: true,
		build(app, t, done) {
			const users = app.globalTable(t.table, { key: str, value: str })
			sink(
				app.stream(t.input, { key: str, value: event }).join(users, (e, name) => `${name}:${e.amount}`),
				t.output,
				str,
				done
			)
		},
	},
	{
		name: 'repartition-count',
		build(app, t, done) {
			sink(
				app
					.stream(t.input, { key: str, value: event })
					.groupBy((_, v) => `bucket-${v!.amount % 100}`, { key: str, value: event })
					.count({ storeName: 'bucket-counts', key: str, value: num })
					.toStream(),
				t.output,
				num,
				done
			)
		},
	},
]

function sink<V>(
	stream: KStream<string, V>,
	topic: string,
	value: ReturnType<typeof json<V>> | typeof str,
	done: () => void
): void {
	stream.peek(done).to(topic, { key: str, value: value as never })
}

function parseArgs(argv: string[]): Options {
	const arg = (name: string): string | undefined => {
		const index = argv.indexOf(`--${name}`)
		return index === -1 ? undefined : argv[index + 1]
	}
	return {
		records: Number(arg('records') ?? 200_000),
		keys: Number(arg('keys') ?? 1_000),
		partitions: Number(arg('partitions') ?? 4),
		valueBytes: Number(arg('value-bytes') ?? 100),
		runs: Number(arg('runs') ?? 3),
		scenarios: arg('scenario')?.split(',') ?? [...SCENARIOS.map(s => s.name), 'restore'],
		guarantees: (arg('guarantee')?.split(',') as Guarantee[] | undefined) ?? ['at_least_once', 'exactly_once'],
		provider: (['lmdb', 'rocksdb'] as const).find(p => p === arg('provider')) ?? 'memory',
	}
}

async function createTopics(client: KafkaClient, names: string[], partitions: number): Promise<void> {
	await client.createTopics(names.map(name => ({ name, numPartitions: partitions, replicationFactor: 1 })))
	for (;;) {
		const metadata = await client.getMetadata(names)
		const ready = names.every(name => {
			const topic = metadata.topics.get(name)
			return topic?.partitions.size === partitions && [...topic.partitions.values()].every(p => p.leaderId >= 0)
		})
		if (ready) return
		await new Promise(resolve => setTimeout(resolve, 100))
	}
}

async function preload(client: KafkaClient, topics: Topics, options: Options, usesTable: boolean): Promise<void> {
	const producer = client.producer({ lingerMs: 5 })
	if (usesTable) {
		const rows = Array.from({ length: options.keys }, (_, k) => ({
			key: `user-${k}`,
			value: str.encode(`name-${k}`),
		}))
		await producer.send(topics.table, rows)
	}
	const pad = 'x'.repeat(Math.max(0, options.valueBytes - 40))
	const batchSize = 5_000
	for (let offset = 0; offset < options.records; offset += batchSize) {
		const batch = Array.from({ length: Math.min(batchSize, options.records - offset) }, (_, i) => {
			const n = offset + i
			const user = `user-${n % options.keys}`
			return { key: user, value: event.encode({ user, amount: n, pad }) }
		})
		await producer.send(topics.input, batch)
	}
	await producer.disconnect()
}

async function setUp(brokers: string[], scenario: Scenario, options: Options): Promise<Topics> {
	const admin = new KafkaClient({ brokers, clientId: 'flow-bench-setup', logLevel: 'silent' })
	await admin.connect()
	const topics: Topics = {
		input: uniqueName('flow-bench-in'),
		table: uniqueName('flow-bench-table'),
		output: uniqueName('flow-bench-out'),
	}
	await createTopics(admin, Object.values(topics), options.partitions)
	await preload(admin, topics, options, scenario.usesTable === true)
	await admin.disconnect()
	return topics
}

/** State directories of LMDB runs, removed when the benchmark ends. */
const stateDirs: string[] = []

function createApp(brokers: string[], applicationId: string, guarantee: Guarantee, options: Options): FlowApp {
	const stateDir = options.provider === 'memory' ? undefined : mkdtempSync(join(tmpdir(), 'flow-bench-'))
	if (stateDir) stateDirs.push(stateDir)
	return flow({
		applicationId,
		client: { brokers, clientId: 'flow-bench', logLevel: 'silent' },
		processingGuarantee: guarantee,
		consumer: { autoOffsetReset: 'earliest' },
		...(stateDir
			? { stateStoreProvider: options.provider === 'rocksdb' ? rocksdb({ stateDir }) : lmdb({ stateDir }) }
			: {}),
	})
}

/**
 * Deletes the topics of a run and the internal topics of its application: a full benchmark writes gigabytes, and a
 * broker's startup metadata grows with every topic left behind.
 */
async function tearDown(brokers: string[], topics: Topics, applicationId: string): Promise<void> {
	const client = new KafkaClient({ brokers, clientId: 'flow-bench-setup', logLevel: 'silent' })
	await client.connect()
	const admin = client.admin()
	const internal = (await admin.listTopics()).filter(name => name.startsWith(`${applicationId}-`))
	await admin.deleteTopics([...Object.values(topics), ...internal])
	await client.disconnect()
}

async function runOnce(brokers: string[], scenario: Scenario, guarantee: Guarantee, options: Options): Promise<number> {
	const topics = await setUp(brokers, scenario, options)
	const applicationId = uniqueName(`flow-bench-${scenario.name}`)
	const rate = await processAll(createApp(brokers, applicationId, guarantee, options), scenario, topics, options)
	await tearDown(brokers, topics, applicationId)
	return rate
}

/** Starts `app` and resolves with records/s once every input record has passed through it; closes it. */
async function processAll(app: FlowApp, scenario: Scenario, topics: Topics, options: Options): Promise<number> {
	let processed = 0
	let finished!: () => void
	const allProcessed = new Promise<void>(resolve => {
		finished = resolve
	})
	scenario.build(app, topics, () => {
		if (++processed === options.records) finished()
	})

	await app.start()
	const startedAt = performance.now()
	await allProcessed
	const elapsedMs = performance.now() - startedAt
	await app.close()
	return (options.records / elapsedMs) * 1000
}

/**
 * Restoration throughput: builds the `count` state (one changelog record per input record), closes
 * the app, then starts a fresh instance with empty in-memory state and times `start()`, which
 * returns once the state is restored from the changelog. Changelog records per second.
 */
async function restoreOnce(brokers: string[], guarantee: Guarantee, options: Options): Promise<number> {
	const count = SCENARIOS.find(s => s.name === 'count')!
	const topics = await setUp(brokers, count, options)
	const applicationId = uniqueName('flow-bench-restore')
	await processAll(createApp(brokers, applicationId, guarantee, options), count, topics, options)

	const app = createApp(brokers, applicationId, guarantee, options)
	count.build(app, topics, () => {})
	const startedAt = performance.now()
	await app.start()
	const elapsedMs = performance.now() - startedAt
	await app.close()

	const admin = new KafkaClient({ brokers, clientId: 'flow-bench-setup', logLevel: 'silent' })
	await admin.connect()
	const partitions = Array.from({ length: options.partitions }, (_, p) => p)
	const changelog = `${applicationId}-counts-changelog`
	const [earliest, latest] = await Promise.all([
		admin.admin().fetchTopicOffsets(changelog, partitions, 'earliest'),
		admin.admin().fetchTopicOffsets(changelog, partitions, 'latest'),
	])
	await admin.disconnect()
	await tearDown(brokers, topics, applicationId)
	// Includes the transaction markers of an exactly_once changelog (one per commit, a small share).
	let records = 0
	for (const p of partitions) records += Number(latest.get(p)! - earliest.get(p)!)
	return (records / elapsedMs) * 1000
}

async function main(): Promise<void> {
	const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
	if (!brokers) throw new Error('Set KAFKA_BROKERS (for example localhost:9092)')
	const options = parseArgs(process.argv.slice(2))

	console.log(
		`flow benchmark: ${options.records} records, ${options.keys} keys, ${options.partitions} partitions, ` +
			`~${options.valueBytes} byte values, ${options.provider} state, best of ${options.runs} runs\n`
	)
	const rows: Array<{ scenario: string; guarantee: string; 'records/s': string; runs: string }> = []
	const measurements = SCENARIOS.filter(s => options.scenarios.includes(s.name)).map(scenario => ({
		name: scenario.name,
		once: (guarantee: Guarantee) => runOnce(brokers, scenario, guarantee, options),
	}))
	if (options.scenarios.includes('restore')) {
		measurements.push({ name: 'restore', once: guarantee => restoreOnce(brokers, guarantee, options) })
	}
	for (const scenario of measurements) {
		for (const guarantee of options.guarantees) {
			const rates: number[] = []
			for (let run = 0; run < options.runs; run++) {
				rates.push(await scenario.once(guarantee))
			}
			rows.push({
				scenario: scenario.name,
				guarantee,
				'records/s': Math.round(Math.max(...rates)).toLocaleString('en-US'),
				runs: rates.map(r => Math.round(r)).join(' / '),
			})
			console.log(`${scenario.name} ${guarantee}: ${rates.map(r => Math.round(r)).join(' / ')} records/s`)
		}
	}
	console.log()
	console.table(rows)
	for (const dir of stateDirs) rmSync(dir, { recursive: true, force: true })
}

main().catch(err => {
	console.error(err)
	process.exit(1)
})
