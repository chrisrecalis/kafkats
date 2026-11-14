import { KafkaClient } from '@kafkats/client'
import { TimeWindows, codec, flow, type FlowApp, type FlowConfig, type KStream } from '@kafkats/flow'

import { uniqueName } from './utils.js'

/**
 * End-to-end throughput of @kafkats/flow topologies against a real broker.
 *
 * Each scenario preloads its input topic, starts one application and measures the time from
 * `start()` until the last record has passed through the whole topology (records per second of
 * input). The broker is taken from KAFKA_BROKERS.
 *
 *   KAFKA_BROKERS=localhost:9092 pnpm -C packages/benchmark bench:flow
 *   ... bench:flow -- --records 200000 --scenario count --guarantee exactly_once --runs 3
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
					.count({ storeName: 'windowed-counts', key: str, value: num })
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
		scenarios: arg('scenario')?.split(',') ?? SCENARIOS.map(s => s.name),
		guarantees: (arg('guarantee')?.split(',') as Guarantee[] | undefined) ?? ['at_least_once', 'exactly_once'],
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

async function runOnce(brokers: string[], scenario: Scenario, guarantee: Guarantee, options: Options): Promise<number> {
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

	const app = flow({
		applicationId: uniqueName(`flow-bench-${scenario.name}`),
		client: { brokers, clientId: 'flow-bench', logLevel: 'silent' },
		processingGuarantee: guarantee,
		consumer: { autoOffsetReset: 'earliest' },
	})

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

async function main(): Promise<void> {
	const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
	if (!brokers) throw new Error('Set KAFKA_BROKERS (for example localhost:9092)')
	const options = parseArgs(process.argv.slice(2))

	console.log(
		`flow benchmark: ${options.records} records, ${options.keys} keys, ${options.partitions} partitions, ` +
			`~${options.valueBytes} byte values, best of ${options.runs} runs\n`
	)
	const rows: Array<{ scenario: string; guarantee: string; 'records/s': string; runs: string }> = []
	for (const scenario of SCENARIOS.filter(s => options.scenarios.includes(s.name))) {
		for (const guarantee of options.guarantees) {
			const rates: number[] = []
			for (let run = 0; run < options.runs; run++) {
				rates.push(await runOnce(brokers, scenario, guarantee, options))
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
}

main().catch(err => {
	console.error(err)
	process.exit(1)
})
