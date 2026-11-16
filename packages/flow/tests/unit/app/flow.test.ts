import { describe, expect, it } from 'vitest'
import { FlowConfigError, StreamsError, TopologyError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import { topic } from '@/topic.js'

const client = { clientId: 'app', brokers: ['localhost:9092'] }

describe('flow()', () => {
	it('validates the config when the app is created', () => {
		expect(() => flow({ applicationId: 'app', client, numStreamThreads: 2 } as never)).toThrow(FlowConfigError)
	})

	it('gives the TestDriver the resolved settings, the provider, a logger and the topology', () => {
		const app = flow({ applicationId: 'app', client, processingGuarantee: 'exactly_once' })
		app.stream('in').to('out')
		const parts = internals(app)
		expect(parts.settings).toMatchObject({ applicationId: 'app', eos: true })
		expect(parts.stateStoreProvider.name).toBeTypeOf('string')
		expect(parts.logger).toBeDefined()
		const topology = parts.build()
		expect(parts.build()).toBe(topology)
		expect([...topology.groupTopics]).toEqual(['in'])
		expect(() => internals({} as never)).toThrow('not an app made by flow()')
	})

	it('reads codecs from a Topic; the options override them', () => {
		const app = flow({ applicationId: 'app', client })
		const users = topic('users', { key: codec.string(), value: codec.json<{ name: string }>() })
		app.stream(users)
		app.stream('other', { value: codec.string() })
		const sources = internals(app)
			.build()
			.subtopologies.flatMap(s => [...s.sourcesByTopic.values()].flat())
		expect(sources.map(s => [s.topics[0], s.keyCodec, s.valueCodec])).toEqual([
			['users', users.key, users.value],
			['other', expect.anything(), expect.anything()],
		])
	})

	it('start() builds the topology, then fails: this build has no Kafka runtime', async () => {
		const app = flow({ applicationId: 'app', client })
		const stream = app.stream('in')
		const starting = app.start()
		await expect(starting).rejects.toThrow(StreamsError)
		await expect(starting).rejects.toThrow('not available in this build')
		expect(app.state()).toBe('ERROR')
		expect(app.getError()).toBeInstanceOf(StreamsError)
		expect(() => stream.to('late')).toThrow(TopologyError)
		await app.close()
		expect(app.state()).toBe('STOPPED')
	})

	it('start() rejects with the TopologyError of an invalid topology', async () => {
		const app = flow({ applicationId: 'app', client })
		app.stream('users')
		app.globalTable('users', { key: codec.string(), value: codec.string() })
		await expect(app.start()).rejects.toThrow(TopologyError)
	})

	it('close() before start() marks the app STOPPED', async () => {
		const app = flow({ applicationId: 'app', client })
		await app.close()
		expect(app.state()).toBe('STOPPED')
		expect(app.getError()).toBeNull()
	})
})
