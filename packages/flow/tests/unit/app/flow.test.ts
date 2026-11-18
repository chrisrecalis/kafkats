import { describe, expect, it, vi } from 'vitest'
import { KafkaClient } from '@kafkats/client'
import { FlowConfigError, TopologyError } from '@/api/errors.js'
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

	it('start() builds the topology, then fails when the cluster cannot be reached; it may be retried', async () => {
		const unreachable = new KafkaClient(client)
		const connect = vi.spyOn(unreachable, 'connect').mockRejectedValue(new Error('unreachable'))
		const disconnect = vi.spyOn(unreachable, 'disconnect')
		const app = flow({ applicationId: 'app', client: unreachable })
		const stream = app.stream('in')
		await expect(app.start()).rejects.toThrow('unreachable')
		expect(app.state()).toBe('ERROR')
		expect(app.getError()?.message).toBe('unreachable')
		expect(() => stream.to('late')).toThrow(TopologyError)
		await expect(app.start()).rejects.toThrow('unreachable')
		expect(connect).toHaveBeenCalledTimes(2)
		await app.close()
		expect(app.state()).toBe('STOPPED')
		// The user's own client stays as it is.
		expect(disconnect).not.toHaveBeenCalled()
	})

	it('start() rejects with the TopologyError of an invalid topology', async () => {
		const app = flow({ applicationId: 'app', client })
		app.stream('users')
		app.globalTable('users', { key: codec.string(), value: codec.string() })
		await expect(app.start()).rejects.toThrow(TopologyError)
	})

	it('a DSL call that is wrong by itself throws at once; describe() throws for the rules spanning the topology', () => {
		const strings = { key: codec.string(), value: codec.string() }
		const app = flow({ applicationId: 'app', client })
		const keyed = app.stream('in', { value: codec.string() }).selectKey((_key, value) => value)
		expect(() => keyed.groupByKey().count()).toThrow(TopologyError) // no key codec for the repartition topic
		keyed.filter(() => true, { name: 'n' })
		expect(() => keyed.filter(() => true, { name: 'n' })).toThrow(TopologyError)
		expect(() => keyed.filter(() => true, { name: 'bad name!' })).toThrow(TopologyError)
		// A sink encodes with the codecs it has: values that are not Buffers fail each record, not the topology.
		keyed.mapValues(value => ({ value })).to('out')
		expect(() => app.describe()).not.toThrow()

		const conflicting = flow({ applicationId: 'app', client })
		conflicting.stream('users', strings).to('out', strings)
		conflicting.globalTable('users', strings) // a topic read by a stream and by a global table
		expect(() => conflicting.describe()).toThrow(TopologyError)
	})

	it('close() before start() marks the app STOPPED', async () => {
		const app = flow({ applicationId: 'app', client })
		await app.close()
		expect(app.state()).toBe('STOPPED')
		expect(app.getError()).toBeNull()
	})
})
