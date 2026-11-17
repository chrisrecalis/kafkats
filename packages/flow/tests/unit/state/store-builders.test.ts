import { describe, expect, it, vi } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { KeyValueStore } from '@/state.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import {
	definedStoreBuilder,
	keyValueStoreBuilder,
	sessionStoreBuilder,
	Stores,
	timestampedKeyValueStoreBuilder,
	timestampedSessionStoreBuilder,
	timestampedWindowStoreBuilder,
	windowStoreBuilder,
} from '@/state/store-builders.js'
import { DEFAULTS, str } from './support.js'

describe('store builders', () => {
	it('key-value: creates the provider store under the given name, logged by default', () => {
		const builder = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })
		expect(builder).toMatchObject({
			name: 'counts',
			kind: 'key-value',
			timestamped: false,
			keyCodec: str,
			valueCodec: str,
		})
		expect(builder.logging).toMatchObject({ enabled: true, topic: 'app-counts-changelog', skipRestoration: false })
		expect(builder.retentionMs).toBeUndefined()
		expect(builder.clock).toBeUndefined()

		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createKeyValueStore')
		expect(builder.create(provider, 'app.counts.p0').name).toBe('app.counts.p0')
		expect(create).toHaveBeenCalledWith('app.counts.p0', { keyCodec: str, valueCodec: str })
	})

	it('timestamped key-value: values are stored as [int64BE timestamp][value]', async () => {
		const builder = timestampedKeyValueStoreBuilder(DEFAULTS, { name: 'table', keyCodec: str, valueCodec: str })
		expect(builder).toMatchObject({ name: 'table', kind: 'key-value', timestamped: true })
		expect(builder.valueCodec.encode({ value: 'v', timestamp: 5 })).toEqual(
			Buffer.from([0, 0, 0, 0, 0, 0, 0, 5, 0x76])
		)

		const store = builder.create(new InMemoryStateStoreProvider(), 'app.table.p0')
		await store.put('k', { value: 'v', timestamp: 5 })
		expect(await store.get('k')).toEqual({ value: 'v', timestamp: 5 })
	})

	it('window: retention and size reach the provider; the changelog is delete,compact with the retention + 1 day', () => {
		const builder = windowStoreBuilder(DEFAULTS, {
			name: 'windows',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 240_000,
			windowSizeMs: 10_000,
		})
		expect(builder).toMatchObject({ kind: 'window', retentionMs: 240_000, windowSizeMs: 10_000 })
		expect(builder.logging.enabled && builder.logging.configs).toMatchObject({
			'cleanup.policy': 'delete,compact',
			'retention.ms': String(240_000 + 86_400_000),
		})

		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createWindowStore')
		builder.create(provider, 'app.windows.p1')
		expect(create).toHaveBeenCalledWith('app.windows.p1', {
			keyCodec: str,
			valueCodec: str,
			retentionMs: 240_000,
			windowSizeMs: 10_000,
		})
	})

	it('session: retention reaches the provider; the changelog is delete,compact with the retention + 1 day', () => {
		const builder = sessionStoreBuilder(DEFAULTS, {
			name: 'sessions',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 5_000,
		})
		expect(builder).toMatchObject({ kind: 'session', retentionMs: 5_000 })
		expect(builder.windowSizeMs).toBeUndefined()
		expect(builder.logging.enabled && builder.logging.configs['retention.ms']).toBe(String(5_000 + 86_400_000))

		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createSessionStore')
		builder.create(provider, 'app.sessions.p0')
		expect(create).toHaveBeenCalledWith('app.sessions.p0', { keyCodec: str, valueCodec: str, retentionMs: 5_000 })
	})

	it('timestamped window and session stores: [int64BE timestamp][value], with their retention and clock', async () => {
		const clock = { emitFinal: { kind: 'time', sizeMs: 10 } } as const
		const windows = timestampedWindowStoreBuilder(DEFAULTS, {
			name: 'windows',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 30,
			windowSizeMs: 10,
			clock,
		})
		expect(windows).toMatchObject({ kind: 'window', timestamped: true, retentionMs: 30, windowSizeMs: 10, clock })
		const sliding = timestampedSessionStoreBuilder(DEFAULTS, {
			name: 'sliding',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 20,
			clock: {},
		})
		expect(sliding).toMatchObject({ kind: 'session', timestamped: true, retentionMs: 20, clock: {} })

		const provider = new InMemoryStateStoreProvider()
		for (const builder of [windows, sliding]) {
			expect(builder.valueCodec.encode({ value: 'v', timestamp: 5 })).toEqual(
				Buffer.from([0, 0, 0, 0, 0, 0, 0, 5, 0x76])
			)
			const store = builder.create(provider, `app.${builder.name}.p0`)
			const key = { key: 'k', windowStart: 0, windowEnd: 10 }
			await store.put(key, { value: 'v', timestamp: 5 })
			expect(await store.get(key)).toEqual({ value: 'v', timestamp: 5 })
		}
	})

	it('logging: false or a disabled config leaves the store without changelog', () => {
		expect(
			keyValueStoreBuilder(DEFAULTS, { name: 'a', keyCodec: str, valueCodec: str, logging: false }).logging
		).toEqual({
			enabled: false,
		})
		const disabled = sessionStoreBuilder(DEFAULTS, {
			name: 'b',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 1,
			logging: { enabled: false },
		})
		expect(disabled.logging).toEqual({ enabled: false })
	})

	it('one builder creates a separate store per partition', async () => {
		const builder = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })
		const provider = new InMemoryStateStoreProvider()
		const p0 = builder.create(provider, 'app.counts.p0') as KeyValueStore<string, string>
		const p1 = builder.create(provider, 'app.counts.p1') as KeyValueStore<string, string>
		await p0.put('k', 'p0')
		expect(await p1.get('k')).toBeUndefined()
	})
})

describe('Stores: the store definitions of the Processor API (6.21)', () => {
	it('a definition is its name only; its builder is resolved against the defaults of the app registering it', () => {
		const definition = Stores.keyValueStore({ name: 'kv', key: str, value: str })
		expect(definition).toEqual({ name: 'kv' })
		expect(Object.isFrozen(definition)).toBe(true)
		const builder = definedStoreBuilder(definition, DEFAULTS)
		expect(builder).toMatchObject({
			name: 'kv',
			kind: 'key-value',
			timestamped: false,
			keyCodec: str,
			valueCodec: str,
		})
		expect(builder.logging).toMatchObject({ enabled: true, topic: 'app-kv-changelog' })
		expect(builder.clock).toBeUndefined()
		expect(definedStoreBuilder(definition, { applicationId: 'other' }).logging).toMatchObject({
			topic: 'other-kv-changelog',
		})
	})

	it('timestamped, window and session stores, with durations in ms or with a unit', () => {
		const timestamped = definedStoreBuilder(
			Stores.timestampedKeyValueStore({ name: 'ts', key: str, value: str }),
			DEFAULTS
		)
		expect(timestamped).toMatchObject({ kind: 'key-value', timestamped: true })
		expect(timestamped.valueCodec.encode({ value: 'v', timestamp: 1 })).toEqual(
			Buffer.from([0, 0, 0, 0, 0, 0, 0, 1, 0x76])
		)
		const windows = { key: str, value: str, retention: '1m', windowSize: 10_000 } as const
		expect(definedStoreBuilder(Stores.windowStore({ name: 'w', ...windows }), DEFAULTS)).toMatchObject({
			kind: 'window',
			timestamped: false,
			retentionMs: 60_000,
			windowSizeMs: 10_000,
			clock: undefined,
		})
		expect(definedStoreBuilder(Stores.timestampedWindowStore({ name: 'tw', ...windows }), DEFAULTS)).toMatchObject({
			kind: 'window',
			timestamped: true,
			retentionMs: 60_000,
		})
		const session = definedStoreBuilder(
			Stores.sessionStore({ name: 's', key: str, value: str, retention: '1h' }),
			DEFAULTS
		)
		expect(session).toMatchObject({ kind: 'session', timestamped: false, retentionMs: 3_600_000, clock: undefined })
		expect(session.logging.enabled && session.logging.configs['cleanup.policy']).toBe('delete,compact')
	})

	it('logging options: false, or a ChangelogConfig naming the topic', () => {
		const unlogged = Stores.keyValueStore({ name: 'a', key: str, value: str, logging: false })
		expect(definedStoreBuilder(unlogged, DEFAULTS).logging).toEqual({ enabled: false })
		const named = Stores.keyValueStore({
			name: 'b',
			key: str,
			value: str,
			logging: { topicName: 'b-log', replicationFactor: 3, topicConfigs: { 'min.insync.replicas': '2' } },
		})
		expect(definedStoreBuilder(named, DEFAULTS).logging).toMatchObject({
			enabled: true,
			topic: 'b-log',
			replicationFactor: 3,
			configs: { 'min.insync.replicas': '2' },
		})
	})

	it("validates durations as Kafka Streams' Stores does: not negative, the window size within the retention", () => {
		const options = { name: 'w', key: str, value: str }
		expect(() => Stores.windowStore({ ...options, retention: 10, windowSize: 11 })).toThrow(
			"The retention of window store 'w' (10 ms) must be at least its window size (11 ms)"
		)
		expect(() => Stores.windowStore({ ...options, retention: -1, windowSize: 0 })).toThrow(TopologyError)
		expect(() => Stores.sessionStore({ ...options, retention: '1x' as '1s' })).toThrow(TopologyError)
		expect(Stores.windowStore({ ...options, retention: 0, windowSize: 0 }).name).toBe('w')
	})

	it('only definitions made by Stores have a builder', () => {
		expect(() => definedStoreBuilder({ name: 'fake' }, DEFAULTS)).toThrow(
			new TopologyError("The store definition 'fake' was not made by Stores")
		)
	})
})
