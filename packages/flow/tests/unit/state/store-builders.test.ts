import { describe, expect, it, vi } from 'vitest'
import type { KeyValueStore } from '@/state.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import {
	keyValueStoreBuilder,
	sessionStoreBuilder,
	timestampedKeyValueStoreBuilder,
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

	it('window: retention and size reach the provider; the changelog is delete,compact with the retention', () => {
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
			'retention.ms': '240000',
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

	it('session: retention reaches the provider; the changelog is delete,compact with the retention', () => {
		const builder = sessionStoreBuilder(DEFAULTS, {
			name: 'sessions',
			keyCodec: str,
			valueCodec: str,
			retentionMs: 5_000,
		})
		expect(builder).toMatchObject({ kind: 'session', retentionMs: 5_000 })
		expect(builder.windowSizeMs).toBeUndefined()
		expect(builder.logging.enabled && builder.logging.configs['retention.ms']).toBe('5000')

		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createSessionStore')
		builder.create(provider, 'app.sessions.p0')
		expect(create).toHaveBeenCalledWith('app.sessions.p0', { keyCodec: str, valueCodec: str, retentionMs: 5_000 })
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
