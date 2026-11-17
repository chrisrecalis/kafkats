import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
	DEFAULT_CHANGELOG_CONFIGS,
	DEFAULT_REPARTITION_CONFIGS,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
	resolveStoreChangelog,
	WINDOWED_CHANGELOG_ADDITIONAL_RETENTION_MS,
} from '@/changelog/config.js'
import { ChangelogPartitionMismatchError, SourceTopicNotFoundError } from '@/changelog/errors.js'
import { ChangelogWriter, windowedKeyCodec } from '@/changelog/writer.js'
import { codec } from '@/codec.js'
import type { WindowedKey } from '@/state.js'
import {
	ChangelogBackedKeyValueStore,
	ChangelogBackedSessionStore,
	ChangelogBackedWindowStore,
} from '@/state/logging.js'
import { InMemoryKeyValueStore, InMemorySessionStore, InMemoryWindowStore } from '@/state/memory.js'
import { RecordingOutput, collect, str } from './support.js'

const windowed = windowedKeyCodec(str)
const w = (key: string, windowStart: number, windowEnd: number): WindowedKey<string> => ({
	key,
	windowStart,
	windowEnd,
})

describe('changelog', () => {
	describe('buildChangelogTopicName', () => {
		it('builds changelog topic name with appId and storeName', () => {
			expect(buildChangelogTopicName('my-app', 'my-store')).toBe('my-app-my-store-changelog')
		})

		it('handles special characters in names', () => {
			expect(buildChangelogTopicName('app-v1', 'user_counts')).toBe('app-v1-user_counts-changelog')
		})
	})

	describe('topic configs', () => {
		it('key-value changelogs are compacted, kept forever, with 50MB segments', () => {
			expect(DEFAULT_CHANGELOG_CONFIGS).toEqual({
				'cleanup.policy': 'compact',
				'retention.ms': '-1',
				'segment.bytes': '52428800',
			})
		})

		it('repartition topics are deleted, not compacted, and not expired by time (Kafka Streams)', () => {
			expect(DEFAULT_REPARTITION_CONFIGS).toEqual({
				'cleanup.policy': 'delete',
				'retention.ms': '-1',
				'segment.bytes': '52428800',
			})
		})

		it('getDefaultTopicConfigs merges overrides with the defaults', () => {
			expect(getDefaultTopicConfigs()).toEqual(DEFAULT_CHANGELOG_CONFIGS)
			expect(getDefaultTopicConfigs({ 'retention.ms': '86400000', 'min.insync.replicas': '2' })).toEqual({
				'cleanup.policy': 'compact',
				'retention.ms': '86400000',
				'segment.bytes': '52428800',
				'min.insync.replicas': '2',
			})
			expect(getDefaultTopicConfigs({ 'cleanup.policy': 'delete' })['cleanup.policy']).toBe('delete')
		})
	})

	describe('resolveChangelogConfig', () => {
		it('false disables, true and undefined enable', () => {
			expect(resolveChangelogConfig(false).enabled).toBe(false)
			expect(resolveChangelogConfig(undefined).enabled).toBe(true)
			expect(resolveChangelogConfig(true).enabled).toBe(true)
		})

		it('keeps the properties of a config object, enabled unless it says otherwise', () => {
			expect(resolveChangelogConfig({ topicName: 'custom-changelog', replicationFactor: 3 })).toEqual({
				enabled: true,
				topicName: 'custom-changelog',
				replicationFactor: 3,
			})
			expect(resolveChangelogConfig({ enabled: false }).enabled).toBe(false)
		})
	})

	describe('resolveStoreChangelog', () => {
		const defaults = { applicationId: 'app' }

		it('logs a key-value store to <appId>-<store>-changelog with the defaults', () => {
			expect(resolveStoreChangelog(defaults, { name: 'counts' }, undefined)).toEqual({
				enabled: true,
				topic: 'app-counts-changelog',
				configs: DEFAULT_CHANGELOG_CONFIGS,
				replicationFactor: undefined,
				skipRestoration: false,
			})
		})

		it('window and session changelogs are delete,compact with the store retention plus a day (Kafka Streams)', () => {
			expect(WINDOWED_CHANGELOG_ADDITIONAL_RETENTION_MS).toBe(86_400_000)
			const resolved = resolveStoreChangelog(defaults, { name: 'windows', retentionMs: 240_000 }, true)
			expect(resolved.enabled && resolved.configs).toEqual({
				'cleanup.policy': 'delete,compact',
				'retention.ms': String(240_000 + 86_400_000),
				'segment.bytes': '52428800',
			})
		})

		it("applies the application's topic configs over the defaults and the store's own over both", () => {
			const resolved = resolveStoreChangelog(
				{ applicationId: 'app', topicConfigs: { 'retention.ms': '1000', 'min.insync.replicas': '2' } },
				{ name: 'windows', retentionMs: 240_000 },
				{ topicConfigs: { 'min.insync.replicas': '3' } }
			)
			expect(resolved.enabled && resolved.configs).toEqual({
				'cleanup.policy': 'delete,compact',
				'retention.ms': '1000',
				'segment.bytes': '52428800',
				'min.insync.replicas': '3',
			})
		})

		it('takes the topic name, replication factor and skipRestoration of the store config', () => {
			expect(
				resolveStoreChangelog(
					defaults,
					{ name: 'counts' },
					{
						topicName: 'custom',
						replicationFactor: 3,
						skipRestoration: true,
					}
				)
			).toMatchObject({ enabled: true, topic: 'custom', replicationFactor: 3, skipRestoration: true })
		})

		it('a disabled changelog resolves to { enabled: false }', () => {
			expect(resolveStoreChangelog(defaults, { name: 'counts' }, false)).toEqual({ enabled: false })
			expect(resolveStoreChangelog(defaults, { name: 'counts' }, { enabled: false })).toEqual({ enabled: false })
		})
	})

	describe('windowedKeyCodec', () => {
		it('encodes [key][start int64BE][end int64BE] and decodes it back', () => {
			const encoded = windowed.encode(w('test-key', 1_000_000, 2_000_000))
			expect(encoded.subarray(0, 8).toString()).toBe('test-key')
			expect(encoded.readBigInt64BE(8)).toBe(1_000_000n)
			expect(encoded.readBigInt64BE(16)).toBe(2_000_000n)
			expect(windowed.decode(encoded)).toEqual(w('test-key', 1_000_000, 2_000_000))
		})

		it('handles epoch-millisecond and negative times', () => {
			const now = Date.now()
			expect(windowed.decode(windowed.encode(w('key', now, now + 3_600_000)))).toEqual(
				w('key', now, now + 3_600_000)
			)
			expect(windowed.decode(windowed.encode(w('key', -500, -400)))).toEqual(w('key', -500, -400))
		})
	})

	describe('ChangelogWriter', () => {
		it('sends changelog records to the partition of its task, without a timestamp', () => {
			const output = new RecordingOutput()
			const writer = new ChangelogWriter('test-changelog', 3, str, codec.json<number>(), output)

			writer.write('key1', 42)
			writer.writeTombstone('key-to-delete')

			expect(output.sent).toEqual([
				{
					kind: 'changelog',
					record: {
						topic: 'test-changelog',
						partition: 3,
						key: Buffer.from('key1'),
						value: Buffer.from('42'),
					},
				},
				{
					kind: 'changelog',
					record: { topic: 'test-changelog', partition: 3, key: Buffer.from('key-to-delete'), value: null },
				},
			])
		})
	})

	describe('ChangelogBackedKeyValueStore', () => {
		let inner: InMemoryKeyValueStore<string, string>
		let output: RecordingOutput
		let store: ChangelogBackedKeyValueStore<string, string>

		beforeEach(() => {
			inner = new InMemoryKeyValueStore('test-store', { keyCodec: str, valueCodec: str })
			output = new RecordingOutput()
			store = new ChangelogBackedKeyValueStore(inner, new ChangelogWriter('cl', 0, str, str, output))
		})

		it('reads from the store below', async () => {
			await inner.put('a', '1')
			await inner.put('b', '2')
			expect(await store.get('a')).toBe('1')
			expect(await collect(store.all())).toHaveLength(2)
			expect(await collect(store.range('a', 'a'))).toEqual([['a', '1']])
			expect(await store.approximateNumEntries()).toBe(2)
			expect(store.name).toBe('test-store')
		})

		it('writes the store below and the changelog on put and delete', async () => {
			await store.put('key1', 'v')
			expect(await inner.get('key1')).toBe('v')
			await store.delete('key1')
			expect(await inner.get('key1')).toBeUndefined()
			expect(output.changelog()).toEqual([
				{ topic: 'cl', partition: 0, key: 'key1', value: 'v' },
				{ topic: 'cl', partition: 0, key: 'key1', value: null },
			])
		})

		it('writes the store below BEFORE the changelog (local first)', async () => {
			const order: string[] = []
			const put = inner.put.bind(inner)
			vi.spyOn(inner, 'put').mockImplementation(async (key, value) => {
				await put(key, value)
				order.push('inner.put')
			})
			const del = inner.delete.bind(inner)
			vi.spyOn(inner, 'delete').mockImplementation(async key => {
				await del(key)
				order.push('inner.delete')
			})
			const send = output.send.bind(output)
			vi.spyOn(output, 'send').mockImplementation((kind, record) => {
				order.push(record.value === null ? 'tombstone' : 'changelog')
				send(kind, record)
			})

			await store.put('k', '1')
			await store.delete('k')

			expect(order).toEqual(['inner.put', 'changelog', 'inner.delete', 'tombstone'])
		})

		it('logs nothing when the store below fails', async () => {
			vi.spyOn(inner, 'put').mockRejectedValue(new Error('disk full'))
			await expect(store.put('k', '1')).rejects.toThrow('disk full')
			expect(output.sent).toEqual([])
		})
	})

	describe('ChangelogBackedWindowStore', () => {
		it('logs puts and deletes with windowed keys, local first', async () => {
			const inner = new InMemoryWindowStore<string, string>('w', {
				keyCodec: str,
				valueCodec: str,
				windowSizeMs: 1000,
				retentionMs: 60_000,
			})
			const output = new RecordingOutput()
			const store = new ChangelogBackedWindowStore(inner, new ChangelogWriter('cl', 1, windowed, str, output))
			const order: string[] = []
			const put = inner.put.bind(inner)
			vi.spyOn(inner, 'put').mockImplementation(async (key, value) => {
				await put(key, value)
				order.push('inner.put')
			})
			vi.spyOn(output, 'send').mockImplementation(() => void order.push('changelog'))

			await store.put(w('k', 0, 1000), '1')

			expect(order).toEqual(['inner.put', 'changelog'])
			expect(await collect(store.fetch('k', 0, 0))).toEqual([[w('k', 0, 1000), '1']])
		})

		it('KF17: window expiry writes no tombstones (the changelog retention drops expired windows)', async () => {
			const inner = new InMemoryWindowStore<string, string>('w', {
				keyCodec: str,
				valueCodec: str,
				windowSizeMs: 1000,
				retentionMs: 1000,
			})
			const output = new RecordingOutput()
			const store = new ChangelogBackedWindowStore(inner, new ChangelogWriter('cl', 0, windowed, str, output))
			await store.put(w('k', 0, 1000), '1')
			expect(await store.expireOldWindows(10_000)).toBe(1)
			expect(output.changelog()).toEqual([{ topic: 'cl', partition: 0, key: expect.any(String), value: '1' }])
		})
	})

	describe('ChangelogBackedSessionStore', () => {
		function sessionStore() {
			const inner = new InMemorySessionStore<string, string>('s', {
				keyCodec: str,
				valueCodec: str,
				retentionMs: 1000,
			})
			const output = new RecordingOutput()
			const store = new ChangelogBackedSessionStore(inner, new ChangelogWriter('cl', 0, windowed, str, output))
			const tombstones = () =>
				output.sent
					.filter(({ record }) => record.value === null)
					.map(({ record }) => windowed.decode(record.key!))
			return { inner, output, store, tombstones }
		}

		it('logs puts local first', async () => {
			const { inner, output, store } = sessionStore()
			const order: string[] = []
			const put = inner.put.bind(inner)
			vi.spyOn(inner, 'put').mockImplementation(async (key, value) => {
				await put(key, value)
				order.push('inner.put')
			})
			vi.spyOn(output, 'send').mockImplementation(() => void order.push('changelog'))
			await store.put(w('k', 0, 1000), '1')
			expect(order).toEqual(['inner.put', 'changelog'])
		})

		it('B11: remove() writes a tombstone for every session of the key, negative times included', async () => {
			const { inner, store, tombstones } = sessionStore()
			await inner.put(w('k', -500, -400), 'negative')
			await inner.put(w('k', 0, 10), 'zero')
			await inner.put(w('other', 0, 10), 'other key')
			await store.remove('k')
			expect(tombstones()).toEqual([w('k', -500, -400), w('k', 0, 10)])
			expect(await inner.approximateNumEntries()).toBe(1)
		})

		it('KF17: expiry is local only, it writes no tombstones (the changelog retention drops expired sessions)', async () => {
			const { inner, output, store } = sessionStore()
			await inner.put(w('k', 0, 10), 'old')
			await inner.put(w('k', 9_500, 9_600), 'live')
			expect(await store.expireOldSessions(10_000)).toEqual([w('k', 0, 10)])
			expect(await inner.approximateNumEntries()).toBe(1)
			expect(output.sent).toEqual([])
		})
	})

	describe('errors', () => {
		it('ChangelogPartitionMismatchError names the topic, both counts and the source topics', () => {
			const error = new ChangelogPartitionMismatchError('my-app-counts-changelog', 8, 1, ['orders', 'events'])
			expect(error).toBeInstanceOf(Error)
			expect(error.name).toBe('ChangelogPartitionMismatchError')
			expect(error.changelogTopic).toBe('my-app-counts-changelog')
			expect(error.expectedPartitions).toBe(8)
			expect(error.actualPartitions).toBe(1)
			expect(error.sourceTopics).toEqual(['orders', 'events'])
			expect(error.message).toContain('my-app-counts-changelog')
			expect(error.message).toContain('1 partitions')
			expect(error.message).toContain('8')
			expect(error.message).toContain('orders')
			expect(error.message).toContain('events')
		})

		it('SourceTopicNotFoundError names the topic and the store', () => {
			const error = new SourceTopicNotFoundError('orders', 'order-counts')
			expect(error).toBeInstanceOf(Error)
			expect(error.name).toBe('SourceTopicNotFoundError')
			expect(error.topic).toBe('orders')
			expect(error.storeName).toBe('order-counts')
			expect(error.message).toContain('orders')
			expect(error.message).toContain('order-counts')
			expect(error.message).toContain('does not exist')
		})
	})
})
