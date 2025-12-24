import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
	DEFAULT_CHANGELOG_CONFIGS,
	ChangelogWriter,
	windowedKeyCodec,
	ChangelogPartitionMismatchError,
	SourceTopicNotFoundError,
} from '@/changelog.js'
import { ChangelogBackedKeyValueStore } from '@/state/changelog.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'
import { codec } from '@/codec.js'
import type { WindowedKey } from '@/state.js'

describe('changelog', () => {
	describe('buildChangelogTopicName', () => {
		it('builds changelog topic name with appId and storeName', () => {
			const name = buildChangelogTopicName('my-app', 'my-store')
			expect(name).toBe('my-app-my-store-changelog')
		})

		it('handles special characters in names', () => {
			const name = buildChangelogTopicName('app-v1', 'user_counts')
			expect(name).toBe('app-v1-user_counts-changelog')
		})
	})

	describe('DEFAULT_CHANGELOG_CONFIGS', () => {
		it('has cleanup.policy set to compact', () => {
			expect(DEFAULT_CHANGELOG_CONFIGS['cleanup.policy']).toBe('compact')
		})

		it('has retention.ms set to -1 (infinite)', () => {
			expect(DEFAULT_CHANGELOG_CONFIGS['retention.ms']).toBe('-1')
		})

		it('has segment.bytes set to 50MB', () => {
			expect(DEFAULT_CHANGELOG_CONFIGS['segment.bytes']).toBe('52428800')
		})
	})

	describe('getDefaultTopicConfigs', () => {
		it('returns default configs without overrides', () => {
			const configs = getDefaultTopicConfigs()
			expect(configs).toEqual(DEFAULT_CHANGELOG_CONFIGS)
		})

		it('merges overrides with defaults', () => {
			const configs = getDefaultTopicConfigs({
				'retention.ms': '86400000',
				'min.insync.replicas': '2',
			})
			expect(configs).toEqual({
				'cleanup.policy': 'compact',
				'retention.ms': '86400000',
				'segment.bytes': '52428800',
				'min.insync.replicas': '2',
			})
		})

		it('overrides can replace default values', () => {
			const configs = getDefaultTopicConfigs({ 'cleanup.policy': 'delete' })
			expect(configs['cleanup.policy']).toBe('delete')
		})
	})

	describe('resolveChangelogConfig', () => {
		it('returns enabled: false when given false', () => {
			const config = resolveChangelogConfig(false)
			expect(config.enabled).toBe(false)
		})

		it('returns enabled: true when given undefined', () => {
			const config = resolveChangelogConfig(undefined)
			expect(config.enabled).toBe(true)
		})

		it('returns enabled: true when given true', () => {
			const config = resolveChangelogConfig(true)
			expect(config.enabled).toBe(true)
		})

		it('preserves config properties when given object', () => {
			const config = resolveChangelogConfig({
				topicName: 'custom-changelog',
				replicationFactor: 3,
			})
			expect(config).toEqual({
				enabled: true,
				topicName: 'custom-changelog',
				replicationFactor: 3,
			})
		})

		it('respects explicit enabled: false in object config', () => {
			const config = resolveChangelogConfig({ enabled: false })
			expect(config.enabled).toBe(false)
		})
	})

	describe('windowedKeyCodec', () => {
		it('encodes and decodes windowed keys correctly', () => {
			const stringCodec = codec.string()
			const windowed = windowedKeyCodec(stringCodec)

			const original: WindowedKey<string> = {
				key: 'test-key',
				windowStart: 1000000,
				windowEnd: 2000000,
			}

			const encoded = windowed.encode(original)
			const decoded = windowed.decode(encoded)

			expect(decoded.key).toBe('test-key')
			expect(decoded.windowStart).toBe(1000000)
			expect(decoded.windowEnd).toBe(2000000)
		})

		it('handles large timestamp values', () => {
			const stringCodec = codec.string()
			const windowed = windowedKeyCodec(stringCodec)

			const original: WindowedKey<string> = {
				key: 'key',
				windowStart: Date.now(),
				windowEnd: Date.now() + 3600000,
			}

			const encoded = windowed.encode(original)
			const decoded = windowed.decode(encoded)

			expect(decoded.windowStart).toBe(original.windowStart)
			expect(decoded.windowEnd).toBe(original.windowEnd)
		})
	})

	describe('ChangelogWriter', () => {
		it('writes key-value pairs through producer', async () => {
			const mockSend = vi.fn().mockResolvedValue(undefined)
			const mockProducer = { send: mockSend } as never

			const writer = new ChangelogWriter<string, number>(
				'test-changelog',
				codec.string(),
				codec.json<number>(),
				() => mockProducer,
				() => null
			)

			await writer.write('key1', 42)

			expect(mockSend).toHaveBeenCalledWith('test-changelog', {
				key: Buffer.from('key1'),
				value: Buffer.from('42'),
			})
		})

		it('writes through transaction when available', async () => {
			const mockProducerSend = vi.fn().mockResolvedValue(undefined)
			const mockProducer = { send: mockProducerSend } as never

			const mockTxSend = vi.fn().mockResolvedValue(undefined)
			const mockTransaction = { send: mockTxSend } as never

			const writer = new ChangelogWriter<string, number>(
				'test-changelog',
				codec.string(),
				codec.json<number>(),
				() => mockProducer,
				() => mockTransaction
			)

			await writer.write('key1', 42)

			expect(mockTxSend).toHaveBeenCalledWith('test-changelog', {
				key: Buffer.from('key1'),
				value: Buffer.from('42'),
			})
			expect(mockProducerSend).not.toHaveBeenCalled()
		})

		it('writes tombstones with null value', async () => {
			const mockSend = vi.fn().mockResolvedValue(undefined)
			const mockProducer = { send: mockSend } as never

			const writer = new ChangelogWriter<string, number>(
				'test-changelog',
				codec.string(),
				codec.json<number>(),
				() => mockProducer,
				() => null
			)

			await writer.writeTombstone('key-to-delete')

			expect(mockSend).toHaveBeenCalledWith('test-changelog', {
				key: Buffer.from('key-to-delete'),
				value: null,
			})
		})
	})

	describe('ChangelogBackedKeyValueStore', () => {
		let innerStore: InMemoryKeyValueStore<string, number>
		let mockWriter: ChangelogWriter<string, number>
		let writeSpy: ReturnType<typeof vi.fn>
		let tombstoneSpy: ReturnType<typeof vi.fn>

		beforeEach(() => {
			innerStore = new InMemoryKeyValueStore('test-store', {
				keyCodec: codec.string(),
				valueCodec: codec.json<number>(),
			})

			writeSpy = vi.fn().mockResolvedValue(undefined)
			tombstoneSpy = vi.fn().mockResolvedValue(undefined)

			mockWriter = {
				write: writeSpy,
				writeTombstone: tombstoneSpy,
			} as unknown as ChangelogWriter<string, number>
		})

		it('delegates get to inner store', async () => {
			await innerStore.put('key1', 100)
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)

			const value = await store.get('key1')
			expect(value).toBe(100)
		})

		it('writes to inner store and changelog on put', async () => {
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)

			await store.put('key1', 42)

			// Check inner store was updated
			const value = await innerStore.get('key1')
			expect(value).toBe(42)

			// Check changelog was written
			expect(writeSpy).toHaveBeenCalledWith('key1', 42)
		})

		it('deletes from inner store and writes tombstone on delete', async () => {
			await innerStore.put('key1', 100)
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)

			await store.delete('key1')

			// Check inner store was updated
			const value = await innerStore.get('key1')
			expect(value).toBeUndefined()

			// Check tombstone was written
			expect(tombstoneSpy).toHaveBeenCalledWith('key1')
		})

		it('exposes inner store for restoration', async () => {
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)
			expect(store.innerStore).toBe(innerStore)
		})

		it('delegates all() to inner store', async () => {
			await innerStore.put('a', 1)
			await innerStore.put('b', 2)
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)

			const entries: [string, number][] = []
			for await (const entry of store.all()) {
				entries.push(entry)
			}

			expect(entries).toHaveLength(2)
		})

		it('delegates name to inner store', () => {
			const store = new ChangelogBackedKeyValueStore(innerStore, mockWriter)
			expect(store.name).toBe('test-store')
		})
	})

	describe('ChangelogPartitionMismatchError', () => {
		it('creates error with correct properties', () => {
			const error = new ChangelogPartitionMismatchError('my-changelog', 8, 1, ['orders', 'events'])

			expect(error.name).toBe('ChangelogPartitionMismatchError')
			expect(error.changelogTopic).toBe('my-changelog')
			expect(error.expectedPartitions).toBe(8)
			expect(error.actualPartitions).toBe(1)
			expect(error.sourceTopics).toEqual(['orders', 'events'])
		})

		it('generates descriptive error message', () => {
			const error = new ChangelogPartitionMismatchError('my-app-counts-changelog', 8, 1, ['orders', 'events'])

			expect(error.message).toContain('my-app-counts-changelog')
			expect(error.message).toContain('1 partitions')
			expect(error.message).toContain('8')
			expect(error.message).toContain('orders')
			expect(error.message).toContain('events')
		})

		it('is instanceof Error', () => {
			const error = new ChangelogPartitionMismatchError('topic', 8, 1, ['source'])

			expect(error).toBeInstanceOf(Error)
		})
	})

	describe('SourceTopicNotFoundError', () => {
		it('creates error with correct properties', () => {
			const error = new SourceTopicNotFoundError('orders', 'order-counts')

			expect(error.name).toBe('SourceTopicNotFoundError')
			expect(error.topic).toBe('orders')
			expect(error.storeName).toBe('order-counts')
		})

		it('generates descriptive error message', () => {
			const error = new SourceTopicNotFoundError('orders', 'order-counts')

			expect(error.message).toContain('orders')
			expect(error.message).toContain('order-counts')
			expect(error.message).toContain('does not exist')
		})

		it('is instanceof Error', () => {
			const error = new SourceTopicNotFoundError('topic', 'store')

			expect(error).toBeInstanceOf(Error)
		})
	})
})
