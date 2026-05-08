import { describe, expect, it, vi } from 'vitest'

import { TestDriver } from '../../src/testing.js'
import type {
	ChangelogCheckpointStore,
	KeyValueStore,
	WindowStore,
	SessionStore,
	StateStoreProvider,
	KeyValueStoreOptions,
	WindowStoreOptions,
	SessionStoreOptions,
} from '../../src/state.js'
import { InMemoryStateStoreProvider } from '../../src/state/memory.js'

// Wrap InMemoryStateStoreProvider but inject a failing CheckpointStore so tests drive
// the real flushChangelogCheckpoints path through FlowAppImpl.
function makeProviderWithFailingCheckpointStore(checkpointStore: ChangelogCheckpointStore): StateStoreProvider {
	const inner = new InMemoryStateStoreProvider()
	return {
		name: inner.name,
		createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
			return inner.createKeyValueStore(name, options)
		},
		createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
			return inner.createWindowStore(name, options)
		},
		createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
			return inner.createSessionStore(name, options)
		},
		getChangelogCheckpointStore: () => checkpointStore,
		close: () => inner.close(),
	}
}

describe('checkpoint persistence errors surface, not swallow', () => {
	it('flushChangelogCheckpoints logs + records lastError when set() rejects', async () => {
		const setError = new Error('disk full')
		const checkpointStore: ChangelogCheckpointStore = {
			get: vi.fn().mockResolvedValue(undefined),
			set: vi.fn().mockRejectedValue(setError),
			flush: vi.fn().mockResolvedValue(undefined),
		}

		const driver = new TestDriver({
			stateStoreProvider: makeProviderWithFailingCheckpointStore(checkpointStore),
		})
		// Force topology compilation but don't actually start the consumer/producer loop.
		void driver.flow

		// Reach into the private impl to drive flushChangelogCheckpoints directly with a
		// synthetic worker. This actually calls the production method (regression-proof:
		// reverting to .catch(() => {}) would silently break this test's lastError check).
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const flowAny = driver.flow as any
		const worker = {
			pendingChangelogOffsets: new Map([
				['t:0', { topic: 't', partition: 0, offset: 41n }],
				['t:1', { topic: 't', partition: 1, offset: 99n }],
			]),
		}

		await flowAny.flushChangelogCheckpoints(worker)

		// set() called with offset+1 for both partitions.
		expect(checkpointStore.set).toHaveBeenCalledWith('t', 0, 42n)
		expect(checkpointStore.set).toHaveBeenCalledWith('t', 1, 100n)
		// lastError reflects the failure even though we didn't halt.
		expect(flowAny.lastError).toBe(setError)
	})

	it('logs flush() rejections too', async () => {
		const flushError = new Error('fsync failed')
		const checkpointStore: ChangelogCheckpointStore = {
			get: vi.fn().mockResolvedValue(undefined),
			set: vi.fn().mockResolvedValue(undefined),
			flush: vi.fn().mockRejectedValue(flushError),
		}

		const driver = new TestDriver({
			stateStoreProvider: makeProviderWithFailingCheckpointStore(checkpointStore),
		})
		void driver.flow

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const flowAny = driver.flow as any
		await flowAny.flushChangelogCheckpoints({
			pendingChangelogOffsets: new Map([['t:0', { topic: 't', partition: 0, offset: 0n }]]),
		})

		expect(checkpointStore.flush).toHaveBeenCalled()
		expect(flowAny.lastError).toBe(flushError)
	})

	it('dedupes log lines for repeated identical errors and recovers on success', async () => {
		const setError = new Error('disk full')
		let setShouldFail = true
		const checkpointStore: ChangelogCheckpointStore = {
			get: vi.fn().mockResolvedValue(undefined),
			set: vi.fn(() => (setShouldFail ? Promise.reject(setError) : Promise.resolve())),
			flush: vi.fn().mockResolvedValue(undefined),
		}

		const driver = new TestDriver({
			stateStoreProvider: makeProviderWithFailingCheckpointStore(checkpointStore),
		})
		void driver.flow
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const flowAny = driver.flow as any

		const worker = { pendingChangelogOffsets: new Map([['t:0', { topic: 't', partition: 0, offset: 0n }]]) }

		// First failure → message recorded.
		await flowAny.flushChangelogCheckpoints(worker)
		const firstMsg = flowAny.lastCheckpointErrorMessage
		expect(firstMsg).toBe('disk full')

		// Repeat failure → no log re-emit; lastCheckpointErrorMessage stays the same.
		await flowAny.flushChangelogCheckpoints(worker)
		expect(flowAny.lastCheckpointErrorMessage).toBe(firstMsg)

		// Subsequent success → recovery, message cleared.
		setShouldFail = false
		await flowAny.flushChangelogCheckpoints(worker)
		expect(flowAny.lastCheckpointErrorMessage).toBeNull()
	})
})
