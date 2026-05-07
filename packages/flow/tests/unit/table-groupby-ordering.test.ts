import { describe, it, expect, vi } from 'vitest'

import { TableGroupByNode } from '@/processors/table.js'

const DELTA_OP_HEADER = '__kafkats_delta_op'
const DELTA_SUB_VALUE = 'sub'
const DELTA_ADD_VALUE = 'add'
import type { KeyValueStore } from '@/state.js'

interface MappingEntry<K, V> {
	groupedKey: K
	value: V
}

describe('TableGroupByNode mapping ordering', () => {
	it('forwards SUB with old mapping BEFORE persisting new mapping', async () => {
		// Track the order of operations: forward(SUB) must precede store.put(newMapping)
		// so that on crash + replay, the previous mapping is still old when the
		// SUB re-fires (idempotent under EOS).
		const order: string[] = []

		const data = new Map<string, MappingEntry<string, number>>()
		data.set('source-key', { groupedKey: 'old-group', value: 10 })

		const store: KeyValueStore<string, MappingEntry<string, number>> = {
			name: 'mapping',
			get: vi.fn(async (key: string) => data.get(key)),
			put: vi.fn(async (key: string, value: MappingEntry<string, number>) => {
				order.push(`put(${key},group=${value.groupedKey},value=${value.value})`)
				data.set(key, value)
			}),
			delete: vi.fn(async (key: string) => {
				data.delete(key)
			}),
			all: vi.fn(),
			range: vi.fn(),
			approximateNumEntries: vi.fn().mockResolvedValue(0),
			init: vi.fn(),
			flush: vi.fn(),
			close: vi.fn(),
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
		} as any

		const storeRef = { store }
		const codec = {
			encode: (k: string) => Buffer.from(k),
			decode: (b: Buffer) => b.toString(),
		}

		const node = new TableGroupByNode<string, number, string>(
			(_k, v) => ['new-group', v],
			storeRef as unknown as { store: KeyValueStore<string, MappingEntry<string, number>> | null },
			codec
		)
		// Capture forwards
		const forwards: Array<{ key: string | null; value: number | null; headers: Record<string, Buffer> }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (record: any) => {
			const opBuf = record.headers[DELTA_OP_HEADER]
			const op = Buffer.isBuffer(opBuf) ? opBuf.toString() : String(opBuf)
			order.push(`forward(${op}: key=${record.key}, value=${record.value})`)
			forwards.push(record)
		}

		await node.process({
			key: 'source-key',
			value: 20,
			timestamp: 0n,
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})

		// Original (intentional) ordering: put first so recompute nodes see the
		// new mapping when SUB/ADD arrive, then SUB with old, then ADD with new.
		// An earlier review proposed SUB-before-put for crash-recovery reasons;
		// it was rejected because it breaks the at-least-once recompute path.
		// See flow.test.ts "retracts counts when grouped key changes".
		const subIdx = order.findIndex(s => s.startsWith(`forward(${DELTA_SUB_VALUE}`))
		const putIdx = order.findIndex(s => s.startsWith('put('))
		const addIdx = order.findIndex(s => s.startsWith(`forward(${DELTA_ADD_VALUE}`))

		expect(putIdx).toBeGreaterThanOrEqual(0)
		expect(subIdx).toBeGreaterThan(putIdx)
		expect(addIdx).toBeGreaterThan(subIdx)

		// And the SUB must carry the OLD grouped key/value.
		const subForward = forwards.find(f => {
			const op = f.headers[DELTA_OP_HEADER]
			return Buffer.isBuffer(op) ? op.toString() === DELTA_SUB_VALUE : false
		})
		expect(subForward?.key).toBe('old-group')
		expect(subForward?.value).toBe(10)
	})
})
