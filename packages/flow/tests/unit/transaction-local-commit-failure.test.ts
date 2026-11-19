import { describe, expect, it } from 'vitest'
import { classify } from '@/runtime/errors.js'
import type { KeyValueStore } from '@/state.js'
import { kvStore, pipe, TaskHarness } from './support/task-harness.js'
import type { ProcessorContext } from '@/processor/api.js'

describe('exactly-once: applying buffered state after the Kafka commit fails', () => {
	it('drops the buffers of every task of the transaction, not only of the one that failed, and migrates', async () => {
		const h = new TaskHarness({
			eos: true,
			partitions: 3,
			build: b =>
				pipe(b, {
					stores: [kvStore('s')],
					processor: () => {
						let context: ProcessorContext<string, string>
						let store: KeyValueStore<string, string>
						return {
							init: ctx => {
								context = ctx
								store = ctx.getStateStore('s')
							},
							process: async record => {
								await store.put(record.key, record.value)
								return context.forward(record)
							},
						}
					},
				}),
		})
		const tasks = [h.task(0, 0), h.task(0, 1), h.task(0, 2)]
		for (const task of tasks) {
			await h.start(task)
			h.enqueue(task, 'input', [{ key: 'k', value: 'buffered', timestamp: 1 }])
			await h.process(task)
		}
		const raw = (task: (typeof tasks)[number]) => task.stores[0]!.raw as KeyValueStore<string, string>
		raw(tasks[1]!).put = () => Promise.reject(new Error('disk full'))

		const failure = await h.commit().catch((error: unknown) => error)

		// Kafka committed: the stores are now behind their changelogs and only a restore repairs them.
		expect((failure as Error).message).toBe('disk full')
		expect(classify(failure, 'commit')).toBe('migrate')
		expect(h.producer.transactions[0]!.state).toBe('committed')
		expect(tasks.map(task => h.state(task).bufferDiscards)).toEqual([1, 1, 1])
		expect(tasks.map(task => h.state(task).checkpoints)).toEqual([[], [], []])
		expect(h.events).not.toContain('noteCommitted')

		// The task restarts from a restore, which brings a newer value: no buffer of the transaction may hide it.
		for (const task of tasks) {
			delete (raw(task) as Partial<KeyValueStore<string, string>>).put
			await raw(task).put('k', 'restored')
			expect(await (task.store('s') as KeyValueStore<string, string>).get('k')).toBe('restored')
		}
	})
})
