import { describe, expect, it } from 'vitest'
import { KeyedTurns } from '../../src/runtime/keyed-turns.js'

const tick = () => new Promise<void>(resolve => setImmediate(resolve))

describe('KeyedTurns', () => {
	it('runs tasks of one key in order, one at a time, and other keys alongside', async () => {
		const turns = new KeyedTurns<number>()
		const log: string[] = []
		const task = (name: string) => async () => {
			log.push(`${name} starts`)
			await tick()
			log.push(`${name} ends`)
			return name
		}

		const results = await Promise.all([turns.run(0, task('a')), turns.run(0, task('b')), turns.run(1, task('c'))])

		expect(results).toEqual(['a', 'b', 'c'])
		expect(log).toEqual(['a starts', 'c starts', 'a ends', 'b starts', 'c ends', 'b ends'])
	})

	it('lets the next task of a key run after one failed', async () => {
		const turns = new KeyedTurns<string>()
		const failed = turns.run('k', async () => {
			throw new Error('boom')
		})
		const next = turns.run('k', async () => 'ran')

		await expect(failed).rejects.toThrow('boom')
		await expect(next).resolves.toBe('ran')
	})
})
