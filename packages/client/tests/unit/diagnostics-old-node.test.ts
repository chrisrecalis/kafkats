import { describe, expect, it, vi } from 'vitest'

// Node.js before 18.19 has diagnostics channels but no tracing channels.
vi.mock('node:diagnostics_channel', async importOriginal => {
	const original = await importOriginal<{ default: typeof import('node:diagnostics_channel') }>()
	const { tracingChannel: _, ...withoutTracing } = original.default
	return { ...original, default: withoutTracing }
})

describe('diagnostics on a runtime without tracing channels', () => {
	it('loads, runs operations unchanged and publishes nothing', async () => {
		const { diagnostics, hasSubscribers, trace } = await import('@/diagnostics.js')

		const subscriber = { start: vi.fn(), end: vi.fn(), asyncStart: vi.fn(), asyncEnd: vi.fn(), error: vi.fn() }
		diagnostics.commit.start.subscribe(subscriber.start)
		expect(hasSubscribers(diagnostics.commit)).toBe(false)

		const describeOperation = vi.fn()
		await expect(trace(diagnostics.commit, describeOperation, async context => context ?? 'ran')).resolves.toBe(
			'ran'
		)
		expect(describeOperation).not.toHaveBeenCalled()
		expect(subscriber.start).not.toHaveBeenCalled()
	})
})
