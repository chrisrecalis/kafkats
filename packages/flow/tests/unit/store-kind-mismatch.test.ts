import { afterEach, describe, expect, it } from 'vitest'
import { codec, flow, SessionWindows, TimeWindows, type FlowApp } from '../../src/index.js'

type StoreKind = 'key-value' | 'window' | 'session'

/** Materialize a count over `topic` in a store of the given kind called `storeName`. */
function countInto(app: FlowApp, topic: string, kind: StoreKind, storeName: string): void {
	const grouped = app.stream(topic, { key: codec.string(), value: codec.string() }).groupByKey()
	if (kind === 'key-value') {
		grouped.count({ storeName })
	} else {
		grouped
			.windowedBy(kind === 'window' ? TimeWindows.of('10s') : SessionWindows.withInactivityGap('10s'))
			.count({ storeName })
	}
}

describe('state stores of different kinds sharing a name', () => {
	const apps: FlowApp[] = []
	function createApp(): FlowApp {
		const app = flow({
			applicationId: 'store-kinds',
			client: { clientId: 'store-kinds', brokers: ['localhost:9092'] },
		})
		apps.push(app)
		return app
	}
	afterEach(async () => {
		await Promise.all(apps.splice(0).map(app => app.close()))
	})

	const kinds: StoreKind[] = ['key-value', 'window', 'session']
	const mismatches = kinds.flatMap(existing =>
		kinds.filter(k => k !== existing).map(requested => [existing, requested])
	)

	it.each(mismatches)('asking for a %s store as a %s store throws', (existing, requested) => {
		const app = createApp()
		countInto(app, 'first', existing as StoreKind, 'shared')

		expect(() => countInto(app, 'second', requested as StoreKind, 'shared')).toThrow(
			`State store 'shared' already exists as a ${existing} store and cannot be used as a ${requested} store`
		)
	})

	it.each(kinds)('asking for an existing %s store by name again returns it', kind => {
		const app = createApp()
		countInto(app, 'first', kind, 'shared')

		expect(() => countInto(app, 'first', kind, 'shared')).not.toThrow()
	})

	it("asking for a global table's store as a window store throws", () => {
		const app = createApp()
		app.globalTable('lookup', {
			key: codec.string(),
			value: codec.string(),
			materialized: { storeName: 'shared' },
		})

		expect(() => countInto(app, 'events', 'window', 'shared')).toThrow(
			"State store 'shared' already exists as a key-value store and cannot be used as a window store"
		)
	})
})
