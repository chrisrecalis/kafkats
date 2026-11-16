/**
 * What the TestDriver needs of an app made by flow() (4.14): its topology and resolved settings, behind a symbol
 * (not part of the public API; no runtime entry).
 */
import type { Logger } from '@kafkats/client'
import type { FlowApp } from '@/api/app.js'
import type { AppSettings } from '@/app/config.js'
import type { StateStoreProvider } from '@/state.js'
import type { BuiltTopology } from '@/topology/built-topology.js'

export const INTERNAL = Symbol('kafkats.flow.internal')

export interface AppInternals {
	/** Builds the topology once; the DSL is frozen afterwards. */
	build(): BuiltTopology
	readonly settings: AppSettings
	readonly stateStoreProvider: StateStoreProvider
	readonly logger: Logger
}

export function internals(app: FlowApp): AppInternals {
	const found = (app as Partial<Record<typeof INTERNAL, AppInternals>>)[INTERNAL]
	if (found === undefined) {
		throw new TypeError('not an app made by flow()')
	}
	return found
}
