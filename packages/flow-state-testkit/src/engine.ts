import type { StateStoreProvider } from '@kafkats/flow'

/** A state store provider package as the shared suites need it. */
export interface EngineUnderTest {
	/** Shown in test names. */
	name: string
	create(stateDir: string): StateStoreProvider
	/** Whether approximateNumEntries() is exact (it must always be 0 exactly when a store is empty). */
	exactCounts: boolean
	/**
	 * How a child process (the crash tests) creates the same provider: the module to import and
	 * the name of its `(options: { stateDir }) => StateStoreProvider` export.
	 */
	module: { url: string; factory: string }
}
