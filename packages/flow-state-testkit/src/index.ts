import type { EngineUnderTest } from './engine.js'
import { crashSuite } from './crash.js'
import { memoryParitySuite } from './memory-parity.js'
import { rangeSuite } from './range.js'
import { restartSuite } from './restart.js'
import { sharedDirSuite } from './shared-dir.js'
import { signedTimeOrderingSuite } from './signed-time-ordering.js'
import { windowBoundsSuite } from './window-bounds.js'

export type { EngineUnderTest } from './engine.js'

/**
 * Everything a persistent state store provider has to get right, as vitest suites: store
 * ordering and bounds, agreement with the in-memory stores, restart cycles of a flow app and
 * recovery after SIGKILL. The suites that run a flow app are skipped without KAFKA_BROKERS.
 */
export function describeStateStoreProvider(engine: EngineUnderTest): void {
	rangeSuite(engine)
	windowBoundsSuite(engine)
	signedTimeOrderingSuite(engine)
	memoryParitySuite(engine)
	restartSuite(engine)
	sharedDirSuite(engine)
	crashSuite(engine)
}
