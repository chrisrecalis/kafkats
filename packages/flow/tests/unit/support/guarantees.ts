import type { TestDriverConfig } from '@/testing/driver.js'

/**
 * The processing modes a DSL test runs in, asserting the same outputs in each (DL13): at-least-once, exactly-once
 * committing after every record, and exactly-once committing only when the driver stops (store writes stay buffered).
 */
export const GUARANTEES: readonly (readonly [name: string, config: TestDriverConfig])[] = [
	['at-least-once', {}],
	['exactly-once', { processingGuarantee: 'exactly_once' }],
	['exactly-once, committed on stop', { processingGuarantee: 'exactly_once', commitEvery: 'stop' }],
]
