/**
 * Fake IO for runtime tests (9): a fake cluster (topic logs with transactions, group offsets) and the ports over
 * it, each step of which ends as the test scripts it (`Steps`: ok, fail, hang); the TaskManager's other ports.
 * Free of vitest: the micro-benchmark uses them too.
 */
export * from './fake-io/admin.js'
export * from './fake-io/cluster.js'
export * from './fake-io/group-consumer.js'
export * from './fake-io/producer.js'
export * from './fake-io/records.js'
export * from './fake-io/restore-consumer.js'
export * from './fake-io/runtime-io.js'
export * from './fake-io/steps.js'
export * from './fake-io/task-ports.js'
