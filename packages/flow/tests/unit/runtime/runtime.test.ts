/**
 * The runtime blocks of the legacy flow.test.ts (spec 9.1), to be implemented by WP2.7 against fake IO. New
 * expectations: one consumer and one producer per instance; the transactional id is `<applicationId>-<processId>`
 * (`^test-app-[0-9a-f-]{36}$`, no `-w<slot>`) unless `producer.transactionalId` is given; a record delivered while
 * stopping is not processed under either guarantee; a halt stops the whole runtime, including a run waiting out its
 * recovery backoff.
 */
import { describe, it } from 'vitest'

describe('FlowRuntime', () => {
	it.todo('uses transactions and sends offsets for exactly_once')
	it.todo('binds a batched transaction to the first consume context that opened it')
	it.todo('does not start a new transaction from a buffered record while closing')
	it.todo('finishes a buffered at-least-once record while closing')
	it.todo('does not restart a worker that was in restart backoff when processing was halted')
	it.todo('stops every worker when one of them fails with an error that is not retried')
	it.todo('spawns one worker per stream thread')
	it.todo('derives unique transactional IDs for separate application instances')
	it.todo('preserves an explicit transactional ID for a single stream thread')
})
