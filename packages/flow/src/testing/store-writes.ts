/**
 * Writes of test code to a task's store under exactly-once (4.14): Kafka Streams' producer begins a transaction on
 * the first send, so a store pre-populated through `getStateStore()` is logged and committed like a processor's
 * write. The driver opens its transaction only around its own steps, so a write from test code opens it first.
 */
import type { StateStore } from '@/state.js'

/** The methods of the store kinds that write (and may log to the changelog). */
const WRITES: ReadonlySet<PropertyKey> = new Set(['put', 'delete', 'remove', 'expireOldWindows', 'expireOldSessions'])

/** `store` whose writes await `open()` first; reads pass through. */
export function writesInTransaction<S extends StateStore>(store: S, open: () => Promise<void>): S {
	return new Proxy(store, {
		get(target, property, receiver) {
			const member: unknown = Reflect.get(target, property, receiver)
			if (typeof member !== 'function' || !WRITES.has(property)) {
				return member
			}
			return async (...args: unknown[]) => {
				await open()
				return member.apply(target, args)
			}
		},
	})
}
