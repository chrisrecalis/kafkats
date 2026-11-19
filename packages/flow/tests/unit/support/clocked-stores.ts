/**
 * Window and session stores with a clock, as a task stacks them for its time-based processors (6.2), over in-memory
 * stores: for processor harness tests of windowed operators.
 */
import { codec } from '@/codec.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import type { ValueAndTimestamp } from '@/state.js'
import { InMemorySessionStore, InMemoryWindowStore } from '@/state/memory.js'
import { ClockedSessionStore, ClockedWindowStore, TaskStoreClock } from '@/state/time-clipping.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'

const keyCodec = codec.string()
const valueCodec = codec.json<number>()

/** A window store of timestamped numbers (time-window aggregations) and the provider store under its clipping layer. */
export function clockedWindowStore(name: string, retentionMs: number, windowSizeMs: number) {
	const inner = new InMemoryWindowStore<string, ValueAndTimestamp<number>>(name, {
		keyCodec,
		valueCodec: valueAndTimestampCodec(valueCodec),
		retentionMs,
		windowSizeMs,
	})
	const clock = new TaskStoreClock(new ProcessorMetadataImpl(), name, retentionMs)
	return { inner, store: new ClockedWindowStore(inner, clock, retentionMs) }
}

/** A session store of numbers and the provider store under its clipping layer. */
export function clockedSessionStore(name: string, retentionMs: number) {
	const inner = new InMemorySessionStore<string, number>(name, { keyCodec, valueCodec, retentionMs })
	const clock = new TaskStoreClock(new ProcessorMetadataImpl(), name, retentionMs)
	return { inner, store: new ClockedSessionStore(inner, clock, retentionMs) }
}

/** A session-layout store of timestamped numbers (sliding-window aggregations) and the provider store under it. */
export function clockedSlidingStore(name: string, retentionMs: number) {
	const inner = new InMemorySessionStore<string, ValueAndTimestamp<number>>(name, {
		keyCodec,
		valueCodec: valueAndTimestampCodec(valueCodec),
		retentionMs,
	})
	const clock = new TaskStoreClock(new ProcessorMetadataImpl(), name, retentionMs)
	return { inner, store: new ClockedSessionStore(inner, clock, retentionMs) }
}
