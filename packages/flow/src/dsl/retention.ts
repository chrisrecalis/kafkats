/**
 * The retention model of the DSL's window and session stores (6.2), as Kafka Streams' store factories: a store keeps
 * what a record can still update or join. Time windows keep `size + grace` (`WindowStoreMaterializer.java:51-58`),
 * sessions `gap + grace` (`SessionStoreMaterializer.java:49-57`), sliding windows `2 * timeDifference + grace`, since a
 * record may copy a window that started that long before it (`SlidingWindowStoreMaterializer.java:47-57`), and the
 * buffers of a stream-stream join `before + after + grace` (`StreamJoinedStoreFactory.java:114-115`). An aggregation's
 * `Materialized.retention` may keep windows longer, never shorter.
 */
import { TopologyError } from '@/api/errors.js'
import type { WindowDuration } from '@/api/options.js'
import { durationMs, SessionWindows, TimeWindows, type JoinWindows, type SlidingWindows } from '@/dsl/windows.js'

/** The retention of a windowed aggregation's store: `retention` (`Materialized.retention`), at least the minimum. */
export function aggregationRetentionMs(
	store: string,
	windows: TimeWindows | SessionWindows | SlidingWindows,
	retention: WindowDuration | undefined
): number {
	const [minimumMs, minimum] = minimumRetention(windows)
	if (retention === undefined) {
		return minimumMs
	}
	const retentionMs = durationMs(retention, `The retention of state store '${store}'`)
	if (retentionMs < minimumMs) {
		throw new TopologyError(
			`The retention of state store '${store}' must be no smaller than ${minimum}, got ${retentionMs} ms`
		)
	}
	return retentionMs
}

/** The retention of both buffers of a stream-stream join. */
export function joinRetentionMs(windows: JoinWindows): number {
	return windows.beforeMs + windows.afterMs + windows.graceMs
}

/** The smallest retention of an aggregation's store, and how to name it in an error. */
function minimumRetention(windows: TimeWindows | SessionWindows | SlidingWindows): [number, string] {
	const grace = `the grace period (${windows.graceMs} ms)`
	if (windows instanceof TimeWindows) {
		return [windows.sizeMs + windows.graceMs, `the window size (${windows.sizeMs} ms) plus ${grace}`]
	}
	if (windows instanceof SessionWindows) {
		return [windows.gapMs + windows.graceMs, `the inactivity gap (${windows.gapMs} ms) plus ${grace}`]
	}
	const difference = windows.timeDifferenceMs
	return [2 * difference + windows.graceMs, `twice the time difference (${difference} ms) plus ${grace}`]
}
