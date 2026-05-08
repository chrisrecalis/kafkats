import type { WindowDuration } from '@/types.js'
import { TimeWindows, SlidingWindows, type SessionWindows } from '@/windows.js'

export const SLIDING_WINDOWS_NOT_IMPLEMENTED =
	'SlidingWindows is not yet implemented. Use TimeWindows.of(size).advanceBy(advance) for hopping windows, ' +
	'or SessionWindows for session-based aggregation.'

export function joinWindowMs(joinWindow: TimeWindows | SlidingWindows | SessionWindows): number {
	if (joinWindow instanceof SlidingWindows) {
		throw new Error(SLIDING_WINDOWS_NOT_IMPLEMENTED)
	}
	return joinWindow instanceof TimeWindows
		? parseWindowDuration(joinWindow.size)
		: parseWindowDuration(joinWindow.gap)
}

/**
 * Parse a WindowDuration to milliseconds.
 */
export function parseWindowDuration(duration: WindowDuration): number {
	if (typeof duration === 'number') {
		return duration
	}
	const match = duration.match(/^(\d+)(ms|s|m|h|d)$/)
	if (!match) {
		throw new Error(`Invalid window duration: ${duration}`)
	}
	const value = parseInt(match[1]!, 10)
	const unit = match[2]
	switch (unit) {
		case 'ms':
			return value
		case 's':
			return value * 1000
		case 'm':
			return value * 60 * 1000
		case 'h':
			return value * 60 * 60 * 1000
		case 'd':
			return value * 24 * 60 * 60 * 1000
		default:
			throw new Error(`Unknown time unit: ${unit}`)
	}
}
