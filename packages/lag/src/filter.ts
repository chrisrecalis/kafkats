import type { NameFilter } from './types.js'

export function matches(filter: NameFilter | undefined, name: string, defaultValue: boolean): boolean {
	if (filter === undefined) return defaultValue
	if (Array.isArray(filter)) return filter.includes(name)
	if (filter instanceof RegExp) {
		// Reset lastIndex in case a /g regex was passed
		filter.lastIndex = 0
		return filter.test(name)
	}
	return filter(name)
}

export function isSelected(name: string, include: NameFilter | undefined, exclude: NameFilter | undefined): boolean {
	return matches(include, name, true) && !matches(exclude, name, false)
}

/** Parse a CLI/env filter: comma-separated exact names, or `/regex/` */
export function parseNameFilter(raw: string | undefined): NameFilter | undefined {
	if (raw === undefined) return undefined
	const value = raw.trim()
	if (value === '') return undefined
	if (value.length > 1 && value.startsWith('/') && value.lastIndexOf('/') > 0) {
		const end = value.lastIndexOf('/')
		return new RegExp(value.slice(1, end), value.slice(end + 1))
	}
	return value
		.split(',')
		.map(s => s.trim())
		.filter(s => s.length > 0)
}
