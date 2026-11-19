/**
 * Deprecation warnings of the DSL: a Node DeprecationWarning (silenced by `--no-deprecation`), emitted once per
 * process for each deprecated API.
 */
const warned = new Set<string>()

/** Warns that `api` is deprecated the first time it is used; `advice` says why it matters and what replaces it. */
export function warnDeprecated(api: string, advice: string): void {
	if (warned.has(api)) {
		return
	}
	warned.add(api)
	process.emitWarning(`${api} is deprecated: ${advice}`, 'DeprecationWarning')
}
