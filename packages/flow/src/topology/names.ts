/**
 * Legal names of nodes, state stores and internal topics (KS `Named.validate`, Kafka's `Topic.validate`). Names end up
 * in topic names (repartition and changelog topics), provider store names and commit metadata keys, so all follow
 * Kafka's topic name rule: 1 to 249 ASCII alphanumerics, '.', '_' and '-', other than '.' and '..'.
 */
import { TopologyError } from '@/api/errors.js'

export const MAX_NAME_LENGTH = 249

const LEGAL_NAME = /^[a-zA-Z0-9._-]+$/

/** Throws a TopologyError unless `name` is legal; `what` says what it names: `Node`, `State store`, ... */
export function checkName(what: string, name: string): void {
	if (name === '') {
		throw new TopologyError(`${what} name must not be empty`)
	}
	if (name === '.' || name === '..') {
		throw new TopologyError(`${what} name cannot be '${name}'`)
	}
	if (name.length > MAX_NAME_LENGTH) {
		throw new TopologyError(
			`${what} name '${name}' is ${name.length} characters long; names may be at most ${MAX_NAME_LENGTH}`
		)
	}
	if (!LEGAL_NAME.test(name)) {
		throw new TopologyError(
			`${what} name '${name}' is illegal: names may only contain ASCII alphanumerics, '.', '_' and '-'`
		)
	}
}
