/** Thrown by the parts of the rebuilt DSL that are not implemented yet. */
export class NotImplementedError extends Error {
	override name = 'NotImplementedError'
}

export function notImplemented(what: string): never {
	throw new NotImplementedError(`${what} is not implemented yet`)
}
