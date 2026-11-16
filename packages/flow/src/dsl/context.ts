/** What every DSL object of one app shares: the topology builder, naming and format helpers. */
export interface DslContext {
	readonly applicationId: string
}
