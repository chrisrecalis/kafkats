/** A table update. The old value is populated only where sending old values is enabled. */
export interface Change<V> {
	readonly newValue: V | null
	readonly oldValue: V | null
}
