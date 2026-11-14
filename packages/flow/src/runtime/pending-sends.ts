/**
 * Produce requests of one worker whose acknowledgement has not been awaited yet.
 *
 * Processing a record does not wait for the broker to acknowledge what it produced (output
 * records, changelog records): waiting per record costs a round trip per record. Instead the
 * sends are collected here and awaited together at the next commit point, before any offset is
 * committed: the end of a consumed batch (at-least-once) or the transaction commit (exactly-once).
 * A failed send therefore fails the commit point, and nothing after the last one is acknowledged.
 */
export class PendingSends {
	private pending: Promise<void>[] = []
	private failure: { error: unknown } | null = null
	/** Incremented by discard(), so that sends discarded while in flight cannot fail later work. */
	private generation = 0

	/**
	 * Track a send. `onAck` runs when it is acknowledged, unless the send was discarded in the
	 * meantime: an acknowledgement that arrives after its work was rolled back must not be
	 * credited to whatever is being processed by then.
	 */
	track<T>(send: Promise<T>, onAck?: (result: T) => void | Promise<void>): void {
		const generation = this.generation
		this.pending.push(
			send.then(
				result => {
					if (onAck && generation === this.generation) return onAck(result)
				},
				error => {
					if (generation === this.generation) this.failure ??= { error }
				}
			)
		)
	}

	/** Wait until every tracked send has been answered, whatever the answer. */
	async settled(): Promise<void> {
		// Acknowledgement callbacks may track further sends, hence the loop.
		while (this.pending.length > 0) {
			const pending = this.pending
			this.pending = []
			await Promise.all(pending)
		}
	}

	/** Wait for every tracked send; throws the first failure. */
	async flush(): Promise<void> {
		await this.settled()
		const failure = this.failure
		this.failure = null
		if (failure) throw failure.error
	}

	/**
	 * Forget the tracked sends: their work is being rolled back, so neither their failures nor
	 * their acknowledgement callbacks count any more. Resolves once they have all been answered.
	 */
	discard(): Promise<void> {
		this.generation += 1
		const pending = this.pending
		this.pending = []
		this.failure = null
		return Promise.all(pending).then(() => {})
	}
}
