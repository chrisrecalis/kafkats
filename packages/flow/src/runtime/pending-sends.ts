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

	track(send: Promise<unknown>): void {
		const generation = this.generation
		this.pending.push(
			send.then(
				() => {},
				error => {
					if (generation === this.generation) this.failure ??= { error }
				}
			)
		)
	}

	/** Wait for every tracked send; throws the first failure. */
	async flush(): Promise<void> {
		// Acknowledgement callbacks may track further sends, hence the loop.
		while (this.pending.length > 0) {
			const pending = this.pending
			this.pending = []
			await Promise.all(pending)
		}
		const failure = this.failure
		this.failure = null
		if (failure) throw failure.error
	}

	/** Forget the tracked sends (their outcome no longer matters: the work is being discarded). */
	discard(): void {
		this.generation += 1
		this.pending = []
		this.failure = null
	}
}
