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
	/** Sends that have not been answered yet. None of these promises rejects. */
	private readonly pending = new Set<Promise<void>>()
	/** The first failure since the last discard(). It stays until then: every flush() reports it. */
	private failure: { error: unknown } | null = null
	/** Incremented by discard(), so that sends discarded while in flight cannot fail later work. */
	private generation = 0

	/**
	 * Track a send. `onAck` runs when it is acknowledged, unless the send was discarded in the
	 * meantime: an acknowledgement that arrives after its work was rolled back must not be
	 * credited to whatever is being processed by then. A failing `onAck` counts as a failed send.
	 */
	track<T>(send: Promise<T>, onAck?: (result: T) => void | Promise<void>): void {
		const generation = this.generation
		const tracked: Promise<void> = send
			.then(result => {
				if (onAck && generation === this.generation) return onAck(result)
			})
			.catch(error => {
				if (generation === this.generation) this.failure ??= { error }
			})
			.then(() => {
				this.pending.delete(tracked)
			})
		this.pending.add(tracked)
	}

	/** Wait until every tracked send has been answered, whatever the answer. */
	async settled(): Promise<void> {
		// Acknowledgement callbacks may track further sends, hence the loop.
		while (this.pending.size > 0) {
			await Promise.all(this.pending)
		}
	}

	/**
	 * Wait for every tracked send; throws if one has failed since the last discard(). Callers
	 * that flush concurrently (partitions processed in parallel) all wait for all sends and all
	 * see the failure.
	 */
	async flush(): Promise<void> {
		await this.settled()
		if (this.failure) throw this.failure.error
	}

	/**
	 * Forget the tracked sends: their work is being rolled back, so neither their failures nor
	 * their acknowledgement callbacks count any more. Resolves once they have all been answered.
	 */
	discard(): Promise<void> {
		this.generation += 1
		this.failure = null
		const discarded = [...this.pending]
		this.pending.clear()
		return Promise.all(discarded).then(() => {})
	}
}
