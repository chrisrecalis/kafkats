/**
 * Runs a task on request, never twice at the same time: requests that arrive while it runs share
 * a single run that starts once the current one has finished.
 *
 * For work where one run covers everything requested before it started, such as syncing a log.
 */
export class Coalesced {
	private running: Promise<void> | null = null
	private next: Promise<void> | null = null

	constructor(private readonly task: () => Promise<void>) {}

	request(): Promise<void> {
		if (!this.running) return this.start()
		// The current run may have started before this request, so it does not count for it.
		this.next ??= this.running
			.catch(() => {})
			.then(() => {
				this.next = null
				return this.start()
			})
		return this.next
	}

	private start(): Promise<void> {
		const run = this.task().finally(() => {
			// With a run queued behind this one, `running` stays set until that run replaces it.
			if (this.running === run && !this.next) this.running = null
		})
		this.running = run
		return run
	}
}
