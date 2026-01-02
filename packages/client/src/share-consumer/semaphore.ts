export class Semaphore {
	private available: number
	private readonly waiting: Array<{ resolve: () => void }> = []

	constructor(capacity: number) {
		this.available = capacity
	}

	async acquire(signal?: AbortSignal): Promise<boolean> {
		if (this.available > 0) {
			this.available--
			return true
		}

		return new Promise<boolean>(resolve => {
			let onAbort: (() => void) | null = null

			const cleanup = () => {
				if (signal && onAbort) {
					signal.removeEventListener('abort', onAbort)
				}
				onAbort = null
			}

			const entry = {
				resolve: () => {
					cleanup()
					resolve(true)
				},
			}
			this.waiting.push(entry)

			if (signal) {
				onAbort = () => {
					const idx = this.waiting.indexOf(entry)
					if (idx !== -1) {
						this.waiting.splice(idx, 1)
					}
					cleanup()
					resolve(false)
				}

				if (signal.aborted) {
					const idx = this.waiting.indexOf(entry)
					if (idx !== -1) {
						this.waiting.splice(idx, 1)
					}
					cleanup()
					resolve(false)
					return
				}

				signal.addEventListener('abort', onAbort)
			}
		})
	}

	release(): void {
		const next = this.waiting.shift()
		if (next) {
			next.resolve()
			return
		}
		this.available++
	}
}
