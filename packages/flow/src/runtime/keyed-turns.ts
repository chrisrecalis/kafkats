/**
 * Runs tasks with the same key one after another, in the order they were asked for, and tasks with
 * different keys concurrently.
 */
export class KeyedTurns<K> {
	/** Per key with tasks running or waiting: settles (never rejects) when the last of them has. */
	private readonly last = new Map<K, Promise<void>>()

	run<T>(key: K, task: () => Promise<T>): Promise<T> {
		const previous = this.last.get(key)
		const turn = previous ? previous.then(task) : task()
		const settled = turn.then(
			() => {},
			() => {}
		)
		this.last.set(key, settled)
		void settled.then(() => {
			if (this.last.get(key) === settled) this.last.delete(key)
		})
		return turn
	}
}
