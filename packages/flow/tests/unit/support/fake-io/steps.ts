/**
 * Scripted answers of faked calls: each faked operation has a `Steps` the test tells how its next calls end.
 */
import { raceSignal } from '@/runtime/signals.js'

/**
 * How one faked call ends. 'ok': answered at once (its effect applies, it resolves). An Error: rejected at once with
 * it, without effect. 'hang': not answered until the test answers it (`Steps.hung`); a call that takes a signal
 * rejects with an AbortError once its signal aborts, as the Kafka adapters race their calls (3.6). A function: called
 * when the call is made, its result is the outcome (side effects at call time).
 */
export type Outcome = 'ok' | 'hang' | Error | (() => Outcome)

/** A call answered 'hang', waiting for the test. */
export interface HungCall {
	/**
	 * 'ok' applies the call's effect and resolves it, an Error rejects it; later answers do nothing. A call whose
	 * signal aborted already rejected: answering it 'ok' still applies its effect, as a broker that handles a
	 * request after its sender gave up.
	 */
	answer(outcome?: 'ok' | Error): void
}

/** The outcomes of one faked operation, one per call: the queued ones in order (`next`), then the standing one. */
export class Steps {
	/** Calls answered 'hang' and not answered by the test yet, in call order. */
	readonly hung: HungCall[] = []
	private readonly queued: Outcome[] = []
	private standing: Outcome = 'ok'

	/** The next calls end with these, in order. */
	next(...outcomes: Outcome[]): this {
		this.queued.push(...outcomes)
		return this
	}

	/** Every call after the queued ones ends with this (initially 'ok'). */
	always(outcome: Outcome): this {
		this.standing = outcome
		return this
	}

	/** Answers every hung call. */
	answerAll(outcome: 'ok' | Error = 'ok'): void {
		while (this.hung.length > 0) this.hung[0]!.answer(outcome)
	}

	/**
	 * Makes one call: `effect` runs once the call is answered 'ok' and its result resolves the call; an effect that
	 * throws rejects it. A call whose signal aborted before it was made rejects with an AbortError, takes no outcome
	 * and has no effect.
	 */
	perform<T>(effect: () => T, signal?: AbortSignal): Promise<T> {
		if (signal?.aborted) {
			return raceSignal(new Promise<T>(() => {}), signal)
		}
		let outcome = this.queued.shift() ?? this.standing
		while (typeof outcome === 'function') outcome = outcome()
		if (outcome === 'ok') {
			return attempt(effect)
		}
		if (outcome !== 'hang') {
			return Promise.reject(outcome)
		}
		const answered = new Promise<T>((resolve, reject) => {
			let settled = false
			const call: HungCall = {
				answer: (answer = 'ok') => {
					if (settled) return
					settled = true
					this.hung.splice(this.hung.indexOf(call), 1)
					if (answer === 'ok') attempt(effect).then(resolve, reject)
					else reject(answer)
				},
			}
			this.hung.push(call)
		})
		return signal ? raceSignal(answered, signal) : answered
	}
}

function attempt<T>(effect: () => T): Promise<T> {
	try {
		return Promise.resolve(effect())
	} catch (error) {
		return Promise.reject(error)
	}
}

export interface Deferred<T> {
	readonly promise: Promise<T>
	resolve(value: T): void
	reject(error: unknown): void
}

export function deferred<T = void>(): Deferred<T> {
	let resolve!: (value: T) => void
	let reject!: (error: unknown) => void
	const promise = new Promise<T>((res, rej) => {
		resolve = res
		reject = rej
	})
	return { promise, resolve, reject }
}
