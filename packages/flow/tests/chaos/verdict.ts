/**
 * What a chaos scenario passes on: the output summaries per mode, the at-least-once and exactly-once rules, and the
 * worker faults that fail every scenario.
 */
import type { Guarantee, Mode, Out, Rec, Worker } from './common.js'

export function summarizeStateless(inputs: Rec[], outs: Out[]) {
	const want = new Map<string, number>()
	for (const r of inputs) want.set(`m:${r.value}`, (want.get(`m:${r.value}`) ?? 0) + 1)
	const seen = new Map<string, number>()
	for (const o of outs) seen.set(o.value, (seen.get(o.value) ?? 0) + 1)
	let missing = 0
	let dupes = 0
	let unexpected = 0
	const missingSample: string[] = []
	const dupSample: string[] = []
	for (const [v, w] of want) {
		const c = seen.get(v) ?? 0
		if (c < w) {
			missing += w - c
			if (missingSample.length < 5) missingSample.push(v)
		} else if (c > w) {
			dupes += c - w
			if (dupSample.length < 5) dupSample.push(`${v}x${c}`)
		}
	}
	for (const v of seen.keys()) if (!want.has(v)) unexpected++
	return { inputs: inputs.length, outputs: outs.length, missing, missingSample, dupes, dupSample, unexpected }
}

/**
 * The join modes: every input record yields `m:<value>|<version>` (the stateless rules apply to `m:<value>`), and the
 * table versions one key joined never go back in output order (`regressions`): the table only moves forward, and under
 * exactly-once every committed result saw the table at least as new as the results committed before it.
 */
export function summarizeJoin(inputs: Rec[], outs: Out[]) {
	const latest = new Map<string, number>()
	const regressionSample: string[] = []
	let regressions = 0
	const values = outs.map(o => {
		const bar = o.value.lastIndexOf('|')
		const version = Number(o.value.slice(bar + 1))
		const before = latest.get(o.key) ?? 0
		if (version < before) {
			regressions++
			if (regressionSample.length < 5) regressionSample.push(`${o.value} after version ${before}`)
		}
		latest.set(o.key, Math.max(version, before))
		return { ...o, value: o.value.slice(0, bar) }
	})
	return { ...summarizeStateless(inputs, values), regressions, regressionSample }
}

/** Per-key count sequences in output order, checked against the expected 1..perKey (per-key override: expectedByKey). */
export function summarizeCount(
	keys: number,
	perKeyDefault: number,
	outs: Out[],
	expectedByKey?: Map<string, number>,
	keyPrefix = 'k'
) {
	const byKey = new Map<string, number[]>()
	for (const o of outs) {
		const a = byKey.get(o.key) ?? []
		a.push(Number(o.value))
		byKey.set(o.key, a)
	}
	let exactKeys = 0
	let belowKeys = 0
	let lastBelowKeys = 0
	let nonMonotonicKeys = 0
	const problems: string[] = []
	let maxBelow = 0
	let maxCount = 0
	for (let k = 0; k < keys; k++) {
		const a = byKey.get(`${keyPrefix}${k}`) ?? []
		const perKey = expectedByKey?.get(`k${k}`) ?? perKeyDefault
		const max = a.length ? Math.max(...a) : 0
		const last = a[a.length - 1] ?? 0
		const exact = a.length === perKey && a.every((v, i) => v === i + 1)
		maxCount = Math.max(maxCount, max)
		if (exact) exactKeys++
		if (last < perKey) lastBelowKeys++
		if (a.some((v, i) => i > 0 && v < a[i - 1]!)) nonMonotonicKeys++
		if (max < perKey) {
			belowKeys++
			maxBelow = Math.max(maxBelow, perKey - max)
		}
		if (!exact && problems.length < 6)
			problems.push(`k${k}: n=${a.length} max=${max} last=${last} seq=${compress(a)}`)
	}
	return {
		keys,
		perKey: perKeyDefault,
		exactKeys,
		belowKeys,
		lastBelowKeys,
		nonMonotonicKeys,
		maxBelow,
		maxCount,
		problems,
	}
}

export type Summary =
	| ReturnType<typeof summarizeStateless>
	| ReturnType<typeof summarizeJoin>
	| ReturnType<typeof summarizeCount>

/** The output of `mode` against its input (count modes: `keys` x `perKey`, or the per-key input counts given). */
export function summarize(
	mode: Mode,
	inputs: Rec[],
	outs: Out[],
	{ keys, perKey, expectedByKey }: { keys: number; perKey: number; expectedByKey?: Map<string, number> }
): Summary {
	switch (mode) {
		case 'stateless':
			return summarizeStateless(inputs, outs)
		case 'table-join':
		case 'global-join':
			return summarizeJoin(inputs, outs)
		case 'count':
			return summarizeCount(keys, perKey, outs, expectedByKey)
		case 'regroup':
			return summarizeCount(keys, perKey, outs, expectedByKey, 'nk')
	}
}

/** At-least-once rule: every input is reflected (nothing missing; every count reached its input count). */
export function drained(sum: Summary): boolean {
	return 'exactKeys' in sum ? sum.belowKeys === 0 && sum.lastBelowKeys === 0 : sum.missing === 0
}

/** Exactly-once rule: every input is reflected exactly once, in order. */
export function exact(sum: Summary): boolean {
	if ('exactKeys' in sum) return sum.exactKeys === sum.keys
	const regressions = 'regressions' in sum ? sum.regressions : 0
	return sum.missing === 0 && sum.dupes === 0 && sum.unexpected === 0 && regressions === 0
}

export const passes = (guarantee: Guarantee, sum: Summary) => (guarantee === 'exactly_once' ? exact(sum) : drained(sum))

/**
 * What fails every scenario besides its own rule: an unhandled rejection or uncaught exception in any worker, or a
 * worker whose last status is ERROR (the app halted; ERROR is final).
 */
export function workerFaults(workers: readonly Worker[]) {
	const unhandled = workers.flatMap(w => w.lines.filter(l => /UNHANDLED_REJECTION|UNCAUGHT_EXCEPTION/.test(l.type)))
	const errored = workers
		.filter(w => w.last('status')?.state === 'ERROR')
		.map(w => `${w.label}: ${w.last('status').error}`)
	return { ok: unhandled.length === 0 && errored.length === 0, unhandled, errored }
}

export function printFaults({ unhandled, errored }: ReturnType<typeof workerFaults>) {
	console.log(
		'  unhandled rejections/exceptions:',
		unhandled.length,
		unhandled.slice(0, 4).map(u => `${u.label}: ${u.message} @ ${u.stack}`)
	)
	console.log('  workers in ERROR:', JSON.stringify(errored))
}

function compress(a: number[]): string {
	if (a.length <= 30) return a.join(',')
	return a.slice(0, 12).join(',') + ',...,' + a.slice(-12).join(',')
}
