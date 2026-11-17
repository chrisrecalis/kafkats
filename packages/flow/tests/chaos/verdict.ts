/**
 * What a chaos scenario passes on: the output summaries per mode, the at-least-once and exactly-once rules, and the
 * worker faults that fail every scenario.
 */
import {
	genRight,
	joinPartner,
	TICK_KEY,
	type Guarantee,
	type Mode,
	type Out,
	type Rec,
	type Worker,
} from './common.js'

/** The output values against the values expected (as a multiset): what is missing, duplicated or unexpected. */
export function summarizeValues(expected: readonly string[], outs: Out[]) {
	const want = new Map<string, number>()
	for (const v of expected) want.set(v, (want.get(v) ?? 0) + 1)
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
	return { inputs: expected.length, outputs: outs.length, missing, missingSample, dupes, dupSample, unexpected }
}

/** The stateless mode: every input record yields `m:<value>`. */
export const summarizeStateless = (inputs: Rec[], outs: Out[]) =>
	summarizeValues(
		inputs.map(r => `m:${r.value}`),
		outs
	)

/**
 * The `join` mode: every input record yields exactly one result, `m:<value>|<right value>` with the right record it
 * joins (`joinPartner`), else `m:<value>|null` once its window closed. Tick results are not checked.
 */
export function summarizeWindowedJoin(inputs: Rec[], outs: Out[], keys: number, perKey: number) {
	const right = genRight(keys, perKey)
	const expected = inputs.map(r => `m:${r.value}|${joinPartner(r, right)?.value ?? 'null'}`)
	const matched = expected.filter(v => !v.endsWith('|null')).length
	return {
		...summarizeValues(
			expected,
			outs.filter(o => o.key !== TICK_KEY)
		),
		matched,
	}
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

/**
 * Count sequences per key in output order against the number of records each key counts: exactly 1..n under
 * exactly-once; under at-least-once the last and the max count reach n.
 */
export function summarizeCount(expected: ReadonlyMap<string, number>, outs: Out[]) {
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
	for (const [key, n] of expected) {
		const a = byKey.get(key) ?? []
		const max = a.length ? Math.max(...a) : 0
		const last = a[a.length - 1] ?? 0
		const exact = a.length === n && a.every((v, i) => v === i + 1)
		maxCount = Math.max(maxCount, max)
		if (exact) exactKeys++
		if (last < n) lastBelowKeys++
		if (a.some((v, i) => i > 0 && v < a[i - 1]!)) nonMonotonicKeys++
		if (max < n) {
			belowKeys++
			maxBelow = Math.max(maxBelow, n - max)
		}
		if (!exact && problems.length < 6)
			problems.push(`${key}: n=${a.length}/${n} max=${max} last=${last} seq=${compress(a)}`)
	}
	const unexpectedKeys = [...byKey.keys()].filter(key => !expected.has(key)).length
	return {
		keys: expected.size,
		exactKeys,
		belowKeys,
		lastBelowKeys,
		nonMonotonicKeys,
		maxBelow,
		maxCount,
		unexpectedKeys,
		problems,
	}
}

/** How many records each output key counts: the input key (count), `n<key>` (regroup) or `<key>@<window start>` (window). */
function expectedCounts(inputs: Rec[], keyOf: (r: Rec) => string): Map<string, number> {
	const expected = new Map<string, number>()
	for (const r of inputs) expected.set(keyOf(r), (expected.get(keyOf(r)) ?? 0) + 1)
	return expected
}

/** The window of the `window` mode a record falls in: 1 s, aligned to the epoch. */
const windowStart = (timestamp: number) => timestamp - (timestamp % 1_000)

export type Summary =
	| ReturnType<typeof summarizeStateless>
	| ReturnType<typeof summarizeJoin>
	| ReturnType<typeof summarizeWindowedJoin>
	| ReturnType<typeof summarizeCount>

/**
 * The output of `mode` against the input records it got (the join mode's right records follow from `keys` and
 * `perKey`, see `genRight`).
 */
export function summarize(
	mode: Mode,
	inputs: Rec[],
	outs: Out[],
	{ keys, perKey }: { keys: number; perKey: number }
): Summary {
	switch (mode) {
		case 'stateless':
			return summarizeStateless(inputs, outs)
		case 'table-join':
		case 'global-join':
			return summarizeJoin(inputs, outs)
		case 'join':
			return summarizeWindowedJoin(inputs, outs, keys, perKey)
		case 'count':
			return summarizeCount(
				expectedCounts(inputs, r => r.key),
				outs
			)
		case 'regroup':
			return summarizeCount(
				expectedCounts(inputs, r => `n${r.key}`),
				outs
			)
		case 'window':
			return summarizeCount(
				expectedCounts(inputs, r => `${r.key}@${windowStart(r.timestamp!)}`),
				outs
			)
	}
}

/** At-least-once rule: every input is reflected (nothing missing; every count reached its input count). */
export function drained(sum: Summary): boolean {
	return 'exactKeys' in sum ? sum.belowKeys === 0 && sum.lastBelowKeys === 0 : sum.missing === 0
}

/** Exactly-once rule: every input is reflected exactly once, in order. */
export function exact(sum: Summary): boolean {
	if ('exactKeys' in sum) return sum.exactKeys === sum.keys && sum.unexpectedKeys === 0
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
