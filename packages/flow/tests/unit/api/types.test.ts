import { describe, expectTypeOf, it } from 'vitest'
import type { FlowApp } from '@/api/app.js'
import type { KGroupedStream, SessionWindowedKStream, TimeWindowedKStream, Windowed } from '@/api/grouped.js'
import type { KStream } from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import { codec } from '@/codec.js'
import type { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import type { FixedKeyProcessor, Processor } from '@/processor/api.js'
import type { KeyValueStore, StateStore, TimestampedWindowStore } from '@/state.js'
import { Stores } from '@/state/store-builders.js'

/** Compile-time checks of the public overloads; never called. */
function overloads(
	app: FlowApp,
	stream: KStream<string, number>,
	other: KStream<string, boolean>,
	table: KTable<string, Date>,
	global: GlobalKTable<number, Date>,
	byName: GlobalKTable<string, Date>,
	windows: { join: JoinWindows; time: TimeWindows; session: SessionWindows; sliding: SlidingWindows }
) {
	expectTypeOf(stream.mapValues(value => String(value))).toEqualTypeOf<KStream<string, string>>()
	expectTypeOf(stream.mapValuesWithKey((key, value) => [key, value])).toEqualTypeOf<
		KStream<string, (string | number | null)[]>
	>()
	expectTypeOf(table.mapValuesWithKey((key, value) => `${key}${value.getTime()}`)).toEqualTypeOf<
		KTable<string, string>
	>()
	expectTypeOf(stream.selectKey((key, value) => `${key}:${value}`)).toEqualTypeOf<KStream<string, number>>()

	expectTypeOf(stream.join(other, (value, otherValue) => [value, otherValue] as const, windows.join)).toEqualTypeOf<
		KStream<string, readonly [number, boolean]>
	>()
	expectTypeOf(stream.leftJoin(other, (_value, otherValue) => otherValue, windows.join)).toEqualTypeOf<
		KStream<string, boolean | null>
	>()
	expectTypeOf(
		stream.join(other, (value, otherValue) => value + Number(otherValue), { within: windows.time })
	).toEqualTypeOf<KStream<string, number>>()
	expectTypeOf(stream.join(table, (value, tableValue) => tableValue.getTime() + value)).toEqualTypeOf<
		KStream<string, number>
	>()
	expectTypeOf(stream.leftJoin(table, (_value, tableValue) => tableValue)).toEqualTypeOf<
		KStream<string, Date | null>
	>()
	expectTypeOf(
		stream.join(
			global,
			(_key, value) => value,
			(value, globalValue) => globalValue.getTime() + value
		)
	).toEqualTypeOf<KStream<string, number>>()
	expectTypeOf(
		stream.leftJoin(
			global,
			(_key, value) => value,
			(key: string | null, value: number, globalValue: Date | null) => [key, value, globalValue] as const
		)
	).toEqualTypeOf<KStream<string, readonly [string | null, number, Date | null]>>()
	const recordKey = (key: string | null) => key
	// @ts-expect-error the selector must pick a key of the global table
	stream.join(global, recordKey, (value: number) => value)
	expectTypeOf(stream.leftJoin(byName, (_value, globalValue) => globalValue)).toEqualTypeOf<
		KStream<string, Date | null>
	>()
	// @ts-expect-error the deprecated form needs a global table keyed like the stream
	stream.join(global, (value: number) => value)
	expectTypeOf(
		stream.outerJoin(other, (value, otherValue) => [value, otherValue] as const, windows.join)
	).toEqualTypeOf<KStream<string, readonly [number | null, boolean | null]>>()

	const grouped = stream.groupByKey()
	expectTypeOf(grouped).toEqualTypeOf<KGroupedStream<string, number>>()
	expectTypeOf(
		grouped.aggregate(
			() => 0,
			(_key, value, sum) => sum + value
		)
	).toEqualTypeOf<KTable<string, number>>()
	expectTypeOf(
		grouped.aggregate(
			() => '',
			(_key, value, text) => text + value,
			{ value: codec.string() }
		)
	).toEqualTypeOf<KTable<string, string>>()
	grouped.aggregate(
		// @ts-expect-error aggregates of another type than the values need their codec
		() => '',
		(_key, value, text) => text + value
	)
	expectTypeOf(grouped.windowedBy(windows.time)).toEqualTypeOf<TimeWindowedKStream<string, number>>()
	expectTypeOf(grouped.windowedBy(windows.session)).toEqualTypeOf<SessionWindowedKStream<string, number>>()
	// Time and session windows pass null values on (Kafka Streams); sliding windows drop them.
	expectTypeOf(
		grouped.windowedBy(windows.time).aggregate(
			() => 0,
			(_key, value: number | null, sum) => sum + (value ?? 0)
		)
	).toEqualTypeOf<KTable<Windowed<string>, number>>()
	expectTypeOf(
		grouped.windowedBy(windows.session).aggregate(
			() => 0,
			(_key, value: number | null, aggregate) => aggregate + (value ?? 0),
			(_key, a, b) => a + b
		)
	).toEqualTypeOf<KTable<Windowed<string>, number>>()
	expectTypeOf(
		grouped.windowedBy(windows.time).reduce((sum, value: number | null) => sum + (value ?? 0))
	).toEqualTypeOf<KTable<Windowed<string>, number>>()
	// @ts-expect-error a time-window reducer receives null values
	grouped.windowedBy(windows.session).reduce((sum: number, value: number) => sum + value)
	expectTypeOf(
		grouped.windowedBy(windows.sliding).aggregate(
			() => 0,
			(_key, value: number, sum) => sum + value
		)
	).toEqualTypeOf<KTable<Windowed<string>, number>>()
	grouped.windowedBy(windows.time).aggregate(
		// @ts-expect-error windowed aggregates of another type than the values need their codec too
		() => '',
		(_key, value, text) => `${text}${value}`
	)

	// Deletes reach transformValues() and toStream(mapper) as null values, no other table callback.
	table.filter((_key, value) => value.getTime() > 0)
	table.mapValues(value => value.getTime())
	// @ts-expect-error the mapper of toStream() gets the null value of a delete
	table.toStream((_key, value: Date) => value.getTime())
	table.transformValues(() => ({ transform: (_key, value) => expectTypeOf(value).toEqualTypeOf<Date | null>() }))
	expectTypeOf(table.toStream()).toEqualTypeOf<KStream<string, Date>>()
	expectTypeOf(table.toStream((key, value) => `${key}${value?.getTime()}`)).toEqualTypeOf<KStream<string, Date>>()
	expectTypeOf(table.groupBy((key, value) => [value.getTime(), key] as const)).toMatchTypeOf<unknown>()

	expectTypeOf(app.globalTable<number, Date>('dates')).toEqualTypeOf<GlobalKTable<number, Date>>()
	// @ts-expect-error a global table cannot be streamed
	app.globalTable('dates').toStream()
	// @ts-expect-error mapValues() passes the value alone: a mapper requiring the key is mapValuesWithKey()'s
	stream.mapValues((key: string | null, value: number | null) => [key, value])

	// Processor API: typed stores and processors.
	const counts = Stores.keyValueStore({ name: 'counts', key: codec.string(), value: codec.json<number>() })
	const windowStore = Stores.timestampedWindowStore({
		name: 'windows',
		key: codec.string(),
		value: codec.string(),
		retention: '1h',
		windowSize: '1m',
	})
	const process: Processor<string | null, number | null, boolean, string> = {
		init(context) {
			expectTypeOf(context.getStateStore(counts)).toEqualTypeOf<KeyValueStore<string, number>>()
			expectTypeOf(context.getStateStore(windowStore)).toEqualTypeOf<TimestampedWindowStore<string, string>>()
			expectTypeOf(context.getStateStore('counts')).toEqualTypeOf<StateStore>()
		},
		process() {},
	}
	expectTypeOf(stream.process(() => process, { stores: [counts, 'other'] })).toEqualTypeOf<KStream<boolean, string>>()
	expectTypeOf(
		stream.processValues((): FixedKeyProcessor<string | null, number | null, Date> => ({ process() {} }))
	).toEqualTypeOf<KStream<string, Date>>()
	expectTypeOf(
		table.transformValues(() => ({ transform: (key, value) => `${key}${value?.getTime()}` }))
	).toEqualTypeOf<KTable<string, string>>()
	expectTypeOf(app.addStateStore(counts)).toEqualTypeOf<FlowApp>()
}

describe('public API types', () => {
	it('type-checks the overloads', () => {
		expectTypeOf(overloads).toBeFunction()
	})
})
