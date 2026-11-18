# Operations and Options

Kafka Streams configures operators with builder objects (`Consumed`, `Produced`, `Grouped`, `Materialized`, `Named`, `Branched`, ...). Flow takes the same settings as plain options objects, the last argument of each operator.

## Options Reference

### NamedOptions

Every operator takes `{ name }`. It names the operator's node and, where the operator has them, its stores and topics (an operator that registers several derives them as `<name>-<suffix>`, as Kafka Streams does).

```typescript
stream.filter((key, v) => v !== null, { name: 'drop-tombstones' })
```

Names are 1 to 249 ASCII letters, digits, `.`, `_` and `-` (they end up in topic names), and unique in the app: an illegal or duplicate name throws `TopologyError`. Without names, node, store and topic names are generated from counters in the order of the DSL calls, so adding an operator can rename what comes after it. See [Topology and Naming](/flow/topology#naming).

### Consumed

For `app.stream()`, `app.table()` and `app.globalTable()`:

| Option               | Description                                                                                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `key`, `value`       | Codecs. Without one, the source reads raw `Buffer`s.                                                                               |
| `timestampExtractor` | The timestamp of each record. Default: `FlowConfig.defaultTimestampExtractor`.                                                     |
| `offsetReset`        | `'earliest'`, `'latest'` or `'none'` for partitions without a committed offset. Default: `consumer.autoOffsetReset ?? 'earliest'`. |
| `name`               | The source node (for a table: `<name>-source`, and `<name>` for the table node).                                                   |

`app.table()` and `app.globalTable()` also take `materialized` (a [`Materialized`](#materialized)).

### Produced

For `to()` and the deprecated `through()`:

| Option         | Description                                                                                                       |
| -------------- | ----------------------------------------------------------------------------------------------------------------- |
| `key`, `value` | Codecs. Default: the stream's.                                                                                    |
| `partitioner`  | `(key, value, partitionCount) => number \| undefined`; `undefined` leaves the record to the default partitioning. |
| `name`         | The sink node.                                                                                                    |

### Grouped

For `groupBy()` and `groupByKey()` on streams and `groupBy()` on tables:

| Option         | Description                                                                                                                                         |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key`, `value` | Codecs of the grouped records (written to the repartition topic). `KTable.groupBy()` requires `key`.                                                |
| `name`         | The repartition topic `<applicationId>-<name>-repartition`, shared by every aggregation of the grouping, and the key-selecting node of `groupBy()`. |

### Materialized

How a table is stored. Used by `app.table()`, `toTable()`, and in `materialized` of table operations:

| Option         | Description                                                                                                                                             |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `storeName`    | The store, its changelog `<applicationId>-<storeName>-changelog` and its persisted state. Name stores whose state must survive changes of the topology. |
| `key`, `value` | Codecs of the store.                                                                                                                                    |
| `changelog`    | `true` (default), `false`, or a `ChangelogConfig`. See [Changelog topics](/flow/state-stores#changelog-topics).                                         |
| `retention`    | Windowed aggregations: how long windows are kept. See [Retention](/flow/windowing#retention).                                                           |

### AggregateOptions and WindowedAggregateOptions

`count()`, `reduce()` and `aggregate()` take a `Materialized` and a `name`. Windowed aggregations also take `emit`: `'on-update'` (default) forwards every update of a window, `'on-window-close'` only its final result once the window closed. See [Emit on window close](/flow/windowing#emit-on-window-close).

```typescript
grouped.count({ name: 'count-clicks', storeName: 'clicks-per-user' })
grouped.windowedBy(TimeWindows.ofSizeWithNoGrace('1m')).count({ emit: 'on-window-close' })
```

### TableOptions

For `KTable.filter()`, `filterNot()`, `mapValues()`, `mapValuesWithKey()` and table joins: `{ name, materialized }`. With `materialized.storeName` the result table is materialized. `transformValues()` also takes `stores`.

### StreamJoined

For stream-stream joins (the fourth argument, after the `JoinWindows`):

| Option                       | Description                                                                                                   |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `key`, `value`, `otherValue` | Codecs of the key and of both sides' values (required where the streams do not know them).                    |
| `storeName`                  | Base name of the join stores: `<storeName>-this-join-store`, `<storeName>-other-join-store`. Default: `name`. |
| `changelog`                  | Changelog backing of the join stores.                                                                         |
| `name`                       | Names the join's nodes and the repartition topics of re-keyed sides (`<name>-left`, `<name>-right`).          |

### Joined

For stream-table joins: `key`, `value`, `otherValue` codecs and `name` (the join node, and the repartition topic of a re-keyed stream). The stream-stream join overloads taking `Joined` with `within` are deprecated; see [Joins](/flow/joins#deprecated-forms).

### Repartitioned

For `repartition()`: `name`, `key` and `value` codecs, `numberOfPartitions`, `partitioner`. See [repartition](/flow/streams#repartition).

### SuppressOptions

For `KTable.suppress()`: `{ until: 'window-close' }` or `{ until: 'time-limit', timeLimit }`, with `maxRecords`, `maxBytes`, `whenFull` and `name`. See [Suppression](/flow/suppress).

### SplitOptions and BranchOptions

For `split({ name })` and `branch(predicate, { name, chain })`. See [Splitting a stream](/flow/streams#splitting-a-stream).

### ProcessOptions and TransformValuesOptions

For `process()`, `processValues()` and `KTable.transformValues()`: `name` and `stores`, the state stores the processor uses. See the [Processor API](/flow/processor-api).

## Durations

Windows, grace periods, retention and suppress time limits take a `WindowDuration`: milliseconds, or a number with a unit.

```typescript
TimeWindows.ofSizeAndGrace('5m', '30s')
TimeWindows.ofSizeAndGrace(300_000, 30_000) // the same
// units: 'ms', 's', 'm', 'h', 'd'; fractions allowed: '1.5s'
```

A duration must come out as a whole, non-negative number of milliseconds (`JoinWindows.before()` and `after()` may be negative), otherwise the factory throws `TopologyError`.

## How Codecs Are Resolved

Flow tracks the codecs it knows for a stream or table at each point of the topology:

- A source knows the codecs it was given (else it reads raw `Buffer`s).
- An operator that changes keys (`map`, `selectKey`, `flatMap`, `process`, `groupBy`, `toStream(mapper)`) forgets the key codec; one that changes values (`mapValues`, `flatMapValues`, `processValues`, joins, `map`) forgets the value codec. Operators that change neither keep both.
- An aggregation's result has the codecs of its store: the key codec of the grouping, and its value codec (`count()`: int64; `reduce()`: the value codec; `aggregate()`: `value`, required when the aggregate type differs from the value type).
- Windowed aggregations key their results with a windowed codec of the base key's codec; see [Windowed keys](/flow/windowing#windowed-keys).

An operator option always wins. An operator that has to encode something (a repartition topic, a store) and finds no codec throws `TopologyError` naming itself and the missing codec, from the DSL call that adds it:

```typescript
stream
	.selectKey((key, order) => order!.customerId)
	// the key changed: groupByKey() must repartition, and needs the new key's codec
	.groupByKey({ key: string() })
	.count()
```

A sink without a codec writes keys and values as they are: that works only for `Buffer`s (and `null`); anything else is a serialization error, which goes to the [production exception handler](/flow/error-handling#production-exceptions).

## Operation Catalog

| Operation                                | On                     | Result                         | Stateful | Repartitions                    |
| ---------------------------------------- | ---------------------- | ------------------------------ | -------- | ------------------------------- |
| `filter`, `filterNot`                    | KStream                | KStream                        | no       | no                              |
| `filter`, `filterNot`                    | KTable                 | KTable                         | optional | no                              |
| `map`, `flatMap`, `selectKey`            | KStream                | KStream                        | no       | marks downstream                |
| `mapValues`, `flatMapValues` (`WithKey`) | KStream                | KStream                        | no       | no                              |
| `mapValues`, `mapValuesWithKey`          | KTable                 | KTable                         | optional | no                              |
| `transformValues`                        | KTable                 | KTable                         | optional | no                              |
| `peek`, `foreach`                        | KStream                | KStream / none                 | no       | no                              |
| `merge`, `split`, `branch`               | KStream                | KStream(s)                     | no       | no                              |
| `process`, `processValues`               | KStream                | KStream                        | optional | `process` marks downstream      |
| `to`, `through`, `repartition`           | KStream                | none / KStream                 | no       | `repartition` always            |
| `toTable`                                | KStream                | KTable                         | yes      | if keys changed                 |
| `toStream`                               | KTable                 | KStream                        | no       | with a mapper, marks downstream |
| `groupByKey`                             | KStream                | KGroupedStream                 | -        | if keys changed                 |
| `groupBy`                                | KStream, KTable        | KGroupedStream / KGroupedTable | -        | always                          |
| `count`, `reduce`, `aggregate`           | grouped streams/tables | KTable                         | yes      | -                               |
| `windowedBy`                             | KGroupedStream         | windowed stream                | -        | -                               |
| `join`, `leftJoin`, `outerJoin` (stream) | KStream                | KStream                        | yes      | re-keyed sides                  |
| `join`, `leftJoin` (table)               | KStream                | KStream                        | yes      | a re-keyed stream               |
| `join`, `leftJoin` (global table)        | KStream                | KStream                        | no*      | never                           |
| `join`, `leftJoin`, `outerJoin`          | KTable                 | KTable                         | optional | never                           |
| `suppress`                               | KTable                 | KTable                         | yes      | no                              |

\* The global table itself is state every instance holds; the join keeps no state of its own.

"Marks downstream": the operator may change keys, so the next key-based operation (`groupByKey`, a join with a stream or table, `toTable`) repartitions first. See [Repartitioning](/flow/streams#repartitioning).
