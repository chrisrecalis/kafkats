# Topology and Naming

The DSL calls of an app describe a **topology**: source nodes reading topics, processor nodes, sink nodes writing topics, and the state stores processors use. This page covers how a topology is cut into tasks, how its nodes, stores and internal topics are named, and how to inspect it with `describe()`.

## Subtopologies and Tasks

When the app starts (or `describe()` is called), the topology is built and frozen: later DSL calls throw `TopologyError`. Building cuts it into **subtopologies**: nodes connected through parent edges, processors that share a state store, and sources of the same topic belong together; a topic written by a sink and read by a source (a repartition topic, a `through()` topic, or a topic the app writes with `to()` and reads with `stream()`) separates them.

Each subtopology runs as one **task** per partition of its input topics, with the id `<subtopology>_<partition>`. A task processes partition N of every input topic of its subtopology, with its own processor instances, its own instance of each state store (partition N of the store's changelog), its own stream time and its own committed offsets. Tasks are what the instances of an application divide among themselves: the consumer group assigns partitions, and an instance runs the tasks of the partitions it was assigned.

- The topics feeding a stateful subtopology must have the same partition count; `start()` throws `CoPartitioningError` otherwise. Repartition topics without an explicit count take the count of the topics they are co-partitioned with.
- A stateless subtopology may read topics with different partition counts: its task `x_N` reads partition N of each topic that has one.
- Changelog topics have as many partitions as their subtopology has tasks.

## Naming

Names matter for what outlives a deployment: **store names** key the persisted state and name the changelog topics; **internal topic names** are where repartitioned records and changelogs live. Node names only appear in `describe()`, logs and error handler contexts.

### Generated names

Without explicit names, everything is named from counters, in the order of the DSL calls, as Kafka Streams does:

| What                         | Generated name                                               |
| ---------------------------- | ------------------------------------------------------------ |
| Nodes                        | `KSTREAM-FILTER-0000000003`, `KTABLE-SOURCE-0000000001`, ... |
| Aggregation stores           | `count-store-<n>`, `reduce-store-<n>`, `aggregate-store-<n>`, `window-count-store-<n>`, ... |
| Table stores                 | `table-<topic>-<n>`, `global-table-<topic>-<n>`, `toTable-store-<n>` |
| Stream-stream join stores    | `stream-join-left-<n>`, `stream-join-right-<n>` (left joins `stream-left-join-...`, outer joins `stream-outer-join-...`) |
| Suppress buffers             | `KTABLE-SUPPRESS-STATE-STORE-<n>`                            |
| Repartition topics           | `<applicationId>-repartition-<n>-repartition`                |
| Changelog topics             | `<applicationId>-<storeName>-changelog`                      |

Adding, removing or reordering an operator can therefore change the generated names after it. A changed store name means a new, empty store (and changelog); a changed repartition topic means a new topic. Name the stores and repartitioning operations of an application that will evolve.

### Explicit names

```typescript
stream
	.selectKey((key, view) => view!.userId, { name: 'by-user' })
	.groupByKey({ key: string(), name: 'views-by-user' }) // topic <applicationId>-views-by-user-repartition
	.count({ name: 'count-views', storeName: 'views-per-user' }) // changelog <applicationId>-views-per-user-changelog
```

- Every operator takes `{ name }` ([NamedOptions](/flow/operations#namedoptions)). An operator that registers several nodes, stores or topics names them `<name>-<suffix>`: a named inner stream-stream join has the nodes `<name>-this-join`, `<name>-other-join` and `<name>-merge`, the stores `<name>-this-join-store` and `<name>-other-join-store`, and the repartition topics `<name>-left` and `<name>-right`; a named `repartition()` has `<name>-repartition-sink` and `<name>-repartition-source`; a named table source is `<name>` with its source node `<name>-source`.
- Stores are named by `Materialized.storeName` (or the `storeName` of an aggregation), a `Stores` definition's `name`, `StreamJoined.storeName`, or `<name>-store` for `suppress()`.
- Repartition topics are named by `Grouped.name`, `Repartitioned.name`, `Joined.name` / `StreamJoined.name`, or, for an aggregation of an unnamed grouping, its `storeName`.
- Names are 1 to 249 ASCII letters, digits, `.`, `_` and `-`, not `.` or `..`. An illegal name, two nodes or two stores of one name, or one repartition name claimed by two different operations throws `TopologyError` when the topology is built. All aggregations of one named grouping share its repartition topic.

## describe()

`app.describe()` builds the topology (freezing it, as `start()` does) and returns its description in Kafka Streams' shape: subtopologies with their nodes, and global stores. An invalid topology throws its `TopologyError` here, which makes `describe()` a cheap check in a unit test.

```typescript
const app = flow({ applicationId: 'page-views', client })

const users = app.table('users', { key: string(), value: json<User>() })
app.stream('page-views', { key: string(), value: json<PageView>() })
	.selectKey((key, view) => view!.user, { name: 'by-user' })
	.join(users, (view, user) => ({ ...view, region: user.region }), { name: 'with-region', key: string() })
	.groupBy((key, view) => view!.region, { key: string(), value: json(), name: 'by-region' })
	.windowedBy(TimeWindows.ofSizeAndGrace('1m', '10s'))
	.count({ storeName: 'views-per-region' })
	.toStream()
	.to('views-per-region')

console.log(app.describe().toString())
```

```
Topologies:
   Sub-topology: 0
    Source: with-region-repartition-source (topics: [page-views-with-region-repartition])
      --> with-region
    Processor: with-region (stores: [table-users-0])
      --> by-region
      <-- with-region-repartition-source
    Source: KSTREAM-SOURCE-0000000000 (topics: [users])
      --> KTABLE-SOURCE-0000000001
    Processor: by-region (stores: [])
      --> by-region-repartition-sink
      <-- with-region
    Processor: KTABLE-SOURCE-0000000001 (stores: [table-users-0])
      --> none
      <-- KSTREAM-SOURCE-0000000000
    Sink: by-region-repartition-sink (topic: page-views-by-region-repartition)
      <-- by-region

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000002 (topics: [page-views])
      --> by-user
    Processor: by-user (stores: [])
      --> with-region-repartition-sink
      <-- KSTREAM-SOURCE-0000000002
    Sink: with-region-repartition-sink (topic: page-views-with-region-repartition)
      <-- by-user

  Sub-topology: 2
    Source: by-region-repartition-source (topics: [page-views-by-region-repartition])
      --> KSTREAM-AGGREGATE-0000000003
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [views-per-region])
      --> KTABLE-TOSTREAM-0000000004
      <-- by-region-repartition-source
    Processor: KTABLE-TOSTREAM-0000000004 (stores: [])
      --> KSTREAM-SINK-0000000005
      <-- KSTREAM-AGGREGATE-0000000003
    Sink: KSTREAM-SINK-0000000005 (topic: views-per-region)
      <-- KTABLE-TOSTREAM-0000000004
```

The key changed by `selectKey()` makes the join repartition the stream (`with-region`), and `groupBy()` repartitions again (`by-region`); each repartition topic ends one subtopology and starts another. The table `users` and the join that reads its store are in one subtopology, so `users` and `page-views-with-region-repartition` must have the same partition count, which flow gives the repartition topic.

The description is plain data (`JSON.stringify` keeps all of it):

```typescript
interface TopologyDescription {
	subtopologies: { id: number; nodes: TopologyNodeDescription[] }[]
	globalStores: { id: number; source: TopologyNodeDescription; processor: TopologyNodeDescription }[]
	toString(): string
}

interface TopologyNodeDescription {
	name: string
	kind: 'source' | 'processor' | 'sink'
	topics: string[] // sources: topics read; sinks: the topic written (none with a topic name extractor)
	stores: string[] // processors: connected stores
	predecessors: string[]
	successors: string[]
}
```

Nodes and neighbours are listed in Kafka Streams' order (more descendants first, then by name). Internal topics carry their full names. Global stores are numbered after the subtopologies.
