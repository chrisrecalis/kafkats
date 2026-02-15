---
'@kafkats/flow': minor
'@kafkats/client': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Fix flow integrity for KGroupedTable aggregations

**Breaking:** `KGroupedTable.reduce()` now requires `(adder, subtractor)` and `aggregate()` requires `(initializer, adder, subtractor)` matching Kafka Streams semantics. This ensures correct retraction behavior when source table rows are updated or deleted.

- Add delta retraction protocol for table groupBy: emits SUB/ADD headers so downstream aggregation nodes can properly subtract old values and add new values
- Store both grouped key and source value in the mapping store (`GroupedTableMapping`) so retractions carry the correct old value
- Add `TableDeltaCountNode`, `TableDeltaReduceNode`, `TableDeltaAggregateNode` for delta-aware aggregations
- Reorder changelog write-before-local-store in all store wrappers to reduce inconsistency window on crash
- Add `restrictRestorationToSourcePartitions` flag to prevent incorrect partition filtering during changelog restoration for re-keyed stores
- Fix checkpoint offset in exactly_once mode to prevent skipping records on restoration
