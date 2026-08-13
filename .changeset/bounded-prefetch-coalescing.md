---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Bounded consumer prefetching and fetch-response coalescing. Before issuing a Fetch, the background loop now reserves part of the shared `maxBufferedBytes` budget — concurrent broker fetches split the remaining capacity between them, and the loop parks (instead of polling on a timer) until `poll()` drains records or partitions are removed, preventing unbounded buffer growth. On drain, responses for the same topic-partition and assignment epoch are coalesced so Kafka fetch-response boundaries no longer surface as separate `runBatch` invocations (and separate EOS transactions); responses from different rebalance epochs stay separate to preserve fencing.
