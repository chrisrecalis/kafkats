---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Add `transactionConcurrency` to flow config. A producer can only hold one open transaction, so an
exactly-once stream thread processed its whole assignment serially. Setting `transactionConcurrency`
gives the thread that many producers, sharding assigned partitions across them by partition index, so
several transactions commit in parallel. Defaults to 1, which keeps existing transactional IDs and
behaviour unchanged. `start()` rejects `transactionConcurrency > 1` for topologies that build state
downstream of a key change, since such state is not aligned with the source partitioning.

Key changes are now tracked through `KStream`, so `selectKey()` and `map()` ahead of a stateful
operator mark the resulting store as not partition-aligned. Previously only `groupBy()` did, which
also meant changelog restoration for those stores was incorrectly restricted to the assigned source
partitions.
