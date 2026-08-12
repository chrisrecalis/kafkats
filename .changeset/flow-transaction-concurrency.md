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
behaviour unchanged.
