---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Require stable OffsetFetch v7 reads when initializing assigned consumer partitions so pending transactional offset
commits cannot cause cross-owner duplicate processing during rebalances.
