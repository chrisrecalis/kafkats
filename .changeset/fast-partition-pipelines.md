---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Keep each consumer partition moving independently across buffered polls so a slow partition handler no longer blocks work already available for other partitions.
