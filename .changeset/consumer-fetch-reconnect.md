---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Recover consumer fetches after broker disconnects without blocking healthy brokers, and back off failed leader lookups.
