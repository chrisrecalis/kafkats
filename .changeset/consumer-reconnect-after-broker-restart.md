---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Reconnect consumer fetches after a partition leader's connection closes, instead of retrying the dead connection until the process restarts.
