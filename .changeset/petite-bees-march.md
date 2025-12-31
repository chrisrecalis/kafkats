---
'@kafkats/client': minor
'@kafkats/codec-zod': minor
'@kafkats/flow': minor
'@kafkats/flow-state-lmdb': minor
---

- perf(client): optional native CRC32C via @node-rs/crc32 (now an optional peer dependency) with JS fallback; faster Record/RecordBatch encoding (more single-pass, fewer intermediate allocations); encoder uses Buffer.allocUnsafe; producer avoids per-message remapping. Also adds benchmark heap snapshot runners and updates docs.
- chore(packages): add per-package README.md plus npm metadata (keywords, homepage, repository, bugs) for @kafkats/client, @kafkats/flow, @kafkats/codec-zod,
  @kafkats/flow-state-lmdb to improve npm package visibility.
