---
"@kafkats/client": patch
"@kafkats/flow": patch
"@kafkats/codec-zod": patch
"@kafkats/flow-state-lmdb": patch
---

fix(client): stop logging per-transaction events at info level

An exactly-once loop commits a transaction per batch, so `transaction committed` was
emitting at batch rate — several lines per second per producer at the default `info`
level, drowning out everything else in the log. `transaction committed`, `transaction
aborted`, and `sequence wrap - splitting batch` are now `debug`. Pass
`logLevel: 'debug'` in the client config to get them back.

Two fencing events moved the other way, from `info` to `warn`: `producer fenced` and
`share group member fenced; abandoning assignment and rejoining`. Both are abnormal
conditions that force a re-init or reassignment, and both were previously invisible to
anyone filtering at `warn` to escape the noise above.

One-shot lifecycle logging (connect/disconnect, group join/leave, producer init) is
unchanged and still `info`.
