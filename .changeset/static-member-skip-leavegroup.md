---
"@kafkats/client": patch
"@kafkats/flow": patch
"@kafkats/codec-zod": patch
"@kafkats/flow-state-lmdb": patch
---

fix(consumer): static members must not send LeaveGroup on shutdown (KIP-345)

A consumer configured with `groupInstanceId` (static membership) was sending a
`LeaveGroup` request on graceful shutdown. Per KIP-345 a static member must not do
this — sending `LeaveGroup` triggers an immediate group rebalance, defeating the
purpose of static membership (cheap rolling restarts, where the member is expected
to rejoin within `sessionTimeoutMs` with its existing partition assignment).

`ConsumerGroup.leave()` now skips the `LeaveGroup` request when `groupInstanceId` is
set and simply stops heartbeating, letting the broker hold the member's partitions
until it rejoins or the session times out. Dynamic members (no `groupInstanceId`)
continue to send `LeaveGroup` for an immediate rebalance.
