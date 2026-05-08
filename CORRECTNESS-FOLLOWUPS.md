# Correctness follow-ups — round 2

**Status as of 2026-05-08:**
- ✅ F1 — EOS rebalance offset-commit gap → merged in PR #69
- ✅ F2 — Cleanup never fires for WindowedReduceNode/SessionReduceNode (+ SessionAggregateNode + tombstones) → merged in PR #70
- ✅ F3 — `OffsetManager.commitPartitions` partial-failure → pivoted to deletion in PR #71 (Codex caught zero callers)
- ✅ F4 — Checkpoint errors silently swallowed (+ 2 sibling sites + log-spam dedupe + recovery line) → merged in PR #72
- ❌ F5 — Negative-window-start guard → **skipped + closed in PR #73**. The fix exposes a pre-existing LMDB byte-ordering bug (`writeBigInt64BE(-1)` = `0xFF…FF` sorts AFTER positive values in unsigned-byte order, breaking range scans on negative-start windows). Both pre-fix and post-fix bugs are unreachable in production (Kafka timestamps post-1970 epoch ms), so neither is worth shipping. Proper fix needs F5b: order-preserving signed integer encoding in LMDB time keys.
- ⏸ F6 — `expireOldWindows` strict-vs-inclusive boundary → **deferred**. The "inconsistency" is between range/fetch (inclusive on both ends) and expire (strict cutoff) — fundamentally different operation shapes; comparing them was apples-to-oranges. Probability of a window ending at exactly `currentTime - retentionMs` is academic at ms resolution. Not worth a PR.
- 🔜 F7 — Aborted-txn restoration integration test (would have caught #62's premise being wrong)
- 🔜 F8 — WindowStore/SessionStore put-then-changelog ordering parity tests
- ⏸ F9 — `ShareConsumer.stream()` test coverage → **lower priority** (share consumer is `@experimental`, requires integration scaffolding with broker)

### Newly-introduced follow-ups
- **F5b** — LMDB time-key encoding doesn't preserve signed-integer order. `writeBigInt64BE(-1)` sorts after `0` in lex-byte order, breaking range scans on negative-start windows (and `expireOldWindows` correctness for negative cutoffs reachable in test scenarios). Fix: bias by `2^63` and use `writeBigUInt64BE`, or XOR the sign bit on encode/decode. Affects `LMDBWindowStore.fetch/fetchAll/expireOldWindows` and `LMDBSessionStore.expireOldSessions`.
- **F7b — ChangelogRestorer always pays idleTimeoutMs on transactional changelog topics.** Codex (F7 review) walkthrough: under `read_committed`, the LSO returned by `fetchTopicOffsets('latest')` includes the offset *after* the closing COMMIT control batch. The consumer filters control batches and aborted records, so the last *delivered* user message sits at offset `LSO − 2` (one user record + one COMMIT marker). Restoration's fast-path check (`changelog.ts:372`: `message.offset + 1n >= endOffset`) is never satisfied → the restorer must wait `idleTimeoutMs` to terminate every restart. **Affects all EOS / transactional flows.** Cost: ~one `idleTimeoutMs` per partition group on every cold start. Fix candidates: track `consumer.position()` against `endOffset`, OR pause partitions when the consumer's HWM-fetched position reaches `endOffset` regardless of last delivered message, OR floor `idleTimeoutMs` once `pendingPartitions` is empty by position rather than by delivered offset.


After the 26-PR review-derived bug-fix stack landed (#42–#67), the subagent reviews surfaced these residual correctness issues that were explicitly out-of-scope at the time. This doc tracks the second-round work to close them.

Workflow per issue: branch → fix → triple review (Opus subagent for correctness, Opus subagent for simplification, Codex CLI gpt-5.5-high for an independent third opinion) → apply findings autonomously → verify locally → commit (`fix:` + `refactor:` separation) → push → CI → auto-merge unless any reviewer says skip.

---

## Critical (real production correctness gaps)

### F1 — EOS rebalance offset-commit gap
**Location:** `packages/flow/src/flow.ts:1136-1147` (rebalance handler)
**Bug:** `consumer.on('rebalance', ...)` fires synchronously and EventEmitter listeners cannot be awaited, so the consumer rejoins the group before the EOS commit completes. New generation can re-process records that were on the verge of commit → exactly-once degrades to at-least-once across rebalances.
**Fix direction:** Wire the commit through the awaitable `partitionsRevoked` consumer hook (which IS awaited by the consumer's protocol path — verified at `packages/client/src/consumer/partition-provider.ts:202,226,250`). Replace or supplement the non-awaitable `'rebalance'` event listener.
**Source:** PR #59 review subagent verdict — "the actual bug the PR's own comment identifies is NOT fixed."

### F2 — Cleanup never fires for WindowedReduceNode and SessionReduceNode
**Location:** `packages/flow/src/processors/aggregation.ts` — `WindowedReduceNode.process`, `SessionReduceNode.process`
**Bug:** Stream-time-driven `expireOldWindows` retention only runs in `WindowedAggregateNode`. The two reduce-flavored siblings have no expiry trigger at all. Any flow using `groupByKey().windowedBy(...).reduce(...)` or session-window reduce grows state-store unboundedly until OOM.
**Fix direction:** Hoist the `cleanupState` field + the post-write expiry trigger into `WindowedReduceNode` (mirrors the `WindowedAggregateNode` pattern). For `SessionReduceNode`, decide whether sessions need a cleanup hook at all (sessions naturally complete on inactivity, but truly-idle keys still leak — same fix shape with different semantics).
**Source:** PR #60 review subagent — "WindowedReduceNode and SessionReduceNode have no expiry trigger at all — pre-existing gap, unchanged here."

### F3 — `OffsetManager.commitPartitions` partial-failure poisoning
**Location:** `packages/client/src/consumer/offset-manager.ts:317-398` (`commitPartitions`)
**Bug:** Same partial-commit-failure-poisons-retries bug as `commitPendingOffsets` (fixed in #54). Used during cooperative rebalance revoke, so partitions are about to be released — less consequential, but a mixed-error response still leaves the stuck partitions polluting whatever isn't released this cycle.
**Fix direction:** Apply the same per-partition success-clear pattern from #54. Now that `commitPendingOffsets` exists as the reference implementation, the structure can be DRY'd.
**Source:** PR #54 review subagent — "`commitPartitions` (offset-manager.ts:317-398) has the same partial-failure bug but is untouched here."

---

## Important (durability / observability)

### F4 — Checkpoint errors silently swallowed
**Location:** `packages/flow/src/flow.ts:1050` — `.catch(() => {})` on `changelogCheckpointStore.set()` and the new `.flush?()` we added in #66
**Bug:** Pre-existing bare `.catch(() => {})` swallows checkpoint persistence failures (disk full, IO error, fsync failure now too after #66's flush hook). Silent failure means the checkpoint silently lags reality, then on next restart restoration over-restores already-applied records (idempotent but wasteful) — or worse, advances the checkpoint past unflushed data → state corruption.
**Fix direction:** At minimum: log at error level, emit a flow-level event so the user can react. Better: surface the error to the EOS commit pipeline so a checkpoint failure aborts the next transaction.
**Source:** PR #66 review subagent — "checkpoint-set errors with `.catch(() => {})` — now also swallows fsync failures silently."

---

## Minor / boundary

### F5 — Negative-window-start guard silently drops legitimate windows
**Location:** `packages/flow/src/processors/aggregation.ts` — `windowStartsFor` generator
**Bug:** For records very close to the unix epoch with a window-size larger than the timestamp, the legitimate negative-start window is silently dropped rather than clamped at 0 or surfaced. Not reachable for any realistic Kafka timestamp (post-1970 epoch ms), but it's a quiet compromise rather than an explicit policy.
**Fix direction:** Either clamp `windowStart = max(0, windowStart)` (and accept that windows near epoch get a shorter effective span) or document the silent skip explicitly with a one-line WHY.
**Source:** PR #60 / #63 review subagents — "silently drops legitimate windows when records arrive near t=0."

### F6 — `expireOldWindows` strict-vs-inclusive boundary inconsistency
**Location:** `packages/flow-state-lmdb/src/lmdb.ts` — `expireOldWindows`
**Bug:** Uses strict `windowEnd < cutoff` for retention drop. A window with `windowEnd === cutoff` is kept rather than dropped — defensible boundary choice but inconsistent with the inclusive-bound convention across the rest of the file (`range`, `fetch`, `fetchAll`).
**Fix direction:** Pick a convention and apply uniformly. Most LMDB-style retention is exclusive-of-cutoff (matches Kafka log retention), so switching to `<= cutoff - 1ms` or documenting the strict-`<` as intentional.
**Source:** PR #67 review subagent.

---

## Test gaps (regression protection, not bugs)

### F7 — Restoration over a changelog containing aborted transactions
**Bug:** No integration test exercises the case that `LMDBChangelogRestorer` correctly skips aborted-txn records. Would have caught #62's premise being wrong (its claim that the default isolation was `read_uncommitted`). Future drift in `DEFAULT_CONSUMER_CONFIG.isolationLevel` would silently corrupt restoration without any test failure.
**Fix direction:** Integration test in `packages/flow/tests/integration/` that produces a few records to a changelog topic via a transactional producer, aborts one of them, then restores via `ChangelogRestorer` and asserts the aborted record is NOT in the restored state.
**Source:** PR #62 close-comment + #66 review subagent.

### F8 — WindowStore / SessionStore put-then-changelog ordering parity tests
**Bug:** PR #57 fixed the put-then-changelog ordering across `ChangelogBackedKeyValueStore`, `ChangelogBackedWindowStore`, and `ChangelogBackedSessionStore`. But the unit tests assert the ordering only for the KeyValueStore variant. Future refactor that accidentally inverts WindowStore/SessionStore ordering wouldn't fail any test.
**Fix direction:** Two parallel tests in `packages/flow/tests/unit/changelog.test.ts` mirroring the existing KV-store ordering test.
**Source:** PR #57 review subagent — "Missing: ordering tests for WindowStore / SessionStore variants."

### F9 — `ShareConsumer.stream()` has no unit tests
**Bug:** The fix in #56 (release-not-ack on shutdown) has no test coverage at all. PR body acknowledged this honestly — `stream()` requires full coordinator/fetch scaffolding.
**Fix direction:** Either add an integration test that breaks out of `for await` and asserts the message is released back to the share group, or extract `stream()`'s lifecycle bookkeeping into a testable inner type. Lower priority since share consumer is `@experimental` (KIP-932 client-side support is fresh).
**Source:** PR #56 review subagent — "No unit tests for `stream()` at all."
