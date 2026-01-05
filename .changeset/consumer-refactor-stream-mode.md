---
"@kafkats/client": minor
"@kafkats/flow": minor
"@kafkats/codec-zod": minor
"@kafkats/flow-state-lmdb": minor
---

### Share Groups Support

Added KIP-932 Share Groups implementation with `ShareConsumer` class for scalable, lock-free message consumption without partition assignment.

### Consumer Refactoring

- Refactored consumer to use modular batch-based processing architecture
- Simplified stream mode with poll-based API, removing separate stream-mode module
- Fixed `sessionLost` flag not being cleared after successful rejoin
