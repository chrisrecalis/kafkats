# @kafkats/flow-state-testkit

Private package: the test suites every persistent state store provider of `@kafkats/flow` has to pass. Not published.

```typescript
// packages/flow-state-<engine>/tests/provider.test.ts
import { describeStateStoreProvider } from '@kafkats/flow-state-testkit'
import { lmdb } from '@/lmdb.js'

describeStateStoreProvider({
	name: 'LMDB',
	create: stateDir => lmdb({ stateDir }),
	exactCounts: true,
	module: { url: new URL('../src/lmdb.ts', import.meta.url).href, factory: 'lmdb' },
})
```

- `range`, `window-bounds`, `signed-time-ordering`, `memory-parity`, `checkpoint-sentinel`: store behaviour and
  changelog checkpoints (`-1n`, the checkpoint that invalidates a store, kept exactly), no broker needed.
- `restart`, `shared-dir`, `crash`: run a flow app against the broker in `KAFKA_BROKERS` and are skipped without it. `crash` kills child processes (`hardening-worker.ts`) that create the provider from `module`.

A new case for the store contract goes here, so every provider gets it.
