import { describeStateStoreProvider } from '@kafkats/flow-state-testkit'
import { lmdb } from '@/lmdb.js'

// The contract every persistent state store provider has to pass (see @kafkats/flow-state-testkit).
describeStateStoreProvider({
	name: 'LMDB',
	create: stateDir => lmdb({ stateDir }),
	exactCounts: true,
	module: { url: new URL('../src/lmdb.ts', import.meta.url).href, factory: 'lmdb' },
})
