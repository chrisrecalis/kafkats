import { describeStateStoreProvider } from '@kafkats/flow-state-testkit'
import { rocksdb } from '@/rocksdb.js'

// The contract every persistent state store provider has to pass (see @kafkats/flow-state-testkit).
describeStateStoreProvider({
	name: 'RocksDB',
	create: stateDir => rocksdb({ stateDir }),
	exactCounts: false,
	module: { url: new URL('../src/rocksdb.ts', import.meta.url).href, factory: 'rocksdb' },
})
