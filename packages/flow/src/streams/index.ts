// Stream implementations
export { KStreamImpl, ensureStream } from '@/streams/kstream.js'
export { KTableImpl, type FlowAppInterface } from '@/streams/ktable.js'
export { KGroupedStreamImpl, WindowedKGroupedStreamImpl, KGroupedTableImpl } from '@/streams/grouped.js'
