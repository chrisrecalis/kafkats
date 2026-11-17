/**
 * @kafkats/flow testing utilities: run a topology deterministically, without Kafka (Kafka Streams' TopologyTestDriver).
 *
 * `TestDriver` runs the production task runtime over in-memory ports. Every record sent through the test context is
 * processed, together with everything it leads to, before `send()` resolves; records written to a topic the topology
 * also reads (`through()`, repartition topics) are fed back into it. `MockProducer` records every send and the
 * committed transactions; offsets are committed after every record by default (`commitEvery`).
 *
 * @example
 * ```typescript
 * import { TestDriver } from '@kafkats/flow/testing'
 *
 * const driver = new TestDriver()
 *
 * driver.input('orders', { value: codec.json<Order>() })
 *   .mapValues(order => ({ ...order, processed: true }))
 *   .to('processed-orders', { value: codec.json() })
 *
 * await driver.run(async ({ send, output }) => {
 *   await send('orders', { id: '1', amount: 100 })
 *   expect(output('processed-orders')).toHaveLength(1)
 * })
 * ```
 */
export {
	TestDriver,
	type TestDriverConfig,
	type TestContextApi,
	type SendMessageOptions,
	type OutputOptions,
	type WaitOptions,
	type DecodedRecord,
} from '@/testing/driver.js'
export {
	MockConsumer,
	MockProducer,
	type TestRecord,
	type SendOptions,
	type CommittedTransaction,
} from '@/testing/mocks.js'
export {
	ResultCollector,
	testRecord,
	testRecordSequence,
	createFactory,
	timestamps,
	quickTest,
	type ProcessorTestRecord,
	type QuickTestHelpers,
} from '@/testing/helpers.js'
