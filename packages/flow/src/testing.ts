/**
 * @kafkats/flow testing utilities: run a topology against an in-memory Kafka.
 *
 * `TestDriver` replaces the client's consumers and producers with `MockConsumer` / `MockProducer`.
 * Records sent through the test context are delivered straight to the topology; everything the
 * topology produces is captured by the producer, and records written to a topic the topology also
 * reads (`through()`, repartition topics) are fed back into it.
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
export * from '@/legacy/testing.js'
