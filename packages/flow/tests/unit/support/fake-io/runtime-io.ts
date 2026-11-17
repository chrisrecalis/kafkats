import type { RuntimeIO } from '@/runtime/io.js'
import { ManualClock } from '../manual-clock.js'
import { FakeAdmin } from './admin.js'
import { FakeCluster } from './cluster.js'
import { FakeGroupConsumer } from './group-consumer.js'
import { FakeProducer } from './producer.js'
import { FakeRestoreConsumer } from './restore-consumer.js'

/**
 * The runtime's IO over one fake cluster and a manual clock. Every port it hands out is kept in `created`, in
 * creation order, for the test to drive and script; the group consumers share the group's committed offsets.
 */
export class FakeIO implements RuntimeIO {
	readonly cluster = new FakeCluster()
	readonly clock: ManualClock
	readonly admin = new FakeAdmin(this.cluster)
	readonly created = {
		consumers: [] as FakeGroupConsumer[],
		producers: [] as FakeProducer[],
		restoreConsumers: [] as FakeRestoreConsumer[],
		globalConsumers: [] as FakeRestoreConsumer[],
	}

	/** The group of the group consumers (the application id). */
	readonly groupId: string

	constructor(options: { groupId?: string; time?: number } = {}) {
		this.groupId = options.groupId ?? 'app'
		this.clock = new ManualClock(options.time ?? 0)
	}

	readonly consumer = (): FakeGroupConsumer =>
		keep(this.created.consumers, new FakeGroupConsumer({ cluster: this.cluster, groupId: this.groupId }))

	readonly producer = (): FakeProducer => keep(this.created.producers, new FakeProducer(this.cluster))

	readonly restoreConsumers = (groupId: string): FakeRestoreConsumer =>
		keep(this.created.restoreConsumers, new FakeRestoreConsumer(this.cluster, groupId))

	readonly globalConsumers = (groupId: string): FakeRestoreConsumer =>
		keep(this.created.globalConsumers, new FakeRestoreConsumer(this.cluster, groupId))
}

function keep<T>(list: T[], item: T): T {
	list.push(item)
	return item
}
