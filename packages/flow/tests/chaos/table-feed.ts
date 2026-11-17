/**
 * The table side of the join modes: version 0 of every key before the workers start (so the inner join matches every
 * input record), then the next version of every key every `everyMs` until stop().
 *
 * One idempotent producer sends the versions in order and none is resent, so the versions of a key never go back in
 * the topic (what the join summary's `regressions` relies on); a failed send ends the feed. hold() waits for the send
 * in flight and keeps the next one back until release(), so no send is in flight across a broker fault.
 */
import type { KafkaClient, Producer } from '@kafkats/client'
import { sleep } from './common.js'

export class TableFeed {
	version = 0
	private held: Promise<void> | null = null
	private release_ = () => {}
	private inFlight: Promise<unknown> = Promise.resolve()
	private stopped = false
	private readonly loop: Promise<void>

	private constructor(
		private readonly producer: Producer,
		private readonly topic: string,
		private readonly keys: number,
		everyMs: number
	) {
		this.loop = this.run(everyMs)
	}

	static async start(client: KafkaClient, topic: string, keys: number, everyMs = 1000): Promise<TableFeed> {
		const producer = client.producer({ lingerMs: 0, idempotent: true })
		await send(producer, topic, keys, 0)
		return new TableFeed(producer, topic, keys, everyMs)
	}

	private async run(everyMs: number) {
		for (;;) {
			await sleep(everyMs)
			while (this.held) await this.held
			if (this.stopped) return
			// No await between the check above and this assignment: hold() either stops this send or waits for it.
			this.inFlight = send(this.producer, this.topic, this.keys, this.version + 1)
			try {
				await this.inFlight
				this.version++
			} catch (e) {
				console.log(`  table feed ended at version ${this.version}: ${(e as Error).message}`)
				return
			}
		}
	}

	async hold() {
		this.held ??= new Promise(resolve => (this.release_ = resolve))
		await this.inFlight.catch(() => {})
	}

	release() {
		this.held = null
		this.release_()
	}

	async stop() {
		this.stopped = true
		this.release()
		await this.loop
		await this.producer.disconnect().catch(() => {})
	}
}

function send(producer: Producer, topic: string, keys: number, version: number) {
	return producer.send(
		topic,
		Array.from({ length: keys }, (_, k) => ({ key: `k${k}`, value: Buffer.from(String(version)) }))
	)
}
