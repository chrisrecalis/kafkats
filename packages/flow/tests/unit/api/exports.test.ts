/**
 * The entry points: every type a public signature names can be imported by name, and runtime internals are not
 * exported.
 */
import { describe, expect, expectTypeOf, it } from 'vitest'
import type {
	FlowProcessDiagnostics,
	FlowProcessScope,
	ForwardResult,
	KeyValueMapper,
	MaybePromise,
	Processor,
	ProcessorContext,
	TopicNameExtractor,
	TopicNameExtractorContext,
} from '@/index.js'
import * as flow from '@/index.js'
import type {
	CommittedOffset,
	CommittedTransaction,
	MockConsumer,
	MockProducer,
	OffsetAndMetadata,
	ProducedMessage,
} from '@/testing.js'

describe('@kafkats/flow exports', () => {
	it('names the types of the public signatures', () => {
		expectTypeOf<Parameters<TopicNameExtractor<string, number>>[2]>().toEqualTypeOf<TopicNameExtractorContext>()
		// The key selector of the KStream.join/leftJoin(globalTable, keySelector, joiner) overloads.
		expectTypeOf<KeyValueMapper<string, number, string>>().toEqualTypeOf<(key: string, value: number) => string>()
		expectTypeOf<ReturnType<Processor<string, string, string, string>['process']>>().toEqualTypeOf<
			MaybePromise<void>
		>()
		expectTypeOf<ReturnType<ProcessorContext<string, string>['forward']>>().toEqualTypeOf<ForwardResult>()
		expectTypeOf<Parameters<FlowProcessDiagnostics['runIn']>[0]>().toEqualTypeOf<FlowProcessScope>()
	})

	it('@kafkats/flow/testing names the types of its mocks', () => {
		expectTypeOf<Parameters<MockProducer['send']>[1]>().toEqualTypeOf<ProducedMessage>()
		expectTypeOf<CommittedTransaction['offsets'][number]>().toEqualTypeOf<OffsetAndMetadata>()
		expectTypeOf<MockConsumer['committed']>().toEqualTypeOf<ReadonlyMap<string, CommittedOffset>>()
	})

	it('keeps the changelog layer internal: the runtime adds it to the stores that log', () => {
		for (const name of [
			'ChangelogWriter',
			'ChangelogBackedKeyValueStore',
			'ChangelogBackedWindowStore',
			'ChangelogBackedSessionStore',
		]) {
			expect(name in flow).toBe(false)
		}
	})
})
