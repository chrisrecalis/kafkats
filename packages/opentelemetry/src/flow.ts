import { tracingChannel } from 'node:diagnostics_channel'
import { ROOT_CONTEXT, SpanKind, context, propagation, trace, type Tracer } from '@opentelemetry/api'
import type { FlowProcessDiagnostics } from '@kafkats/flow'

import { ATTR, SYSTEM_KAFKA } from './attributes.js'
import { onOperation } from './operations.js'
import { endSpan, fromHeaders, notSampled } from './tracing.js'

/**
 * What `@kafkats/flow` publishes for every record one of its tasks processes. Subscribed by name, so this package
 * needs no flow at run time: without a flow application in the process the channel stays silent.
 */
const flowProcess = tracingChannel<unknown, FlowProcessDiagnostics>('kafkats:flow:process')

/**
 * Spans for the records a `@kafkats/flow` application processes. A flow application consumes in batches and processes
 * the records later, one at a time in their tasks, so the client's handler span cannot be their parent.
 *
 * - A `process <topic>` span per record a task processes: from the record entering the topology until everything it
 *   led to settled. It continues the trace of the send that produced the record (its headers).
 * - The processors run with it active, so the records they send (outputs, repartition and changelog records) are its
 *   children, and so is any span they create.
 * - No span for a record inside a trace that is not sampled; the processing still runs in that trace's context.
 */
export function traceFlow(tracer: Tracer): () => void {
	return onOperation(flowProcess, {
		started(processing) {
			const parent = propagation.extract(ROOT_CONTEXT, processing.headers, fromHeaders)
			const span = notSampled(parent)
				? undefined
				: tracer.startSpan(
						`process ${processing.topic}`,
						{
							kind: SpanKind.CONSUMER,
							attributes: {
								[ATTR.system]: SYSTEM_KAFKA,
								[ATTR.topic]: processing.topic,
								[ATTR.partition]: String(processing.partition),
								[ATTR.operationName]: 'process',
								[ATTR.operationType]: 'process',
								[ATTR.consumerGroup]: processing.applicationId,
								[ATTR.offset]: Number(processing.offset),
								[ATTR.taskId]: processing.taskId,
							},
						},
						parent
					)
			const active = span ? trace.setSpan(parent, span) : parent
			processing.runIn(process => context.with(active, process))
			return span
		},
		finished(_, span, error) {
			if (span) endSpan(span, error)
		},
	})
}
