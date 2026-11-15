// Client layer exports (includes producer and consumer)
export * from '@/client/index.js'

// Protocol layer exports
export * from '@/protocol/index.js'

// Utils
export { crc32c, verifyCrc32c, crc32cSigned } from '@/utils/crc32c.js'

// Diagnostics: what the client publishes about its own work (node:diagnostics_channel)
export {
	diagnostics,
	type SendDiagnostics,
	type ProduceRequestDiagnostics,
	type ProcessDiagnostics,
	type CommitDiagnostics,
	type RebalanceDiagnostics,
	type InstanceDiagnostics,
} from '@/diagnostics.js'

// Logger
export { createLogger, noopLogger, type Logger, type LogLevel } from '@/logger.js'
