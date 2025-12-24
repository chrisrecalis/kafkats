/**
 * JSON Logging system for kafkats
 *
 * Provides structured JSON logging with support for error, info, and debug levels.
 */

/**
 * Log levels supported by the logger
 * 'silent' disables all logging
 */
export type LogLevel = 'silent' | 'error' | 'warn' | 'info' | 'debug'

/**
 * Numeric values for log level comparison
 */
const LOG_LEVEL_VALUES: Record<LogLevel, number> = {
	silent: -1,
	error: 0,
	warn: 1,
	info: 2,
	debug: 3,
}

/**
 * Logger interface for structured logging
 */
export interface Logger {
	/**
	 * Log an error message
	 */
	error(message: string, context?: Record<string, unknown>): void

	/**
	 * Log a warning message
	 */
	warn(message: string, context?: Record<string, unknown>): void

	/**
	 * Log an info message
	 */
	info(message: string, context?: Record<string, unknown>): void

	/**
	 * Log a debug message
	 */
	debug(message: string, context?: Record<string, unknown>): void

	/**
	 * Create a child logger with additional default context
	 */
	child(defaultContext: Record<string, unknown>): Logger
}

/**
 * JSON console logger implementation
 */
class JsonLogger implements Logger {
	private readonly level: LogLevel
	private readonly defaultContext: Record<string, unknown>

	constructor(level: LogLevel = 'info', defaultContext: Record<string, unknown> = {}) {
		this.level = level
		this.defaultContext = defaultContext
	}

	private shouldLog(level: LogLevel): boolean {
		return LOG_LEVEL_VALUES[level] <= LOG_LEVEL_VALUES[this.level]
	}

	private log(level: LogLevel, message: string, context?: Record<string, unknown>): void {
		if (!this.shouldLog(level)) {
			return
		}

		const entry = {
			level,
			message,
			timestamp: new Date().toISOString(),
			...this.defaultContext,
			...context,
		}

		const output = JSON.stringify(entry)

		if (level === 'error') {
			console.error(output)
		} else {
			console.log(output)
		}
	}

	error(message: string, context?: Record<string, unknown>): void {
		this.log('error', message, context)
	}

	warn(message: string, context?: Record<string, unknown>): void {
		this.log('warn', message, context)
	}

	info(message: string, context?: Record<string, unknown>): void {
		this.log('info', message, context)
	}

	debug(message: string, context?: Record<string, unknown>): void {
		this.log('debug', message, context)
	}

	child(defaultContext: Record<string, unknown>): Logger {
		return new JsonLogger(this.level, { ...this.defaultContext, ...defaultContext })
	}
}

/**
 * No-op logger that discards all log messages
 */
class NoopLogger implements Logger {
	error(): void {
		// no-op
	}

	warn(): void {
		// no-op
	}

	info(): void {
		// no-op
	}

	debug(): void {
		// no-op
	}

	child(): Logger {
		return this
	}
}

/**
 * Create a JSON console logger
 *
 * @param level - Minimum log level to output (default: 'info')
 * @param defaultContext - Default context to include in all log entries
 * @returns A new Logger instance
 *
 * @example
 * ```typescript
 * const logger = createLogger('debug')
 * logger.info('Starting', { clientId: 'my-app' })
 * // Output: {"level":"info","message":"Starting","timestamp":"...","clientId":"my-app"}
 * ```
 */
export function createLogger(level: LogLevel = 'info', defaultContext: Record<string, unknown> = {}): Logger {
	return new JsonLogger(level, defaultContext)
}

/**
 * No-op logger instance for when logging is disabled
 */
export const noopLogger: Logger = new NoopLogger()
