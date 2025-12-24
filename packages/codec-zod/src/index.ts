import type { Codec } from '@kafkats/client'
import type { ZodType } from 'zod'

export interface ZodCodecOptions {
	encode?: (value: unknown) => Buffer
	decode?: (buffer: Buffer) => unknown
}

export function zodCodec<T>(schema: ZodType<T>, options: ZodCodecOptions = {}): Codec<T> {
	const encodeValue = options.encode ?? ((value: unknown) => Buffer.from(JSON.stringify(value), 'utf-8'))
	const decodeValue = options.decode ?? ((buffer: Buffer): unknown => JSON.parse(buffer.toString('utf-8')))

	return {
		encode: value => encodeValue(schema.parse(value)),
		decode: buffer => schema.parse(decodeValue(buffer)),
	}
}
