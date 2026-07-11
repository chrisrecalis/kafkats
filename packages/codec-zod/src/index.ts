import type { Codec } from '@kafkats/client'
import type { ZodType } from 'zod'

export interface ZodCodecOptions {
	encode?: (value: unknown) => Buffer
	decode?: (buffer: Buffer) => unknown
}

/**
 * Build a Codec<T> from a Zod schema, where T is the schema's OUTPUT type.
 *
 * decode validates the incoming bytes with schema.parse, so consumers always receive a
 * schema-conforming (and possibly transformed/defaulted) value.
 *
 * encode serializes the value as-is, WITHOUT running schema.parse: the value being produced is
 * output-typed (e.g. a value previously decoded by this codec), and Zod cannot reverse
 * transforms/defaults — parsing it against the input-typed schema would throw for any schema
 * where input and output differ (transforms, defaults, pipes). Serialization must round-trip
 * decode(encode(decodedValue)); validation happens on the consume side.
 */
export function zodCodec<T>(schema: ZodType<T>, options: ZodCodecOptions = {}): Codec<T> {
	const encodeValue = options.encode ?? ((value: unknown) => Buffer.from(JSON.stringify(value), 'utf-8'))
	const decodeValue = options.decode ?? ((buffer: Buffer): unknown => JSON.parse(buffer.toString('utf-8')))

	return {
		encode: value => encodeValue(value),
		decode: buffer => schema.parse(decodeValue(buffer)),
	}
}
