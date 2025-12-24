export interface Codec<T> {
	encode(value: T): Buffer
	decode(buffer: Buffer): T
}

export function string(): Codec<string> {
	return {
		encode: value => Buffer.from(value, 'utf-8'),
		decode: buffer => buffer.toString('utf-8'),
	}
}

export function json<T>(): Codec<T> {
	return {
		encode: value => Buffer.from(JSON.stringify(value), 'utf-8'),
		decode: buffer => JSON.parse(buffer.toString('utf-8')) as T,
	}
}

export function buffer(): Codec<Buffer> {
	return {
		encode: value => value,
		decode: value => value,
	}
}

export const codec = {
	string,
	json,
	buffer,
}
