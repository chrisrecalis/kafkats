import type { Codec } from '@/codec.js'

/** A topic name with the codecs of its keys and values. */
export interface Topic<K = Buffer, V = Buffer> {
	name: string
	key?: Codec<K>
	value?: Codec<V>
}

export function topic<K = Buffer, V = Buffer>(
	name: string,
	options?: { key?: Codec<K>; value?: Codec<V> }
): Topic<K, V> {
	return {
		name,
		key: options?.key,
		value: options?.value,
	}
}
