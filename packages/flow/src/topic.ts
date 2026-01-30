import type { Codec } from '@/codec.js'
import type { Topic } from '@/types.js'

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
