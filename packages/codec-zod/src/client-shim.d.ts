declare module '@kafkats/client' {
	export interface Codec<T> {
		encode(value: T): Buffer
		decode(buffer: Buffer): T
	}
}
