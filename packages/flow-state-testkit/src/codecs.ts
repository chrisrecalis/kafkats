/** Plain codecs for store tests (no dependency on a built @kafkats/flow). */
export const stringCodec = {
	encode: (s: string) => Buffer.from(s, 'utf-8'),
	decode: (b: Buffer) => b.toString('utf-8'),
}

/** 8-byte little-endian double. */
export const numberCodec = {
	encode: (n: number) => {
		const b = Buffer.alloc(8)
		b.writeDoubleLE(n, 0)
		return b
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}
