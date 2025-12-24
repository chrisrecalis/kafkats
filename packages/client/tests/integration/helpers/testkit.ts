import { randomUUID } from 'node:crypto'

export function uniqueName(prefix: string): string {
	const id = randomUUID().replace(/-/g, '').slice(0, 12)
	return `${prefix}-${id}`
}

export async function sleep(ms: number): Promise<void> {
	await new Promise(resolve => setTimeout(resolve, ms))
}
