/**
 * Where the driver's producer puts a record sent without a partition (4.14): the producer's default partitioning, so
 * a re-keyed record reaches the task a Kafka run would give it.
 */
import { murmur2, toPositive } from '@kafkats/client'

/** The Java client's default partition of a key: positive murmur2 modulo the partition count; 0 without a key. */
export function defaultPartition(key: Buffer | null, partitions: number): number {
	return key === null ? 0 : toPositive(murmur2(key)) % partitions
}
