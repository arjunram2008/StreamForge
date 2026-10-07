package partition

import "hash/fnv"

// Select keeps a key on the same partition. Empty keys rotate across partitions.
func Select(key string, count int, next *uint64) int {
	if key != "" {
		h := fnv.New32a()
		h.Write([]byte(key))
		return int(h.Sum32() % uint32(count))
	}
	p := int(*next % uint64(count))
	*next++
	return p
}
