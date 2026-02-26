package hashroute

import (
	"hash/fnv"
	"strings"
)

const PartitionCount = 25

// CanonicalizeStreamKey normalizes incoming stream keys before hashing.
func CanonicalizeStreamKey(streamKey string) string {
	return strings.ToLower(strings.TrimSpace(streamKey))
}

func PartitionForStreamKey(streamKey string) int {
	key := CanonicalizeStreamKey(streamKey)
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % PartitionCount)
}
