package hash

import "github.com/twmb/murmur3"

const ringSeed uint32 = 1001

func New(value string, addSeed uint32) uint32 {
	return murmur3.SeedStringSum32(ringSeed+addSeed, value)
}
