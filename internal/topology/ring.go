package topology

import (
	"cmp"
	"slices"
)

type Ring struct {
	vnodes         []VNode // Kept strictly sorted by Hash
	nodes          map[string]*NodeMeta
	existingHashes map[uint32]struct{}
}

func (r *Ring) GetNode(keyHash uint32) string {
	if len(r.vnodes) == 0 {
		return ""
	}

	idx, _ := slices.BinarySearchFunc(r.vnodes, keyHash, func(vn VNode, target uint32) int {
		return cmp.Compare(vn.Hash, target)
	})

	if idx == len(r.vnodes) {
		idx = 0
	}

	return r.vnodes[idx].NodeID
}
