package topology

import (
	"cmp"
	"fmt"
	"lockr/internal/platform/hash"
	"maps"
	"slices"
	"sync/atomic"
)

type Orchestrator struct {
	Ring atomic.Pointer[Ring]
}

func New() *Orchestrator {
	o := &Orchestrator{}
	initialRing := &Ring{
		vnodes:         make([]VNode, 0),
		nodes:          make(map[string]*NodeMeta),
		existingHashes: make(map[uint32]struct{}),
	}

	o.Ring.Store(initialRing)
	return o
}

func (o *Orchestrator) AddNode(node *NodeMeta, vnodeCount int) {
	for {
		currentRing := o.Ring.Load()

		newNodes := maps.Clone(currentRing.nodes)
		existingHashes := maps.Clone(currentRing.existingHashes)
		newNodes[node.ID] = node

		newVNodes := slices.Grow(slices.Clone(currentRing.vnodes), vnodeCount)

		for i := 0; i < vnodeCount; i++ {
			vnodeKey := fmt.Sprintf("vnode-%s-%d", node.ID, i)
			hashVal := hash.New(vnodeKey, 0)

			var attempts uint32

			for {
				if _, collision := existingHashes[hashVal]; !collision {
					break
				}
				attempts++
				hashVal = hash.New(vnodeKey, attempts)
			}

			existingHashes[hashVal] = struct{}{}

			newVNodes = append(newVNodes, VNode{
				Hash:   hashVal,
				NodeID: node.ID,
			})
		}

		slices.SortFunc(newVNodes, func(a, b VNode) int {
			return cmp.Compare(a.Hash, b.Hash)
		})

		newRing := &Ring{
			vnodes:         newVNodes,
			nodes:          newNodes,
			existingHashes: existingHashes,
		}

		if o.Ring.CompareAndSwap(currentRing, newRing) {
			return
		}
	}
}

func (o *Orchestrator) GetNode(keyHash uint32) string {
	currentRing := o.Ring.Load()
	if currentRing == nil {
		return ""
	}

	return currentRing.GetNode(keyHash)
}
