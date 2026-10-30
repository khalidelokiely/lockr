package engine

type Node struct {
	wheel  *ExpiryWheel
	shards []*Shard
}

func NewNode(numPartitions, peakSize int, wheel *ExpiryWheel) *Node {
	n := &Node{wheel: wheel, shards: make([]*Shard, numPartitions)}

	for i := range n.shards {
		n.shards[i] = NewShard(uint32(i), peakSize, wheel)
	}

	wheel.SetRouter(n)
	return n
}

func (n *Node) Shard(id uint32) *Shard {
	if int(id) >= len(n.shards) {
		return nil
	}
	return n.shards[id]
}

func (n *Node) ExpireGrant(shardID uint32, key, releaseSecret string) {
	
}
