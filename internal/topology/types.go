package topology

type NodeState string

const (
	StateAlive    NodeState = "ALIVE"
	StateYielding NodeState = "YIELDING"
	StateDead     NodeState = "DEAD"
)

type NodeMeta struct {
	ID      string
	Address string
	Port    int
	State   NodeState
}

// VNode represents a point on the 2^32 ring space
type VNode struct {
	Hash   uint32
	NodeID string // References NodeMeta.ID or Address
}
