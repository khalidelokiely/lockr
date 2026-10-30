package lock

import "sync"

var handleChanPool = sync.Pool{
	New: func() any {
		return make(chan Handle, 1)
	},
}

// GetChan leases a buffered handle channel from the pool.
func GetChan() chan Handle {
	return handleChanPool.Get().(chan Handle)
}
