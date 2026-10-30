package lock

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

type GrantType int

const (
	TypeRead GrantType = iota
	TypeWrite
)

var ErrLockBusy = errors.New("lock busy")
var ErrStaleFencingToken = errors.New("stale fencing token")
var ErrInvalidReleaseSecret = errors.New("invalid release secret")
var ErrNoLockForKey = errors.New("no lock for key")

type Handle struct {
	Key           string `json:"key"`
	FencingToken  uint64 `json:"fencing_token"`
	ReleaseSecret string `json:"release_secret"`
	ExpiresAt     int64  `json:"expires_at"`
}

type AcquireResult struct {
	Handle   Handle
	WaiterID uint64
	WaitChan chan Handle
}

func (r *AcquireResult) IsGranted() bool {
	return r.WaitChan == nil
}

func (r *AcquireResult) Free() {
	if r.WaitChan == nil {
		return
	}
	select {
	case <-r.WaitChan:
	default:
	}

	handleChanPool.Put(r.WaitChan)
}

type Waiter struct {
	ID      uint64
	Type    GrantType
	TTL     time.Duration
	OutChan chan Handle
}

type Grant struct {
	ExpiresAt     int64
	ReleaseSecret string
	FencingToken  uint64 // token issued to THIS grant (readers each get their own)
	IsActive      bool
}

func generateSecret() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)

	if err != nil {
		return ""
	}

	return hex.EncodeToString(b)
}
