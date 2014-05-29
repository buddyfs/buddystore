package chord

import (
	"fmt"
)

var NUM_LM_REPLICAS = 2

type RLockVal struct {
	lockID  string
	version uint
}

type WLockVal struct {
	lockID  string
	version uint
	timeout uint
}

type LManager struct {
	Ring   *Ring                // Ring with whom the client is associated with
	RLocks map[string]*RLockVal //  Map of <keys, ReadLock Values>
	WLocks map[string]*WLockVal //  Map of <keys, WriteLock Values>
}

type LMClient interface {
	RLock(key string) (int, error)
	WLock(key string, version int, timeout uint) (uint, error)
	CommitWLock(key string, version int)
	AbortWLock(key string, version int)
}

/*
Called by the client before making a read on the key
Param key : The key to be looked up
Param forceNoCache : Invalidate existing ReadLocks and get a new lock from LM
*/
func (lm *LManager) RLock(key string, forceNoCache bool) (version uint, err error) {
	if !forceNoCache {
		rLock := lm.RLocks[key]
		if rLock != nil {
			return rLock.version, nil
		}
	}
	LMVnodes, err := lm.Ring.Lookup(NUM_LM_REPLICAS, []byte(key))
	if err != nil {
		return 0, err
	}
	if len(LMVnodes) == 0 {
		return 0, fmt.Errorf("Failed lookup for lockManager")
	}
	retLockID, ver, err := lm.Ring.transport.RLock(LMVnodes[0], key)
	lm.RLocks[key] = &RLockVal{lockID: retLockID, version: ver}

	return 0, nil
}
