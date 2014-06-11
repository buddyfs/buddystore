package buddystore

import (
	"fmt"
	"sync"
)

var NUM_LM_REPLICA = 2

type RLockVal struct {
	lockID  string
	version uint
}

type WLockVal struct {
	lockID  string
	version uint
	timeout uint
}

type LManagerClient struct {
	Vnode  *Vnode               //  Vnode associated with this LMClient
	Ring   RingIntf             // Ring with whom the client is associated with
	RLocks map[string]*RLockVal //  Map of <keys, ReadLock Values>
	WLocks map[string]*WLockVal //  Map of <keys, WriteLock Values>

	rLockMut sync.Mutex // Mutex lock for synchronizing access to RLocks map
	wLockMut sync.Mutex // Mutex lock for synchronizing access to WLocks map

	// Implements:
	LMClientIntf
}

var _ LMClientIntf = new(LManagerClient)

type LMClientIntf interface {
	RLock(key string, forceNoCache bool) (uint, error)
	WLock(key string, version uint, timeout uint) (uint, error)
	CommitWLock(key string, version uint) error
	AbortWLock(key string, version uint) error
	InvalidateRLock(lockID string) error
}

func (lm *LManagerClient) getLManagerReplicas() ([]*Vnode, error) {
	/* TODO : Discuss : Right not supports only one LockManager. */
	LMVnodes, err := lm.Ring.Lookup(NUM_LM_REPLICA, []byte(lm.Ring.GetRingId()))
	if err != nil {
		return nil, err
	}
	if len(LMVnodes) == 0 {
		return nil, fmt.Errorf("Failed lookup for lockManager")
	}
	return LMVnodes, nil
}

/*
Called by the client before making a read on the key
Param key : The key to be looked up
Param forceNoCache : Invalidate existing ReadLocks and get a new lock from LM
*/
func (lm *LManagerClient) RLock(key string, forceNoCache bool) (version uint, err error) {
	lm.rLockMut.Lock()
	defer lm.rLockMut.Unlock()
	if !forceNoCache {
		rLock := lm.RLocks[key]
		if rLock != nil {
			return rLock.version, nil
		}
	}
	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return 0, err
	}
	/* TODO : Discuss : Extract nodeID and send it to server side. Where to get that info*/
	retLockID, ver, err := lm.Ring.Transport().RLock(LMVnodes[0], key, lm.Ring.GetLocalVnode().String())
	if err != nil {
		return 0, fmt.Errorf("Cannot get ReadLock due to ", err)
	}
	lm.RLocks[key] = &RLockVal{lockID: retLockID, version: ver}
	return ver, nil
}

func (lm *LManagerClient) WLock(key string, version uint, timeout uint) (uint, error) {
	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return 0, err
	}

	retLockID, ver, timeout, _, err := lm.Ring.Transport().WLock(LMVnodes[0], key, version, timeout, lm.Ring.GetLocalVnode().String(), nil)
	if err != nil {
		return ver, fmt.Errorf("Cannot get the write lock ", err)
	}
	lm.wLockMut.Lock()
	lm.WLocks[key] = &WLockVal{lockID: retLockID, version: ver, timeout: timeout}
	lm.wLockMut.Unlock()
	return ver, nil
}

func (lm *LManagerClient) CommitWLock(key string, version uint) error {
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	wLockVal := lm.WLocks[key]
	if wLockVal == nil {
		return fmt.Errorf("Cannot find lock to be committed in local writeLocks cache")
	}

	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return err
	}

	err = lm.Ring.Transport().CommitWLock(LMVnodes[0], key, version, lm.Ring.GetLocalVnode().String())
	if err != nil {
		return err
	}
	delete(lm.WLocks, key) //  Delete key from local write locks

	return nil
}

func (lm *LManagerClient) AbortWLock(key string, version uint) error {
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	wLockVal := lm.WLocks[key]
	if wLockVal == nil {
		return fmt.Errorf("Cannot find lock to be committed in local writeLocks cache")
	}

	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return err
	}

	err = lm.Ring.Transport().AbortWLock(LMVnodes[0], key, version, lm.Ring.GetLocalVnode().String())
	if err != nil {
		return err
	}
	delete(lm.WLocks, key)

	return nil
}

func (lm *LManagerClient) InvalidateRLock(lockID string) error {
	var key string = ""
	lm.rLockMut.Lock()
	defer lm.rLockMut.Unlock()
	for k, v := range lm.RLocks {
		if v.lockID == lockID {
			key = k
		}
	}
	if key == "" {
		return fmt.Errorf("Cannot find LockId on Client's RLock cache")
	}
	delete(lm.RLocks, key)
	return nil
}

func (lm *LManagerClient) keyInRLocksCache(key string) bool {
	lm.rLockMut.Lock()
	defer lm.rLockMut.Unlock()
	_, ok := lm.RLocks[key]
	if ok { //  No ternary in Go
		return true
	}
	return false
}
