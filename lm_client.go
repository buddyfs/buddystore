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

type LManagerClient struct {
	Ring   *Ring                // Ring with whom the client is associated with
	RLocks map[string]*RLockVal //  Map of <keys, ReadLock Values>
	WLocks map[string]*WLockVal //  Map of <keys, WriteLock Values>

	// Implements:
	LMClientIntf
}

var _ LMClientIntf = new(LManagerClient)

type LMClientIntf interface {
	RLock(key string, forceNoCache bool) (uint, error)
	WLock(key string, version uint, timeout uint) (uint, error)
	CommitWLock(key string, version uint) error
	AbortWLock(key string, version uint) error
}

func (lm *LManagerClient) getLManagerReplicas() ([]*Vnode, error) {
	/* TODO : Discuss : Right not supports only one LockManager. */
	LMVnodes, err := lm.Ring.Lookup(NUM_LM_REPLICAS, []byte(lm.Ring.config.RingId))
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
	retLockID, ver, err := lm.Ring.transport.RLock(LMVnodes[0], key, "testNodeId")
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


	retLockID, ver, timeout, err := lm.Ring.transport.WLock(LMVnodes[0], key, version, timeout, "testNodeId")
	if err != nil {
		return ver, fmt.Errorf("Cannot get the write lock ", err)
	}
	lm.WLocks[key] = &WLockVal{lockID: retLockID, version: ver, timeout: timeout}
	return ver, nil
}

func (lm *LManagerClient) CommitWLock(key string, version uint) error {
	wLockVal := lm.WLocks[key]
	if wLockVal == nil {
		return fmt.Errorf("Cannot find lock to be committed in local writeLocks cache")
	}

	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return err
	}

	err = lm.Ring.transport.CommitWLock(LMVnodes[0], key, version, "testNodeId")
	if err != nil {
		return err
	}
	delete(lm.WLocks, key) //  Delete key from local write locks

	return nil
}

func (lm *LManagerClient) AbortWLock(key string, version uint) error {
	wLockVal := lm.WLocks[key]
	if wLockVal == nil {
		return fmt.Errorf("Cannot find lock to be committed in local writeLocks cache")
	}

	LMVnodes, err := lm.getLManagerReplicas()
	if err != nil {
		return err
	}

	err = lm.Ring.transport.AbortWLock(LMVnodes[0], key, version, "testNodeId")
	if err != nil {
		return err
	}
	delete(lm.WLocks, key)

	return nil
}
