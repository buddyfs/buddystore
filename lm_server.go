package buddystore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

/*
TODO : Discuss : LockID is currently 160 bits long. Is that good enough? */
type WLockEntry struct {
	nodeID  string
	LockID  string
	version uint
	timeout *time.Time
}

type RLockEntry struct {
	nodeSet map[string][]string //  For each key, there will be a list of nodes and corresponding LockIDs given out. Used during invalidation
}

/* Struct for the Log used for Lock state replication */
type OpsLogEntry struct {
	OpNum   uint64     //  Operation Number
	Op      string     //  Operation that was performed
	Key     string     //  Key on which the operation was performed
	Version uint       //  Version number of the Key
	Timeout *time.Time // Timeout setting if any. For instance, WLocks have timeouts associated with them. When the primary fails, the second should know when to invalidate that entry
}

//  In-memory implementation of LockManager that implements LManagerIntf
type LManager struct {
	//  Local state managed by the LockManager
	Ring      *Ring //  This is to get the Ring's transport when the server has to send invalidations to lm_client cache
	CurrentLM bool  // Boolean flag which says if the node is the current Lock Manager.

	VersionMap map[string]uint        //  key-version mappings. A map of key to the corresponding version
	RLocks     map[string]*RLockEntry // Will have the nodeSets for whom the RLocks have been provided for a key
	WLocks     map[string]*WLockEntry // Will have mapping from key to the metadata to be maintained
	wLockMut   sync.Mutex             // Lock for synchronizing access to WLocks
	rLockMut   sync.Mutex             // Lock for synchronizing access to RLocks
	verMapMut  sync.Mutex             // Lock for synchronizing VersionMap accesses

	TimeoutTicker *time.Ticker // Ticker that will periodically check WLocks for invalidation

	currOpNum uint64         // Current Operation Number
	OpsLog    []*OpsLogEntry //  Actual log used for write-ahead logging each operation
	opsLogMut sync.Mutex     //  Lock for synchronizing access to the OpsLog

}

/* Should be extensible to be used by any underlying storage implementation */
type LManagerIntf interface {
	createRLock(key string, nodeID string, remoteAddr string) (string, uint, error)
	checkWLock(key string) (bool, uint, error)
	createWLock(key string, version uint, timeout uint, nodeID string) (string, uint, uint, error)
	commitWLock(key string, version uint) error
	abortWLock(key string, version uint) error
}

/*
Creates a new Ticker which checks the existing WLocks every 500 Milliseconds */
func (lm *LManager) scheduleTimeoutTicker() {
	lm.TimeoutTicker = time.NewTicker(500 * time.Millisecond)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-lm.TimeoutTicker.C:
				lm.wLockMut.Lock()
				t := time.Now().UTC()
				for k, v := range lm.WLocks {
					if v.timeout.Before(t) || v.timeout.Equal(t) {
						delete(lm.WLocks, k)
					}
				}
				lm.wLockMut.Unlock()
			case <-quit:
				lm.TimeoutTicker.Stop()
				return
			}
		}
	}()
}

/* LockID generator : 20 bits from crypto rand */
func getLockID() (string, error) {
	lockID := make([]byte, 20)
	_, err := rand.Read(lockID)
	if err != nil {
		return "", fmt.Errorf("Error while generating LockID : ", err)
	}
	// Encode the integer into a string and send a nil error response
	return hex.EncodeToString(lockID), nil
}

func (lm *LManager) createRLock(key string, nodeID string, remoteAddr string) (string, uint, error) {

	lm.verMapMut.Lock()
	version := lm.VersionMap[key]
	lm.verMapMut.Unlock()
	if version == 0 {
		return "", 0, fmt.Errorf("ReadLock not possible. Key not present in LM")
	}

	lockID, err := getLockID()
	if err != nil {
		return "", 0, err
	}

	lm.rLockMut.Lock()

	if lm.RLocks == nil {
		lm.RLocks = make(map[string]*RLockEntry)
	}

	if lm.RLocks[key] == nil {
		lm.RLocks[key] = &RLockEntry{}
	}
	rLockEntry := lm.RLocks[key]

	if rLockEntry.nodeSet == nil {
		rLockEntry.nodeSet = make(map[string][]string)
	}

	rLockEntry.nodeSet[nodeID] = make([]string, 2)
	rLockEntry.nodeSet[nodeID][0] = lockID     // Added the nodeID to the nodeSet for the given key
	rLockEntry.nodeSet[nodeID][1] = remoteAddr // Remote address added to invalidate it when a commit happens to this key
	lm.rLockMut.Unlock()
	return lockID, version, nil
}

func (lm *LManager) checkWLock(key string) (bool, uint, error) {
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	wLockEntry := lm.WLocks[key]
	if wLockEntry == nil {
		return false, 0, nil
	}

	return true, wLockEntry.version, nil
}

/*
TODO : Discuss : If Wlock exists then it will give back the version that is currently being written, not the committed version
TODO : Discuss : Do not give the requested timeout right away. Validation.
*/
func (lm *LManager) createWLock(key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {

	lm.wLockMut.Lock()
	if lm.WLocks == nil {
		lm.WLocks = make(map[string]*WLockEntry)
	}
	lm.wLockMut.Unlock()

	if lm.TimeoutTicker == nil {
		lm.scheduleTimeoutTicker()
	}

	present, _, err := lm.checkWLock(key)
	if err != nil {
		return "", 0, 0, fmt.Errorf("Error while checking if a write lock exists already for that key")
	}
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	if present {
		return "", lm.WLocks[key].version, 0, fmt.Errorf("WriteLock not possible. Key is currently being updated")
	}

	lm.verMapMut.Lock()
	//  Check if requested version is greater than the committed version
	if version <= lm.VersionMap[key] {
		if version == 0 { // Client wants to update
			version = lm.VersionMap[key] + 1
		} else {
			return "", lm.VersionMap[key], 0, fmt.Errorf("Committed version is higher than requested version")
		}
	}
	lm.verMapMut.Unlock()

	lockID, err := getLockID()
	if err != nil {
		return "", 0, 0, err
	}
	t := time.Now().UTC()
	t = t.Add(time.Duration(timeout) * time.Second)
	lm.opsLogMut.Lock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "WRITE", Key: key, Version: version, Timeout: &t}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)
	lm.WLocks[key] = &WLockEntry{nodeID: nodeID, LockID: lockID, version: version, timeout: &t}
	lm.opsLogMut.Unlock()
	return lockID, version, timeout, nil
}

func (lm *LManager) commitWLock(key string, version uint, nodeID string) error {
	present, ver, err := lm.checkWLock(key)
	if err != nil {
		return fmt.Errorf("Error while looking up the existing set of write locks in Lock Manager")
	}
	if !present {
		return fmt.Errorf("Lock not available. Cannot commit")
	}
	if ver != version {
		return fmt.Errorf("Requested version doesn't match with the version locked. Cannot commit")
	}

	lm.verMapMut.Lock()
	/*TODO Wait until the backup LMs also perform the same operation and then commit it */
	if lm.VersionMap == nil {
		lm.VersionMap = make(map[string]uint)
	}
	lm.VersionMap[key] = version
	lm.verMapMut.Unlock()
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	lm.opsLogMut.Lock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "COMMIT", Key: key, Version: version, Timeout: nil}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)
	delete(lm.WLocks, key)
	lm.opsLogMut.Unlock()

	/* If it is the first version, then there could not be any previous version cached in RLock caches */
	if version == 1 {
		return nil
	}

	lm.rLockMut.Lock()
	if lm.RLocks[key] != nil {
		for k, v := range lm.RLocks[key].nodeSet {
			err := lm.Ring.transport.InvalidateRLock(&Vnode{Id: []byte(k), Host: v[1]}, v[0])
			if err != nil {
				// No-op
			}
		}
	}
	delete(lm.RLocks, key)
	lm.rLockMut.Unlock()
	return nil
}

func (lm *LManager) abortWLock(key string, version uint, nodeID string) error {
	present, ver, err := lm.checkWLock(key)
	if err != nil {
		return fmt.Errorf("Error while looking up the existing set of write locks in Lock Manager")
	}
	if !present {
		return fmt.Errorf("Lock not available. Nothing to abort")
	}
	if ver != version {
		return fmt.Errorf("Requested version doesn't match with the version locked. Cannot abort")
	}

	lm.opsLogMut.Lock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "ABORT", Key: key, Version: version, Timeout: nil}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)
	lm.wLockMut.Lock()
	delete(lm.WLocks, key)
	lm.wLockMut.Unlock()
	lm.opsLogMut.Unlock()
	return nil
}
