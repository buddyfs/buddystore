package chord

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

/*
TODO : Discuss : LockID is currently 160 bits long. Is that good enough? */
type WLockEntry struct {
	nodeID  string
	LockID  string
	version uint
	timeout uint
}

type RLockEntry struct {
	nodeSet map[string]string //  For each key, there will be a list of nodes and corresponding LockIDs given out. Used during invalidation
}

//  In-memory implementation of LockManager that implements LManagerIntf
type LManager struct {
	//  Local state managed by the LockManager
	CurrentLM bool // Boolean flag which says if the node is the current Lock Manager.

	VersionMap map[string]uint        //  key-version mappings. A map of key to the corresponding version
	RLocks     map[string]*RLockEntry // Will have the nodeSets for whom the RLocks have been provided for a key
	WLocks     map[string]*WLockEntry // Will have mapping from key to the metadata to be maintained

}

/* Should be extensible to be used by any underlying storage implementation */
type LManagerIntf interface {
	createRLock(key string, nodeID string) (string, uint, error)
	checkWLock(key string) (bool, uint, error)
	createWLock(key string, version uint, timeout uint, nodeID string) (string, uint, uint, error)
	commitWLock(key string, version uint) error
	abortWLock(key string, version uint) error
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

/*
TODO : Discussion. When the server part comes up, it should instantiate multiple LockManager instances - one for each ring the node is part of.
Then based on the request that comes in, the server should be able to delegate to the correct LM instance. So the net.go handleConn should have a map(ringId, LMinstance).

*/
func (lm *LManager) createRLock(key string, nodeID string) (string, uint, error) {

	version := lm.VersionMap[key]
	if version == 0 {
		return "", 0, fmt.Errorf("ReadLock not possible. Key not present in LM")
	}

	lockID, err := getLockID()
	if err != nil {
		return "", 0, err
	}

	if lm.RLocks == nil {
		lm.RLocks = make(map[string]*RLockEntry)
	}

	if lm.RLocks[key] == nil {
		lm.RLocks[key] = &RLockEntry{}
	}
	rLockEntry := lm.RLocks[key]

	if rLockEntry.nodeSet == nil {
		rLockEntry.nodeSet = make(map[string]string)
	}

	rLockEntry.nodeSet[nodeID] = lockID // Added the nodeID to the nodeSet for the given key
	return lockID, lm.VersionMap[key], nil
}

func (lm *LManager) checkWLock(key string) (bool, uint, error) {
	wLockEntry := lm.WLocks[key]
	if wLockEntry == nil {
		return false, 0, nil
	}

	return true, wLockEntry.version, nil
}

/*
TODO : Discuss : If Wlock exists then it will give back the version that is currently being written, not the committed version
*/
func (lm *LManager) createWLock(key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {
	if lm.WLocks == nil {
		lm.WLocks = make(map[string]*WLockEntry)
	}

	present, _, err := lm.checkWLock(key)
	if err != nil {
		return "", 0, 0, fmt.Errorf("Error while checking if a write lock exists already for that key")
	}
	if present {
		return "", lm.WLocks[key].version, 0, fmt.Errorf("WriteLock not possible. Key is currently being updated")
	}

	//  Check if requested version is greater than the committed version
	if version <= lm.VersionMap[key] {
		return "", lm.VersionMap[key], 0, fmt.Errorf("Committed version is higher than requested version")
	}

	lockID, err := getLockID()
	if err != nil {
		return "", 0, 0, err
	}
	lm.WLocks[key] = &WLockEntry{nodeID: nodeID, LockID: lockID, version: version, timeout: timeout}
	return lockID, version, timeout, nil
}

/*
TODO : Discuss : Is the version number really needed here? The client can just send the LockID to get it committed. The WLocks implementation will change accordingly
*/
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

	/*TODO Wait until the backup LMs also perform the same operation and then commit it */
	if lm.VersionMap == nil {
		lm.VersionMap = make(map[string]uint)
	}
	lm.VersionMap[key] = version
	delete(lm.WLocks, key)

	/*TODO : Notifying nodeset */

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

	delete(lm.WLocks, key)
	return nil
}
