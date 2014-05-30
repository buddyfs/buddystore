package chord

import (
    "fmt"
    "crypto/rand"
    "encoding/hex"
)
/*
TODO : Discuss : LockID is currently 160 bits long. Is that good enough? */
type WLockEntry struct {
    nodeID      string
    LockID      string
    version     uint
    timeout     uint
}

type RLockEntry struct {
    nodeSet     map[string]string   //  For each key, there will be a list of nodes and corresponding LockIDs given out. Used during invalidation
}

//  In-memory implementation of LockManager State
type LManager struct {
    //  Local state managed by the LockManager
    CurrentLM   bool    // Boolean flag which says if the node is the current Lock Manager. 

    VersionMap     map[string]uint //  key-version mappings. A map of key to the corresponding version
    RLocks      map[string]*RLockEntry  // Will have the nodeSets for whom the RLocks have been provided for a key
    WLocks      map[string]*WLockEntry  // Will have mapping from key to the metadata to be maintained

}

/* Should be extensible to be used by any underlying storage implementation */
type LManagerIntf interface {
	createRLock(key string) (string, uint, error)
	checkWLock(key string) (bool, uint, error)
	createWLock(key string, version string, timeout uint) (string, uint, error)
	commitWLock(key string, version string) error
	abortWLockReq(key string, version uint) error
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
    rLockEntry := lm.RLocks[key]
    lockID, err := getLockID()
    if err != nil {
        return "", 0 , err
    }
    rLockEntry.nodeSet[nodeID] = lockID    // Added the nodeID to the nodeSet for the given key
    return lockID,lm.VersionMap[key], nil
}
