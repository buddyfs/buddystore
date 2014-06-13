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
	CopySet map[string][]string //  For each key, there will be a list of nodes and corresponding LockIDs given out. Used during invalidation
}

/* Struct for the Log used for Lock state replication */
type OpsLogEntry struct {
	OpNum   uint64     //  Operation Number
	Op      string     //  Operation that was performed
	Key     string     //  Key on which the operation was performed
	Version uint       //  Version number of the Key
	Timeout *time.Time // Timeout setting if any. For instance, WLocks have timeouts associated with them. When the primary fails, the second should know when to invalidate that entry

	// For handling replication. Nodes should be able to reconstruct the state using this log
	CopySet     *RLockEntry //  2D array. Maps nodeID and remote address. Used for RLock calls to maintain copysets
	LockId      string      //  For RLocks and WLocks, the LockID which the primary LM used should be replicated to the secondaries. Do not generate new LockIDs in the secondary
	CommitPoint uint64      // Operation number of the last committed operation
	Vn          *Vnode      //  Identity of the VNode, can be extended to be used for sending out of band signals to the primary.
}

//  In-memory implementation of LockManager that implements LManagerIntf
type LManager struct {
	//  Local state managed by the LockManager
	Ring      *Ring  //  This is to get the Ring's transport when the server has to send invalidations to lm_client cache
	Vn        *Vnode //  The Vnode this LockManager is associated with
	CurrentLM bool   // Boolean flag which says if the node is the current Lock Manager.

	VersionMap map[string]uint        //  key-version mappings. A map of key to the corresponding version
	RLocks     map[string]*RLockEntry // Will have the CopySets for whom the RLocks have been provided for a key
	WLocks     map[string]*WLockEntry // Will have mapping from key to the metadata to be maintained
	wLockMut   sync.Mutex             // Lock for synchronizing access to WLocks
	rLockMut   sync.Mutex             // Lock for synchronizing access to RLocks
	verMapMut  sync.Mutex             // Lock for synchronizing VersionMap accesses

	TimeoutTicker *time.Ticker // Ticker that will periodically check WLocks for invalidation
	LMCheckTicker *time.Ticker // Ticker that will periodically checks if LM has changed

	currOpNum uint64         // Current Operation Number
	OpsLog    []*OpsLogEntry //  Actual log used for write-ahead logging each operation
	opsLogMut sync.Mutex     //  Lock for synchronizing access to the OpsLog

	// HARP Replication
	CommitPoint uint64 //  Current Commit point of LManager.
	CommitIndex int
}

/* Should be extensible to be used by any underlying storage implementation */
type LManagerIntf interface {
	createRLock(key string, nodeID string, remoteAddr string, opsLogInEntry *OpsLogEntry) (string, uint, uint64, error)
	checkWLock(key string) (bool, uint, error)
	createWLock(key string, version uint, timeout uint, nodeID string, opsLogEntry []*OpsLogEntry) (string, uint, uint, uint64, error)
	commitWLock(key string, version uint, opsLogInEntry *OpsLogEntry) (uint64, error)
	abortWLock(key string, version uint, opsLogInEntry *OpsLogEntry) (uint64, error)
}

func (lm *LManager) appendToLog(opsLogInEntry *OpsLogEntry) {
	// Backup node - Log and return
	lm.opsLogMut.Lock()
	lm.OpsLog = append(lm.OpsLog, opsLogInEntry)
	if len(lm.OpsLog) > 1 {
		for i := len(lm.OpsLog) - 1; lm.OpsLog[i].OpNum < lm.OpsLog[i-1].OpNum; i-- {
			lm.OpsLog[i].OpNum, lm.OpsLog[i-1].OpNum = lm.OpsLog[i-1].OpNum, lm.OpsLog[i].OpNum
		}
		// Move commitpoint, if all the previous logs are available
		for i := lm.CommitIndex + 1; i <= len(lm.OpsLog)-1; i++ {
			if (i == 0) || (lm.OpsLog[i].OpNum == lm.OpsLog[i-1].OpNum+1) {
				lm.CommitPoint++
				lm.CommitIndex++
			}
		}
	}
	lm.opsLogMut.Unlock()
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

/* Logic is moved to stabilize operation in vnode.go */
func (lm *LManager) ScheduleLMCheckTicker() {
	lm.LMCheckTicker = time.NewTicker(100 * time.Millisecond)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-lm.LMCheckTicker.C:
				// Lookup for RingID
				LMVnodes, err := lm.Ring.Lookup(1, []byte(lm.Ring.config.RingId))
				if err != nil {
					continue
				}
				if lm.Vn.String() == LMVnodes[0].String() {
					if lm.CurrentLM {
						// No-op
					} else {
						lm.CurrentLM = true
						/* TODO : I am the new LockManager, two cases :
						   1. The previous LockManager died
						   2. I just joined and figured out that I am the LockManager.
						*/

					}
				} else {
					if lm.CurrentLM {
						//  I was the LockManager (or I am the one with the best knowledge of the previous LM) , now someone else has joined, give him the full LockState and set his CurrentLM when he is ready.
					} else {
						lm.CurrentLM = false
						// No-op
					}
				}

			case <-quit:
				lm.LMCheckTicker.Stop()
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

/*
Once both the secondary nodes get back with a success indication, then the method can reply that the RLock is provided, else it should fail.
*/
func (lm *LManager) createRLock(key string, nodeID string, remoteAddr string, opsLogInEntry *OpsLogEntry) (string, uint, uint64, error) {

	if opsLogInEntry != nil && lm.CurrentLM {
		return "", 0, lm.CommitPoint, nil
	}

	if !lm.CurrentLM {
		if opsLogInEntry == nil {
			return "", 0, 0, TransientError("[%s] 500: Retry, RLock request reached the non-Primary Lock Manager", lm.Vn.Host)
		}
		lm.appendToLog(opsLogInEntry)
		return opsLogInEntry.LockId, opsLogInEntry.Version, lm.CommitPoint, nil
	}

	lm.verMapMut.Lock()
	version := lm.VersionMap[key]
	lm.verMapMut.Unlock()
	if version == 0 {
		return "", 0, lm.CommitPoint, fmt.Errorf("[%s] ReadLock not possible. Key not present in LM", lm.Vn.Host)
	}

	lockID, err := getLockID()
	if err != nil {
		return "", 0, lm.CommitPoint, err
	}

	lm.opsLogMut.Lock()
	defer lm.opsLogMut.Unlock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "READ", Key: key, CopySet: lm.RLocks[key], LockId: lockID, CommitPoint: lm.CommitPoint, Vn: lm.Vn}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)

	// If current Lock Manager, replicate to next two nodes
	if lm.CurrentLM {
		vnodes, err := lm.Ring.transport.FindSuccessors(lm.Vn, NUM_LM_REPLICA, []byte(lm.Ring.config.RingId))
		if err != nil {
			return "", 0, lm.CommitPoint, TransientError("Retry Later. Expected : Atleast 2 successors to the LockManager, but only ", len(vnodes), " are available")
		}
		for i := range vnodes {
			if vnodes[i] == nil {
				continue
			}

			_, _, _, err := lm.Ring.Transport().RLock(vnodes[i], key, lm.Vn.String(), opsLogEntry)
			if err != nil {
				return "", 0, lm.CommitPoint, TransientError("Retry : Cannot replicate operation to enough replica")
			}
		}
	}
	lm.rLockMut.Lock()

	if lm.RLocks == nil {
		lm.RLocks = make(map[string]*RLockEntry)
	}

	if lm.RLocks[key] == nil {
		lm.RLocks[key] = &RLockEntry{}
	}
	rLockEntry := lm.RLocks[key]

	if rLockEntry.CopySet == nil {
		rLockEntry.CopySet = make(map[string][]string)
	}

	rLockEntry.CopySet[nodeID] = make([]string, 2)
	rLockEntry.CopySet[nodeID][0] = lockID     // Added the nodeID to the CopySet for the given key
	rLockEntry.CopySet[nodeID][1] = remoteAddr // Remote address added to invalidate it when a commit happens to this key
	lm.rLockMut.Unlock()

	return lockID, version, lm.CommitPoint, nil
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
The actual client will set only the first four parameters, the last parameter is an optimization for primary - backup communication.
We will pass nodeID for all the operations
*/
func (lm *LManager) createWLock(key string, version uint, timeout uint, nodeID string, opsLogInEntry *OpsLogEntry) (string, uint, uint, uint64, error) {

	if opsLogInEntry != nil && lm.CurrentLM {
		return "", 0, 0, lm.CommitPoint, nil
	}

	if !lm.CurrentLM {
		if opsLogInEntry == nil {
			return "", 0, 0, 0, TransientError("500: Retry, WLock request reached the non-Primary Lock Manager")
		}
		lm.appendToLog(opsLogInEntry)
		return opsLogInEntry.LockId, version, timeout, lm.CommitPoint, nil
	}

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
		return "", 0, 0, lm.CommitPoint, fmt.Errorf("Error while checking if a write lock exists already for that key")
	}
	lm.wLockMut.Lock()
	defer lm.wLockMut.Unlock()
	if present {
		return "", lm.WLocks[key].version, 0, lm.CommitPoint, fmt.Errorf("WriteLock not possible. Key is currently being updated")
	}

	lm.verMapMut.Lock()
	//  Check if requested version is greater than the committed version
	if version <= lm.VersionMap[key] {
		if version == 0 { // Client wants to update
			version = lm.VersionMap[key] + 1
		} else {
			return "", lm.VersionMap[key], 0, lm.CommitPoint, fmt.Errorf("Committed version is higher than requested version")
		}
	}
	lm.verMapMut.Unlock()

	lockID, err := getLockID()
	if err != nil {
		return "", 0, 0, lm.CommitPoint, err
	}
	t := time.Now().UTC()
	t = t.Add(time.Duration(timeout) * time.Second)
	lm.opsLogMut.Lock()
	defer lm.opsLogMut.Unlock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "WRITE", Key: key, Version: version, Timeout: &t, LockId: lockID, CommitPoint: lm.CommitPoint, Vn: lm.Vn}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)

	// If current LockManager, replicate the operation to the next two nodes
	if lm.CurrentLM {
		vnodes, err := lm.Ring.transport.FindSuccessors(lm.Vn, NUM_LM_REPLICA, []byte(lm.Ring.config.RingId))
		if err != nil {
			lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
			return "", 0, 0, lm.CommitPoint, TransientError("Retry Later. Expected : Atleast ", NUM_LM_REPLICA, " successors to the LockManager, but only ", len(vnodes), " are available")
		}
		for i := range vnodes {
			if vnodes[i] != nil {
				_, _, _, _, err := lm.Ring.Transport().WLock(vnodes[i], key, version, timeout, lm.Vn.String(), opsLogEntry)
				if err != nil {
					lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
					return "", 0, 0, lm.CommitPoint, TransientError("Retry : Cannot replicate operation to enough nodes")
				}
			}

		}
	}

	lm.WLocks[key] = &WLockEntry{nodeID: nodeID, LockID: lockID, version: version, timeout: &t}
	return lockID, version, timeout, lm.CommitPoint, nil
}

func (lm *LManager) commitWLock(key string, version uint, nodeID string, opsLogInEntry *OpsLogEntry) (uint64, error) {

	if opsLogInEntry != nil && lm.CurrentLM {
		return lm.CommitPoint, nil
	}

	if !lm.CurrentLM {
		if opsLogInEntry == nil {
			return lm.CommitPoint, TransientError("500: Retry, Commit WLock request reached the non-Primary Lock Manager")
		}
		lm.appendToLog(opsLogInEntry)
		return lm.CommitPoint, nil
	}

	present, ver, err := lm.checkWLock(key)
	if err != nil {
		return lm.CommitPoint, fmt.Errorf("Error while looking up the existing set of write locks in Lock Manager")
	}
	if !present {
		return lm.CommitPoint, fmt.Errorf("Lock not available. Cannot commit")
	}
	if ver != version {
		return lm.CommitPoint, fmt.Errorf("Requested version doesn't match with the version locked. Cannot commit")
	}

	lm.opsLogMut.Lock()
	defer lm.opsLogMut.Unlock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "COMMIT", Key: key, Version: version, CommitPoint: lm.CommitPoint, Vn: lm.Vn}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)

	// If current LockManager, replicate operation on the next NUM_LM_REPLICA nodes
	if lm.CurrentLM {
		vnodes, err := lm.Ring.transport.FindSuccessors(lm.Vn, NUM_LM_REPLICA, []byte(lm.Ring.config.RingId))
		if err != nil {
			lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
			return lm.CommitPoint, TransientError("Retry Later. Expected : Atleast ", NUM_LM_REPLICA, " successors to the LockManager, but only ", len(vnodes), " are available")
		}
		for i := range vnodes {
			if vnodes[i] == nil {
				continue
			}

			_, err := lm.Ring.Transport().CommitWLock(vnodes[i], key, version, lm.Vn.String(), opsLogEntry)
			if err != nil {
				lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
				return lm.CommitPoint, TransientError("Retry : Operation couldn't be replicated to enough nodes. Got error :  %q", err)
			}
		}
	}

	lm.wLockMut.Lock()
	lm.verMapMut.Lock()
	if lm.VersionMap == nil {
		lm.VersionMap = make(map[string]uint)
	}
	lm.VersionMap[key] = version
	lm.verMapMut.Unlock()
	delete(lm.WLocks, key)
	lm.wLockMut.Unlock()

	/* The Read cache invalidation should happen only after the result is returned to the client. Consistency */
	defer func() {
		/* If it is the first version, then there could not be any previous version cached in RLock caches */
		if version == 1 {
			return
		}
		lm.rLockMut.Lock()
		if lm.RLocks[key] != nil {
			for k, v := range lm.RLocks[key].CopySet {
				err := lm.Ring.transport.InvalidateRLock(&Vnode{Id: []byte(k), Host: v[1]}, v[0])
				if err != nil {
					// No-op
				}
			}
		}
		delete(lm.RLocks, key)
		lm.rLockMut.Unlock()
	}()
	return lm.CommitPoint, nil
}

/* TODO : Minor : Fix this : We do not need the nodeID */
func (lm *LManager) abortWLock(key string, version uint, nodeID string, opsLogInEntry *OpsLogEntry) (uint64, error) {

	if opsLogInEntry != nil && lm.CurrentLM {
		return lm.CommitPoint, nil
	}

	if !lm.CurrentLM {
		if opsLogInEntry == nil {
			return lm.CommitPoint, TransientError("500: Retry, Abort WLock request reached the non-Primary Lock Manager")
		}
		lm.appendToLog(opsLogInEntry)
		return lm.CommitPoint, nil
	}

	present, ver, err := lm.checkWLock(key)
	if err != nil {
		return lm.CommitPoint, fmt.Errorf("Error while looking up the existing set of write locks in Lock Manager")
	}
	if !present {
		return lm.CommitPoint, fmt.Errorf("Lock not available. Nothing to abort")
	}
	if ver != version {
		return lm.CommitPoint, fmt.Errorf("Requested version doesn't match with the version locked. Cannot abort")
	}

	lm.opsLogMut.Lock()
	defer lm.opsLogMut.Unlock()
	lm.currOpNum++
	opsLogEntry := &OpsLogEntry{OpNum: lm.currOpNum, Op: "ABORT", Key: key, Version: version, CommitPoint: lm.CommitPoint, Vn: lm.Vn}
	lm.OpsLog = append(lm.OpsLog, opsLogEntry)

	// If Current LockManager then replicate operation to the next NUM_LM_REPLICA nodes
	if lm.CurrentLM {
		vnodes, err := lm.Ring.transport.FindSuccessors(lm.Vn, NUM_LM_REPLICA, []byte(lm.Ring.config.RingId))
		if err != nil {
			lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
			return lm.CommitPoint, TransientError("Retry Later. Expected : Atleast ", NUM_LM_REPLICA, " successors to the LockManager, but only ", len(vnodes), " are available")
		}
		for i := range vnodes {
			_, err := lm.Ring.Transport().AbortWLock(vnodes[i], key, version, lm.Vn.String(), opsLogEntry)
			if err != nil {
				lm.OpsLog = lm.OpsLog[:len(lm.OpsLog)-1]
				return lm.CommitPoint, TransientError("Retry : Operation couldn't be replicated to enough replica")
			}
		}
	}

	lm.wLockMut.Lock()
	delete(lm.WLocks, key)
	lm.wLockMut.Unlock()
	return lm.CommitPoint, nil
}

func (lm *LManager) ReplayLog() {
	clearLMForReplay(lm)
	/*if !lm.SyncWithSuccessors() {
	    fmt.Println("Genesis : No Replay needed")
	}*/
	// (Except for the opsLog Lock) Locks not required as all changes are local only
	lm.opsLogMut.Lock()
	for i := range lm.OpsLog {
		switch lm.OpsLog[i].Op {

		case "WRITE":
			lm.WLocks[lm.OpsLog[i].Key] = &WLockEntry{nodeID: lm.OpsLog[i].Vn.String(), LockID: lm.OpsLog[i].LockId, version: lm.OpsLog[i].Version, timeout: lm.OpsLog[i].Timeout}

		case "READ":
			if lm.RLocks == nil {
				lm.RLocks = make(map[string]*RLockEntry)
			}
			if lm.RLocks[lm.OpsLog[i].Key] == nil {
				lm.RLocks[lm.OpsLog[i].Key] = &RLockEntry{}
			}
			rLockEntry := lm.RLocks[lm.OpsLog[i].Key]
			if rLockEntry.CopySet == nil {
				rLockEntry.CopySet = make(map[string][]string)
			}
			rLockEntry.CopySet[lm.OpsLog[i].Vn.String()] = make([]string, 2)
			rLockEntry.CopySet[lm.OpsLog[i].Vn.String()][0] = lm.OpsLog[i].LockId  // Added the nodeID to the CopySet for the given key
			rLockEntry.CopySet[lm.OpsLog[i].Vn.String()][1] = lm.OpsLog[i].Vn.Host // Remote address added to invalidate it when a commit happens to this key

		case "COMMIT":
			lm.VersionMap[lm.OpsLog[i].Key] = lm.OpsLog[i].Version
			delete(lm.WLocks, lm.OpsLog[i].Key)

		case "ABORT":
			delete(lm.WLocks, lm.OpsLog[i].Key)

		default:
			// No-op
		}
	}
	lm.opsLogMut.Unlock()
}

/*   TODO : What if I have no log? Two possibilities
1. I just joined. Go and ask the successor for the opsLog and replay it.
2. Its genesis, there was no LM before this. Go and ask the successor to confirm it.
Well, just ask the successor and execute whatever you have to..
Return true, if the successor has state, else return false (birth)
*/
func (lm *LManager) SyncWithSuccessor() bool {
	return false
}
