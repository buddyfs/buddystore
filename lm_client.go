package chord

type WLockVal struct {
    timeout uint
    lockID string
}

type LManager struct {
    RLocks map[string]string       //  Map of <keys, lockId>
    WLocks map[string]*WLockVal    //  Map of <keys, WriteLock Values
}

type LMClient interface {
	RLock(key string) (int, error)
	WLock(key string, version int, timeout uint) (uint, error)
	CommitWLock(key string, version int)
	AbortWLock(key string, version int)
}

/*
Called by the client before making a read on the key
*/
func (lm * LManager) RLock(key string) (version int, err error) {
     return 0, nil
}
