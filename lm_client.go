type LMClient interface {
    RLock (key string) (int, error)
    WLock (key string, version int, timeout uint) (uint, error)
    CommitWLock (key string, version int)
    AbortWLock (key string, version int)
}

