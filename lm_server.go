package chord

type LMRLockReq struct {
	RingId string
	Key    string
}

type LMRLockResp struct {
	LockId string
	Err    error
}

type LMWLockReq struct {
	RingId  string
	Key     string
	Version int
	Timeout uint
}

type LMLockResp struct {
	LockId  string
	Timeout uint
	Err     error
}

type CommitWLockReq struct {
	RingId  string
	Key     string
	Version int
}

type CommitWLockResp struct {
	Err error
}

type AbortWLockReq struct {
	RingId  string
	Key     string
	Version uint
}
