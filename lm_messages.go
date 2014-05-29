package chord

type tcpBodyLMRLockReq struct {
	Vn  *Vnode
	Key string
}

type tcpBodyLMRLockResp struct {
	LockId  string
	Version uint
	Err     error
}

type tcpBodyLMWLockReq struct {
	RingId  string
	Key     string
	Version int
	Timeout uint
}

type tcpBodyLMLockResp struct {
	LockId  string
	Timeout uint
	Err     error
}

type tcpBodyCommitWLockReq struct {
	RingId  string
	Key     string
	Version int
}

type tcpBodyCommitWLockResp struct {
	Err error
}

type tcpBodyAbortWLockReq struct {
	RingId  string
	Key     string
	Version uint
}
