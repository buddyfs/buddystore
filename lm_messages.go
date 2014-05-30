package chord

type tcpBodyLMRLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
}

type tcpBodyLMRLockResp struct {
	LockId  string
	Version uint
	Err     error
}

type tcpBodyLMWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  int
	Timeout  uint
}

type tcpBodyLMWLockResp struct {
	LockId  string
	Timeout uint
	Err     error
}

type tcpBodyCommitWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  int
}

type tcpBodyCommitWLockResp struct {
	Err error
}

type tcpBodyAbortWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  uint
}

type tcpBodyAbortWLockResp struct {
	Err error
}
