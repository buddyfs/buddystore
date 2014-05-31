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
	Version  uint
	Timeout  uint
}

type tcpBodyLMWLockResp struct {
	LockId  string
    Version uint
	Timeout uint
	Err     error
}

type tcpBodyLMCommitWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  uint
}

type tcpBodyLMCommitWLockResp struct {
	Err error
}

type tcpBodyLMAbortWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  uint
}

type tcpBodyLMAbortWLockResp struct {
	Err error
}
