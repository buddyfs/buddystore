package buddystore

type tcpBodyLMRLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
}

type tcpBodyLMRLockResp struct {
	LockId  string
	Version uint

	// Extends:
	tcpResponseImpl
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

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMCommitWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  uint
}

type tcpBodyLMCommitWLockResp struct {
	Dummy bool

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMAbortWLockReq struct {
	Vn       *Vnode
	SenderID string
	Key      string
	Version  uint
}

type tcpBodyLMAbortWLockResp struct {
	Dummy bool

	// Extends:
	tcpResponseImpl
}
