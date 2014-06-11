package buddystore

type tcpBodyLMRLockReq struct {
	Vn                 *Vnode
	SenderID           string
	Key                string
	SenderAddr         string
	OpsLogEntryPrimary *OpsLogEntry
}

type tcpBodyLMRLockResp struct {
	LockId      string
	Version     uint
	CommitPoint uint64

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMWLockReq struct {
	Vn                 *Vnode
	SenderID           string
	Key                string
	Version            uint
	Timeout            uint
	OpsLogEntryPrimary *OpsLogEntry
}

type tcpBodyLMWLockResp struct {
	LockId      string
	Version     uint
	Timeout     uint
	CommitPoint uint64

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMCommitWLockReq struct {
	Vn                 *Vnode
	SenderID           string
	Key                string
	Version            uint
	OpsLogEntryPrimary *OpsLogEntry
}

type tcpBodyLMCommitWLockResp struct {
	Dummy       bool
	CommitPoint uint64

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMAbortWLockReq struct {
	Vn                 *Vnode
	SenderID           string
	Key                string
	Version            uint
	OpsLogEntryPrimary *OpsLogEntry
}

type tcpBodyLMAbortWLockResp struct {
	Dummy       bool
	CommitPoint uint64

	// Extends:
	tcpResponseImpl
}

type tcpBodyLMInvalidateRLockReq struct {
	Vn     *Vnode
	LockID string
}

type tcpBodyLMInvalidateRLockResp struct {
	Dummy bool

	// Extends:
	tcpResponseImpl
}
