package buddystore

type tcpBodyJoinRingReq struct {
	Target *Vnode
	Joiner *Vnode
	RingId string
}

type tcpBodyJoinRingResp struct {
	Vnodes []*Vnode

	// Extends:
	TCPResponseImpl
}
