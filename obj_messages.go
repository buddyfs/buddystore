package buddystore

// TCP Body for object server requests
type tcpBodyGet struct {
	Vnode   *Vnode
	Key     string
	Version uint
}

type tcpBodySet struct {
	Vnode   *Vnode
	Key     string
	Version uint
	Value   []byte
}

type tcpBodyList struct {
	Vnode *Vnode
}

type tcpBodyBulkSet struct {
	Vnode    *Vnode
	Key      string
	ValueLst []KVStoreValue
}

// TCP body for object server responses
type tcpBodyRespValue struct {
	Value []byte

	// Extends:
	tcpResponseImpl
}

type tcpBodyRespKeys struct {
	Keys []string

	// Extends:
	tcpResponseImpl
}
