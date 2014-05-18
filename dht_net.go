package chord

/* TCP body for DHT requests */
type tcpBodyDHTGet struct {
	RingId string
	Vnode  *Vnode
	Key    string
}

type tcpBodyDHTSet struct {
	RingId string
	Vnode  *Vnode
	Key    string
	Value  []byte
}

type tcpBodyDHTList struct {
	RingId string
	Vnode  *Vnode
}

/* TCP body for DHT responses */
type tcpBodyRespDHTValue struct {
	Value []byte
	Err   error
}

type tcpBodyRespDHTKeys struct {
	Keys []string
	Err  error
}

type DHTStorage struct {
	kv map[string][]byte
}

func (vn *localVnode) DHTGet(ringId string, key string) ([]byte, error) {

	value := vn.store[ringId].kv[key]

	return value, nil
}

func (vn *localVnode) DHTSet(ringId string, key string, value []byte) error {

	vn.store[ringId].kv[key] = value

	return nil
}

func (vn *localVnode) DHTList(ringId string) ([]string, error) {

	return nil, nil
}
