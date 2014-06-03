package chord

/* TCP body for DHT requests */
type tcpBodyDHTGet struct {
	Vnode   *Vnode
	Key     string
	Version uint
}

type tcpBodyDHTSet struct {
	Vnode   *Vnode
	Key     string
	Version uint
	Value   []byte
}

type tcpBodyDHTList struct {
	Vnode *Vnode
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

// New Vnode operations added for supporting DHT

type KVStore struct {
	kv map[string][]byte
}

func (vn *localVnode) Get(key string, version uint) ([]byte, error) {
	value := vn.store.kv[key]

	return value, nil
}

func (vn *localVnode) Set(key string, version uint, value []byte) error {
	/* TODO: Handle delete? */
	vn.store.kv[key] = value

	return nil
}

func (vn *localVnode) List() ([]string, error) {
	ret := make([]string, 0, len(vn.store.kv))

	for key := range vn.store.kv {
		ret = append(ret, key)
	}

	return ret, nil
}
