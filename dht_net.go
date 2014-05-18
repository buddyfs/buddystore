package chord

/* TCP body for DHT requests */
type tcpBodyDHTGet struct {
    ringId   string
    Vnode   *Vnode
    key      string
}

type tcpBodyDHTSet struct {
    ringId    string
    Vnode    *Vnode
    key       string
    value   []byte
}

type tcpBodyDHTList struct {
    ringId    string
    Vnode    *Vnode
}

/* TCP body for DHT responses */
type tcpBodyRespDHTValue {
    Value []byte
    Err     error
}

type tcpBodyRespDHTKeys {
    keys   []string
    Err      error
}

func (vn *localVnode) DHTGet (ringId string, key string) ([]byte, error) {
}

func (vn *localVnode) DHTSet (ringId string, key string, value []byte) (error) {}

func (vn *localVnode) DHTList (ringId string) ([]string, error) {
}


