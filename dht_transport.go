package chord

type DHTTransport struct {
	TCPTransport
}

type DHTTransportIntf interface {
	DHTGet(string, string) ([]byte, error)
	DHTSet(string, string, []byte) error
	DHTList(string) []string
}
