package chord

type DHTTransport struct {
	TCPTransport
}

type DHTTransportIntf interface {
	Get(string, string) ([]byte, error)
	Set(string, string, []byte) error
	List(string) []string
}
