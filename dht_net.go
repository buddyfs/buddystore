package chord

type tcpBodyDht struct {
  ringId string
}

type tcpBodyDhtGet struct {
  tcpBodyDht
  key string
}

type tcpBodyDhtSet struct {
  tcpBodyDht
  key string
  value []byte
}

type tcpBodyDhtList struct {
  tcpBodyDht
}
