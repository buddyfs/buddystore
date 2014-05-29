package chord

type KVStoreClient interface {
	Get(key string) ([]byte, error)
	Set(key string, val []byte) error
}
