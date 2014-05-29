type KVStoreClient interface {
    Get (key string) (val []byte, error)
    Set (key string, []byte val) (error)
}


