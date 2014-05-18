/*
This file wraps the core chord library with DHT functionalities
*/

package chord

import (
	"fmt"
)

type DHTStore struct {
    ring   *Ring
}

type DHTStoreIntf interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	List() []string
}

func (kv *DHTStore) Get(key string) ([]byte, error) {
	_, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		fmt.Println(err)
	}
	return nil, nil
}

func (kv *DHTStore) Set(key string, value []byte) error {
	return nil
}
