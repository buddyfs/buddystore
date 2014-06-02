/*
This file wraps the core chord library with DHT functionalities
*/

package chord

import (
	"fmt"
)

/* The DHT store will be associated with the ring it is meant for */
type DHTStore struct {
	ring *Ring
}

type DHTStoreIntf interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	List() []string
}

/* Get the public key from .ssh folder and start GET RPC */
func (kv *DHTStore) Get(key string) ([]byte, error) {
	succVnodes, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	fmt.Print(succVnodes)
	res, errDhtGet := kv.ring.transport.DHTGet(succVnodes[0], key)
	fmt.Println(res)
	return res, errDhtGet
}

/* Get the public key from .ssh folder and start SET RPC */
func (kv *DHTStore) Set(key string, value []byte) error {
	succVnodes, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Print(succVnodes)
	errDhtSet := kv.ring.transport.DHTSet(succVnodes[0], key, value)
	return errDhtSet
}

/* Get the public key from .ssh folder and start LIST RPC */
func (kv *DHTStore) List() error {
	// Will be used only by the replicators
	return nil
}
