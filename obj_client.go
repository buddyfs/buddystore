package chord

import (
	"fmt"

	"github.com/golang/glog"
)

type KVStoreClient interface {
	Get(key string) ([]byte, error)
	Set(key string, val []byte) error
}

type KVStoreClientImpl struct {
	ring Ring
	lm   LMClientIntf

	// Implements
	KVStoreClient
}

var _ KVStoreClient = &KVStoreClientImpl{}

/*
 * Basic KV store Get operation
 * TODO: Improve Godoc
 */
func (kv KVStoreClientImpl) Get(key string) ([]byte, error) {
	/*
	 * Inform the lock manager we're interested in reading the value for key.
	 * Expected return value: current version number associated with key.
	 * Expected error conditions:
	 *   - Network failure     => Retryable failure
	 *   - Key does not exist  => Fail immediately
	 */
	v, err := kv.lm.RLock(key)

	// TODO: Inspect error and determine if we can retry the operation.
	if err != nil {
		glog.Errorf("Error acquiring RLock in Get(%q): %q", key, err)
		return nil, err
	}

	succVnodes, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Get(%q): %q", key, err)
		return nil, err
	}

	if glog.V(2) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	value, err := kv.ring.transport.Get(succVnodes[0], key, v)

	// TODO: Should we retry this call if there is an error?
	return value, err
}

/* Get the public key from .ssh folder and start SET RPC */
func (kv *KVStoreClientImpl) Set(key string, value []byte) error {
	succVnodes, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Print(succVnodes)
	//  errDhtSet := kv.ring.transport.DHTSet(succVnodes[0], "abcd", key, value)
	//  return errDhtSet
	return nil
}

/* Get the public key from .ssh folder and start LIST RPC */
func (kv *KVStoreClientImpl) List() ([]string, error) {
	// Will be used only by the replicators
	return nil, nil
}
