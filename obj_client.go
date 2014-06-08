package buddystore

import (
	"fmt"

	"github.com/golang/glog"
)

type KVStoreClient interface {
	Get(key string) ([]byte, error)
	Set(key string, val []byte) error
}

type KVStoreClientImpl struct {
	ring RingIntf
	lm   LMClientIntf

	// Implements
	KVStoreClient
}

var _ KVStoreClient = &KVStoreClientImpl{}

func NewKVStoreClient(ring *Ring) *KVStoreClientImpl {
	lm := &LManagerClient{Ring: ring, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	return &KVStoreClientImpl{ring: ring, lm: lm}
}

func NewKVStoreClientWithLM(ringIntf RingIntf, lm LMClientIntf) *KVStoreClientImpl {
	return &KVStoreClientImpl{ring: ringIntf, lm: lm}
}

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
	v, err := kv.lm.RLock(key, false)

	// TODO: Inspect error and determine if we can retry the operation.
	if err != nil {
		glog.Errorf("Error acquiring RLock in Get(%q): %s", key, err)
		return nil, err
	}

	succVnodes, err := kv.ring.Lookup(kv.ring.GetNumSuccessors(), []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Get(%q): %q", key, err)
		return nil, err
	}

	if glog.V(2) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	if len(succVnodes) == 0 {
		glog.Errorf("No successors found during Lookup in Get(%q)", key)
		return nil, fmt.Errorf("No Successors found")
	}

	// TODO: Choose a random successor instead of the first one.
	value, err := kv.ring.Transport().Get(succVnodes[0], key, v)

	// TODO: Should we retry this call if there is an error?
	return value, err
}

/*
 * Basic KV store Set operation
 * TODO: Improve Godoc
 */
func (kv *KVStoreClientImpl) Set(key string, value []byte) error {
	/*
	 * Inform the lock manager we're interested in setting the value for key.
	 * Expected return value: next available version number to write value to.
	 * Expected error conditions:
	 *   - Network failure     => Retryable failure
	 *   - Key does not exist  => Fail immediately
	 *   - Access permissions? => Fail immediately
	 */
	v, err := kv.lm.WLock(key, 0, 10)

	// TODO: Inspect error and determine if we can retry the operation.
	if err != nil {
		glog.Errorf("Error acquiring WLock in Set(%q): %d, %q", key, v, err)
		return err
	}

	succVnodes, err := kv.ring.Lookup(kv.ring.GetNumSuccessors(), []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Set(%q): %q", key, err)
		return err
	}

	if len(succVnodes) == 0 {
		glog.Errorf("No successors found during Lookup in Get(%q)", key)
		return fmt.Errorf("No Successors found")
	}

	if glog.V(2) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	// This request should always go to the master node.
	// Replication happens at the master.
	err = kv.ring.Transport().Set(succVnodes[0], key, v, value)

	if err != nil {
		glog.Errorf("Aborting Set(%q, %d) due to error: %q", key, v, err)

		// Best-effort Abort
		kv.lm.AbortWLock(key, v)

		// Even if the Abort command failed, we can safely return
		// because the lock manager will timeout and abort for us.
		return err
	}

	err = kv.lm.CommitWLock(key, v)
	return err
}
