package buddystore

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
)

type KVStoreClient interface {
	Get(key string, retry bool) ([]byte, error)
	Set(key string, val []byte) error
	GetForSet(key string, retry bool) ([]byte, uint, error)
	SetVersion(key string, version uint, val []byte) error
}

type KVStoreClientImpl struct {
	ring RingIntf
	lm   LMClientIntf

	// Implements: KVStoreClient
}

var _ KVStoreClient = &KVStoreClientImpl{}

func NewKVStoreClient(ring *Ring) *KVStoreClientImpl {
	lm := &LManagerClient{Ring: ring, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	return &KVStoreClientImpl{ring: ring, lm: lm}
}

func NewKVStoreClientWithLM(ringIntf RingIntf, lm LMClientIntf) *KVStoreClientImpl {
	return &KVStoreClientImpl{ring: ringIntf, lm: lm}
}

// Inform the lock manager we're interested in reading the value for key.
// Expected return value:
//    Current version number associated with key
// Expected error conditions:
//    Network failure     => Retryable failure
//    Key does not exist  => Fail immediately
//
// Once current version number has been successfully read, contact nodes
// KV Store to read value at expected version.
// Expected error conditions:
//    Key/version does not exist  => Retry with another node
//    All nodes returned error    => Fail
//
// Optimization:
//    Prioritize reading from local vnode if one of them may contain this data.
func (kv KVStoreClientImpl) Get(key string, retry bool) ([]byte, error) {
	var err error = fmt.Errorf("DUMMY")
	var val []byte

	for err != nil {
		val, err = kv.getWithoutRetry(key)
		// glog.Infof("Get(key) => %s [Err: %s]", val, err)
		if !retry || !isRetryable(err) {
			return val, err
		}

		// TODO: Use some kind of backoff mechanism, like in
		//       https://github.com/cenkalti/backoff
		time.Sleep(500 * time.Millisecond)
	}

	return val, nil
}

func (kv KVStoreClientImpl) getWithoutRetry(key string) ([]byte, error) {
	v, err := kv.lm.RLock(key, false)

	if err != nil {
		glog.Errorf("Error acquiring RLock in Get(%q): %s", key, err)
		return nil, err
	}

	succVnodes, err := kv.ring.Lookup(kv.ring.GetNumSuccessors(), []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Get(%q): %q", key, err)
		return nil, err
	}

	if glog.V(5) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	if len(succVnodes) == 0 {
		glog.Errorf("No successors found during Lookup in Get(%q)", key)
		return nil, fmt.Errorf("No Successors found")
	}

	for i, vnode := range succVnodes {
		if kv.ring.Transport().IsLocalVnode(vnode) {
			if i < len(succVnodes)-1 {
				succVnodes = append(succVnodes[:i], succVnodes[i+1:]...)
			} else {
				copy(succVnodes, succVnodes[:i])
			}
			value, err := kv.ring.Transport().Get(vnode, key, v)
			// fmt.Printf("GetSubLocal(key, vnode) => %s [Err: %s]\n", value, err)

			// If operation failed, try another node
			if err == nil {
				return value, nil
			}
		}
	}

	// TODO: Performance optimization:
	// Make parallel calls and take the fastest successful response.

	for len(succVnodes) > 0 {
		// Pick a random node in the list of possible replicas
		randval := rand.Intn(len(succVnodes))
		node := succVnodes[randval]
		succVnodes = append(succVnodes[:randval], succVnodes[randval+1:]...)

		// Perform read operation on the random node
		value, err := kv.ring.Transport().Get(node, key, v)
		// fmt.Printf("GetSub(key, vnode) => %s [Err: %s]\n", value, err)

		// If operation failed, try another node
		if err == nil {
			return value, nil
		}
	}

	return nil, fmt.Errorf("All read replicas failed")
}

// Inform the lock manager we're interested in setting the value for key.
// Expected return value:
//    Next available version number to write value to
// Expected error conditions:
//    Network failure     => Retryable failure
//    Key does not exist  => Fail immediately
//    Access permissions? => Fail immediately
//
// Once next version number has been successfully read, contact master
// KV Store to write value at new version.
// Expected error conditions:
//    Key/version too old  => TODO: Inform lock manager
//    Transient error      => TODO: Retry
//
// If write operation succeeded without errors, send a commit message to
// the lock manager to finalize the operation. Until the commit returns
// successfully, the new version of this value will not be advertised.
// Expected error conditions:
//    Lock not found       => TODO: Return
//    Transient error      => TODO: Retry
//
// If write operation failed, send an abort message to the lock manager to
// cancel the operation. This is simply to speed up the lock release operation
// instead of waiting for a timeout to happen.
// Expected error conditions:
//    Lock not found       => TODO: Return
//    Transient error      => TODO: Retry
func (kv *KVStoreClientImpl) Set(key string, value []byte) error {
	var err error = fmt.Errorf("DUMMY")
	var v uint

	for err != nil {
		v, err = kv.lm.WLock(key, 0, 10)
		if err == nil {
			break
		}
		if !isRetryable(err) {
			return err
		}

		// TODO: Use some kind of backoff mechanism, like in
		//       https://github.com/cenkalti/backoff
		time.Sleep(500 * time.Millisecond)
	}

	return kv.SetVersion(key, v, value)
}

// Similar to KVStore.Set, but useful for transactional read-update-write
// operations along with KVStore.GetForSet.
//
// Use the version number from the write lease acquired in KVStore.GetForSet.
// Perform regular Set operation with commit/abort.
func (kv *KVStoreClientImpl) SetVersion(key string, version uint, value []byte) error {
	var err error = fmt.Errorf("DUMMY")

	for err != nil {
		err = kv.setVersionWithoutRetry(key, version, value)

		if err == nil {
			return nil
		}

		if !isRetryable(err) {
			return err
		}

		// TODO: Use some kind of backoff mechanism, like in
		//       https://github.com/cenkalti/backoff
		time.Sleep(500 * time.Millisecond)
	}

	// TODO: Unreachable code
	return nil
}

func (kv *KVStoreClientImpl) setVersionWithoutRetry(key string, version uint, value []byte) error {
	succVnodes, err := kv.ring.Lookup(kv.ring.GetNumSuccessors(), []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Set(%q): %q", key, err)
		return err
	}

	if len(succVnodes) == 0 {
		glog.Errorf("No successors found during Lookup in Get(%q)", key)
		return fmt.Errorf("No Successors found")
	}

	if glog.V(5) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	// This request should always go to the master node.
	// Replication happens at the master.
	err = kv.ring.Transport().Set(succVnodes[0], key, version, value)

	if err != nil {
		glog.Errorf("Aborting Set(%q, %d) due to error: %q", key, version, err)

		// Best-effort Abort
		kv.lm.AbortWLock(key, version)

		// Even if the Abort command failed, we can safely return
		// because the lock manager will timeout and abort for us.
		return err
	}

	err = kv.lm.CommitWLock(key, version)
	return err
}

// Similar to KVStore.Get, but useful for transactional read-update-write
// operations along with KVStore.SetVersion.
//
// First, get a write lease from the lock manager. This prevents any
// further write operations on the same key. Proceed to read the latest
// version of the key and get its data, which is returned.
func (kv KVStoreClientImpl) GetForSet(key string, retry bool) ([]byte, uint, error) {
	var err error = fmt.Errorf("DUMMY")
	var val []byte
	var version uint

	for err != nil {
		version, err = kv.lm.WLock(key, 0, 60)
		if err == nil {
			break
		}

		if !retry || !isRetryable(err) {
			return nil, version, err
		}

		// TODO: Use some kind of backoff mechanism, like in
		//       https://github.com/cenkalti/backoff
		time.Sleep(500 * time.Millisecond)
	}

	err = fmt.Errorf("DUMMY")

	for err != nil {
		val, err = kv.getWithoutRetry(key)
		if err == nil {
			return val, version, nil
		}

		if !retry || !isRetryable(err) {
			return nil, version, err
		}

		// TODO: Use some kind of backoff mechanism, like in
		//       https://github.com/cenkalti/backoff
		time.Sleep(500 * time.Millisecond)
	}

	return nil, 0, fmt.Errorf("Code should note have reached here")
}
