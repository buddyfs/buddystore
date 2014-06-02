package chord

import (
	"sync"

	"github.com/golang/glog"
)

type KVStoreClient interface {
	Get(key string) ([]byte, error)
	Set(key string, val []byte) error
}

type KVStoreClientImpl struct {
	ring *Ring
	lm   LMClientIntf

	// Implements
	KVStoreClient
}

var _ KVStoreClient = &KVStoreClientImpl{}

func NewKVStoreClient(ring *Ring) *KVStoreClientImpl {
	lm := &LManagerClient{Ring: ring, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	return &KVStoreClientImpl{ring: ring, lm: lm}
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
	v, err := kv.lm.WLock(key, 1, 10)

	// TODO: Inspect error and determine if we can retry the operation.
	if err != nil {
		glog.Errorf("Error acquiring WLock in Set(%q): %d, %q", key, v, err)
		return err
	}

	succVnodes, err := kv.ring.Lookup(kv.ring.config.NumSuccessors, []byte(key))
	if err != nil {
		glog.Errorf("Error listing successors in Set(%q): %q", key, err)
		return err
	}

	if glog.V(2) {
		glog.Infof("Successfully looked up list of successors: %q", succVnodes)
	}

	var maxParallelism int = 4
	var errs []error = make([]error, len(succVnodes))
	var tokens chan bool = make(chan bool, maxParallelism)
	var wg sync.WaitGroup

	for i := 0; i < maxParallelism; i++ {
		tokens <- true
	}

	writeToReplica := func(node *Vnode, err *error, wg *sync.WaitGroup, tokens chan bool) {
		// Take a token
		<-tokens

		// Perform the operation
		*err = kv.ring.transport.Set(node, key, v, value)

		// Return the token
		tokens <- true

		// Signal completion
		wg.Done()
	}

	for i := range succVnodes {
		wg.Add(1)
		go writeToReplica(succVnodes[i], &errs[i], &wg, tokens)
	}

	wg.Wait()

	// Potential optimizations:
	// - Fail fast => in case one of the replicas failed to write,
	//                abort other threads and abort commit.

	for i := range succVnodes {
		if errs[i] != nil {
			glog.Errorf("Aborting Set(%q, %d) due to error: %q", key, v, errs[i])

			// Best-effort Abort
			kv.lm.AbortWLock(key, v)

			// Even if the Abort command failed, we can safely return
			// because the lock manager will timeout and abort for us.
			return errs[i]
		}
	}

	err = kv.lm.CommitWLock(key, v)
	return err
}

func (kv *KVStoreClientImpl) List() ([]string, error) {
	// Will be used only by the replicators
	return nil, nil
}
