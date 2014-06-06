package buddystore

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	MaxReplicationParallelism = 4
)

/* TCP body for KV store requests */
type tcpBodyGet struct {
	Vnode   *Vnode
	Key     string
	Version uint
}

type tcpBodySet struct {
	Vnode   *Vnode
	Key     string
	Version uint
	Value   []byte
}

type tcpBodyList struct {
	Vnode *Vnode
}

/* TCP body for KV store responses */
type tcpBodyRespValue struct {
	Value []byte
	Err   error
}

type tcpBodyRespKeys struct {
	Keys []string
	Err  error
}

// New Vnode operations added for supporting KV store
type KVStoreValue struct {
	ver uint   // version
	val []byte // value
}

type KVStore struct {
	kv     map[string]*list.List
	kvLock sync.Mutex
}

func (vn *localVnode) Get(key string, version uint) ([]byte, error) {
	vn.store.kvLock.Lock()
	defer vn.store.kvLock.Unlock()

	kvLst, found := vn.store.kv[key]

	if !found {
		return nil, fmt.Errorf("Key not found")
	} else {
		for i := kvLst.Front(); i != nil; i = i.Next() {
			// Found the key value matching the requested version
			if i.Value.(*KVStoreValue).ver == version {
				return i.Value.(*KVStoreValue).val, nil
			}
		}

		return nil, fmt.Errorf("Key value with requested version not found")
	}

	return nil, nil
}

func (vn *localVnode) Set(key string, version uint, value []byte) error {
	vn.store.kvLock.Lock()
	defer vn.store.kvLock.Unlock()

	var wg sync.WaitGroup
	var tokens chan bool
	var errs []error

	tokens = make(chan bool, MaxReplicationParallelism)
	errs = make([]error, len(vn.successors))

	for i := 0; i < MaxReplicationParallelism; i++ {
		tokens <- true
	}

	//Function to replicate the KV to the successors
	replicateKV := func(succVnode *Vnode, key string, version uint, value []byte, wg *sync.WaitGroup, tokens chan bool, retErr *error) error {
		defer wg.Done()

		<-tokens

		*retErr = vn.ring.transport.Set(succVnode, key, version, value)

		tokens <- true

		return nil
	}

	kvVal := &KVStoreValue{ver: version, val: value}

	kvLst, found := vn.store.kv[key]

	if !found {
		// This is the first value being added to the list.
		kvLst = list.New()
		vn.store.kv[key] = kvLst

		kvLst.PushFront(kvVal)
	} else {
		curMaxVerVal := kvLst.Front()

		// Add a value only if the version is greater than the
		// current max version
		if curMaxVerVal != nil {
			maxVer := curMaxVerVal.Value.(*KVStoreValue).ver

			if maxVer >= version {
				return fmt.Errorf("Lower version than current max version")
			}
		}

		kvLst.PushFront(kvVal)
	}

	// If we are the owner of the key, replicate the KV to the
	// successors
	if (vn.predecessor == nil) || ((vn.predecessor != nil) && (betweenRightIncl(vn.predecessor.Id, vn.Id, []byte(key)))) {

		for idx, succ := range vn.successors {
			wg.Add(1)

			go replicateKV(succ, key, version, value, &wg, tokens, &errs[idx])
		}

		wg.Wait()

		for idx := range vn.successors {
			if errs[idx] != nil {
				return errs[idx]
			}
		}
	}

	return nil
}

func (vn *localVnode) List() ([]string, error) {
	ret := make([]string, 0, len(vn.store.kv))

	for key := range vn.store.kv {
		ret = append(ret, key)
	}

	return ret, nil
}

func (vn *localVnode) PurgeVersions(key string, maxVersion uint) error {
	vn.store.kvLock.Lock()
	defer vn.store.kvLock.Unlock()

	kvLst, found := vn.store.kv[key]

	if !found {
		return fmt.Errorf("Key not found")
	}

	i := kvLst.Front()

	for i != nil {
           // Remove all values with version less than the max version
		if i.Value.(*KVStoreValue).ver < maxVersion {
			toBeDel := i
			i = i.Next()
			kvLst.Remove(toBeDel)

			continue
		}

		i = i.Next()
	}

	if kvLst.Len() == 0 {
		delete(vn.store.kv, key)
	}

	return nil
}
