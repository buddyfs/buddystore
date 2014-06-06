package buddystore

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	MaxReplicationParallelism = 8
)

// New Vnode operations added for supporting KV store
type KVStoreValue struct {
	ver uint   // version
	val []byte // value
}

type KVStore struct {
	vn     *localVnode
	kv     map[string]*list.List
	kvLock sync.Mutex

	// Implements:
	KVStoreIntf
}

type KVStoreIntf interface {
	get(string, uint) ([]byte, error)
	set(string, uint, []byte) error
	list() ([]byte, error)
	purgeVersions(string, uint) error
	incSync(string, uint, []byte) error
	incSyncToSucc(*Vnode, string, uint, []byte, *sync.WaitGroup, chan bool, *error)
}

func (kvs *KVStore) get(key string, version uint) ([]byte, error) {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	kvLst, found := kvs.kv[key]

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

func (kvs *KVStore) set(key string, version uint, value []byte) error {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	kvVal := &KVStoreValue{ver: version, val: value}

	kvLst, found := kvs.kv[key]

	if !found {
		// This is the first value being added to the list.
		kvLst = list.New()
		kvs.kv[key] = kvLst

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

	kvs.incSync(key, version, value)

	return nil
}

func (kvs *KVStore) list() ([]string, error) {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	ret := make([]string, 0, len(kvs.kv))

	for key := range kvs.kv {
		ret = append(ret, key)
	}

	return ret, nil
}

func (kvs *KVStore) purgeVersions(key string, maxVersion uint) error {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	kvLst, found := kvs.kv[key]

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
		delete(kvs.kv, key)
	}

	return nil
}

func (kvs *KVStore) incSync(key string, version uint, value []byte) error {
	var wg sync.WaitGroup
	var tokens chan bool
	var errs []error

	tokens = make(chan bool, MaxReplicationParallelism)
	errs = make([]error, len(kvs.vn.successors))

	for i := 0; i < MaxReplicationParallelism; i++ {
		tokens <- true
	}

	// If we are the owner of the key, replicate the KV to the
	// successors
	if (kvs.vn.predecessor == nil) || ((kvs.vn.predecessor != nil) && (betweenRightIncl(kvs.vn.predecessor.Id, kvs.vn.Id, []byte(key)))) {

		for idx, succVn := range kvs.vn.successors {
			if succVn != nil {
				wg.Add(1)

				go kvs.incSyncToSucc(succVn, key, version, value, &wg, tokens, &errs[idx])
			}
		}

		wg.Wait()

		for idx := range kvs.vn.successors {
			if errs[idx] != nil {
				return errs[idx]
			}
		}
	}

	return nil
}

func (kvs *KVStore) incSyncToSucc(succVn *Vnode, key string, version uint, value []byte, wg *sync.WaitGroup, tokens chan bool, retErr *error) {
	defer wg.Done()

	<-tokens

	_, ok := kvs.vn.ring.transport.(*LocalTransport).get(succVn)

	if !ok {
		kvs.vn.ring.transport.(*LocalTransport).remote.Set(succVn, key, version, value)
	}

	tokens <- true
}
