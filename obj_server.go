package buddystore

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	MaxReplicationParallelism = 8
)

type KVStoreValue struct {
	Ver uint   // version
	Val []byte // value
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
	bulkSet(string, []KVStoreValue) error
	syncKeys(*Vnode, string, []uint) error
	handleSyncKeys(*Vnode, string, []uint) error
	missingKeys(*Vnode, string, []uint) error
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
			if i.Value.(*KVStoreValue).Ver == version {
				return i.Value.(*KVStoreValue).Val, nil
			}
		}

		return nil, fmt.Errorf("Key value with requested version not found")
	}

	return nil, nil
}

func (kvs *KVStore) set(key string, version uint, value []byte) error {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	kvVal := &KVStoreValue{Ver: version, Val: value}

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
			maxVer := curMaxVerVal.Value.(*KVStoreValue).Ver

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

func (kvs *KVStore) bulkSet(key string, valLst []KVStoreValue) error {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	if len(valLst) == 0 {
		return fmt.Errorf("Empty list of values")
	}

	kvLst, found := kvs.kv[key]

	for _, val := range valLst {
		kvVal := &KVStoreValue{Ver: val.Ver, Val: val.Val}

		if !found {
			// This is the first value being added to the list.
			kvLst = list.New()
			kvs.kv[key] = kvLst

			kvLst.PushFront(kvVal)

			found = true
		} else {
			curMaxVerVal := kvLst.Front()

			// Add a value only if the version is greater than the
			// current max version
			if curMaxVerVal != nil {
				maxVer := curMaxVerVal.Value.(*KVStoreValue).Ver

				if maxVer >= val.Ver {
					kvLst.PushBack(kvVal)
				} else {
					kvLst.PushFront(kvVal)
				}
			} else {
				kvLst.PushFront(kvVal)
			}
		}
	}

	return nil
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
		if i.Value.(*KVStoreValue).Ver < maxVersion {
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
