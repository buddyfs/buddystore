package chord

import (
	"container/list"
	"fmt"
	"sync"
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

	kvVal := &KVStoreValue{ver: version, val: value}

	kvLst, found := vn.store.kv[key]

	if !found {
		// This is the first value being added to the list.
		kvLst = list.New()
		vn.store.kv[key] = kvLst

		kvLst.PushFront(kvVal)

		return nil
	}

	curMaxVerVal := kvLst.Front()

	if curMaxVerVal != nil {
		maxVer := curMaxVerVal.Value.(*KVStoreValue).ver

		if maxVer >= version {
			return fmt.Errorf("Lower version than current max version")
		}

		kvLst.PushFront(kvVal)
	} else {
		kvLst.PushFront(kvVal)
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
