package buddystore

import (
	"sync"
)

const (
	MaxIncSyncParallelism = 8
)

func (kvs *KVStore) localRepl() {
	return
}

func (kvs *KVStore) globalRepl() {
	return
}

func (kvs *KVStore) incSync(key string, version uint, value []byte) error {
	var wg sync.WaitGroup
	var tokens chan bool
	var errs []error

	tokens = make(chan bool, MaxIncSyncParallelism)
	errs = make([]error, len(kvs.vn.successors))

	for i := 0; i < MaxIncSyncParallelism; i++ {
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

func (kvs *KVStore) syncKeys(ownerVn *Vnode, key string, ver []uint) error {

	go kvs.handleSyncKeys(ownerVn, key, ver)

	return nil
}

func (kvs *KVStore) handleSyncKeys(ownerVn *Vnode, key string, ver []uint) {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	var present bool
	var retVer []uint

	if ownerVn == nil {
		return
	}

	kvLst, found := kvs.kv[key]

	if !found {
		return
	}

	retVer = make([]uint, 0, len(ver))

	for _, version := range ver {
		present = false

		for i := kvLst.Front(); i != nil; i = i.Next() {
			if i.Value.(*KVStoreValue).Ver == version {
				present = true
				break
			}
		}

		if present == false {
			retVer = append(retVer, version)
		}
	}

	_, ok := kvs.vn.ring.transport.(*LocalTransport).get(ownerVn)

	if !ok {
		kvs.vn.ring.transport.(*LocalTransport).remote.MissingKeys(ownerVn, &kvs.vn.Vnode, key, retVer)
	}

	return
}

func (kvs *KVStore) missingKeys(replVn *Vnode, key string, ver []uint) error {

	go kvs.handleMissingKeys(replVn, key, ver)

	return nil
}

func (kvs *KVStore) handleMissingKeys(replVn *Vnode, key string, ver []uint) {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	var valueLst []KVStoreValue

	if replVn == nil {
		return
	}

	kvLst, found := kvs.kv[key]

	if !found {
		return
	}

	valueLst = make([]KVStoreValue, 0, len(ver))

	for _, version := range ver {

		for i := kvLst.Front(); i != nil; i = i.Next() {
			if i.Value.(*KVStoreValue).Ver == version {
				kvVal := KVStoreValue{Ver: version, Val: i.Value.(*KVStoreValue).Val}
				valueLst = append(valueLst, kvVal)
				break
			}
		}
	}

	_, ok := kvs.vn.ring.transport.(*LocalTransport).get(replVn)

	if !ok {
		kvs.vn.ring.transport.(*LocalTransport).remote.BulkSet(replVn, key, valueLst)
	}

	return
}
