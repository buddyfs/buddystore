package buddystore

import (
	"sync"
)

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

func (kvs *KVStore) syncKeys(key string, ver []uint) (string, []uint, error) {
	kvs.kvLock.Lock()
	defer kvs.kvLock.Unlock()

	var present bool
	var retVer []uint

	kvLst, found := kvs.kv[key]

	if !found {
		return key, ver, nil
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

	return key, retVer, nil
}
