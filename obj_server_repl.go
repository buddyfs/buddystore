package buddystore

import (
	"fmt"
	"sync"
)

const (
	MaxIncSyncParallelism = 8
	MaxReplParallelism    = 12
)

func (kvs *KVStore) localRepl() {
	var first_pred *Vnode
	var second_pred *Vnode
	var last_pred *Vnode
	var first_succ *Vnode
	var num_pred int

	kvs.kvLock.Lock()

	for i := 0; i < len(kvs.pred_list); i++ {
		if kvs.pred_list[i] != nil {
			num_pred++
		}
	}

	if num_pred > kvs.vn.Ring().GetNumSuccessors() {
		num_pred = kvs.vn.Ring().GetNumSuccessors()
	}

	first_pred = kvs.pred_list[0]

	if first_pred == nil {
		kvs.kvLock.Unlock()
		return
	}

	second_pred = kvs.pred_list[1]

	if second_pred == nil {
		second_pred = kvs.pred_list[0]
	}

	for i := num_pred - 1; i >= 0; i-- {
		if kvs.pred_list[i] != nil {
			last_pred = kvs.pred_list[i]
			break
		}
	}

	first_succ = kvs.succ_list[0]

	if (first_pred == nil) || (second_pred == nil) || (last_pred == nil) {
		kvs.kvLock.Unlock()
		return
	}

	kvs.kvLock.Unlock()

	var wg sync.WaitGroup
	var tokens chan bool

	tokens = make(chan bool, MaxReplParallelism)

	for i := 0; i < MaxReplParallelism; i++ {
		tokens <- true
	}

	for key := range kvs.kv {

		// Hash the key
		h := kvs.vn.Ring().GetHashFunc()()
		h.Write([]byte(key))
		key_hash := h.Sum(nil)

		if first_succ != nil {
			if betweenRightIncl(last_pred.Id, kvs.vn.localVnodeId(), key_hash) {
				wg.Add(1)
				go kvs.sendSyncKeys(first_succ, key, &wg, tokens)
			}
		}

		if betweenRightIncl(second_pred.Id, first_pred.Id, key_hash) {
			wg.Add(1)
			go kvs.sendSyncKeys(first_pred, key, &wg, tokens)
		}
	}

	wg.Wait()

	return
}

func (kvs *KVStore) globalRepl() {
	var last_pred *Vnode
	var num_pred int

	kvs.kvLock.Lock()

	for i := 0; i < len(kvs.pred_list); i++ {
		if kvs.pred_list[i] != nil {
			num_pred++
		}
	}

	if num_pred <= kvs.vn.Ring().GetNumSuccessors() {
		kvs.kvLock.Unlock()
		return
	}

	last_pred = kvs.pred_list[num_pred-1]

	if last_pred == nil {
		kvs.kvLock.Unlock()
		return
	}

	kvs.kvLock.Unlock()

	var wg sync.WaitGroup
	var tokens chan bool

	tokens = make(chan bool, MaxReplParallelism)

	for i := 0; i < MaxReplParallelism; i++ {
		tokens <- true
	}

	for key := range kvs.kv {
		// Hash the key
		h := kvs.vn.Ring().GetHashFunc()()
		h.Write([]byte(key))
		key_hash := h.Sum(nil)

		if !(betweenRightIncl(last_pred.Id, kvs.vn.localVnodeId(), key_hash)) {

			succ_list, err := kvs.vn.Ring().Lookup(1, []byte(key))

			if err == nil {
				if succ_list[0] != nil {

					wg.Add(1)
					go kvs.sendSyncKeys(succ_list[0], key, &wg, tokens)
				}
			} else {
				fmt.Errorf("Global Repl: Look up failed")
			}
		}
	}

	wg.Wait()

	return
}

func (kvs *KVStore) sendSyncKeys(target *Vnode, key string, wg *sync.WaitGroup, tokens chan bool) {
	defer wg.Done()

	<-tokens

	kvLst, found := kvs.kv[key]

	if !found {
		tokens <- true
		return
	}

	ver := make([]uint, 0, kvLst.Len())

	for i := kvLst.Front(); i != nil; i = i.Next() {
		ver = append(ver, i.Value.(*KVStoreValue).Ver)
	}

	_, ok := kvs.vn.Ring().Transport().(*LocalTransport).get(target)

	if !ok {
		kvs.vn.Ring().Transport().(*LocalTransport).remote.SyncKeys(target, kvs.vn.GetVnode(), key, ver)
	}

	tokens <- true
	return
}

func (kvs *KVStore) incSync(key string, version uint, value []byte) error {
	var wg sync.WaitGroup
	var tokens chan bool
	var errs []error

	tokens = make(chan bool, MaxIncSyncParallelism)
	errs = make([]error, len(kvs.vn.Successors()))

	for i := 0; i < MaxIncSyncParallelism; i++ {
		tokens <- true
	}

	// Hash the key
	h := kvs.vn.Ring().GetHashFunc()()
	h.Write([]byte(key))
	key_hash := h.Sum(nil)

	// If we are the owner of the key, replicate the KV to the
	// successors
	if (kvs.vn.Predecessor() == nil) || ((kvs.vn.Predecessor() != nil) && (betweenRightIncl(kvs.vn.Predecessor().Id, kvs.vn.localVnodeId(), key_hash))) {

		for idx, succVn := range kvs.vn.Successors() {
			if succVn != nil {
				wg.Add(1)

				go kvs.incSyncToSucc(succVn, key, version, value, &wg, tokens, &errs[idx])
			}
		}

		wg.Wait()

		for idx := range kvs.vn.Successors() {
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

	ok := kvs.vn.Ring().Transport().IsLocalVnode(succVn)

	if !ok {
		kvs.vn.Ring().Transport().Set(succVn, key, version, value)
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
		_, ok := kvs.vn.Ring().Transport().(*LocalTransport).get(ownerVn)

		if !ok {
			kvs.vn.Ring().Transport().(*LocalTransport).remote.MissingKeys(ownerVn, kvs.vn.GetVnode(), key, ver)
		}

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

	if len(retVer) == 0 {
		return
	}

	_, ok := kvs.vn.Ring().Transport().(*LocalTransport).get(ownerVn)

	if !ok {
		kvs.vn.Ring().Transport().(*LocalTransport).remote.MissingKeys(ownerVn, kvs.vn.GetVnode(), key, retVer)
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

	_, ok := kvs.vn.Ring().Transport().(*LocalTransport).get(replVn)

	if !ok {
		kvs.vn.Ring().Transport().(*LocalTransport).remote.BulkSet(replVn, key, valueLst)
	}

	return
}
