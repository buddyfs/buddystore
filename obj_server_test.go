package buddystore

import (
	"container/list"
	"crypto/sha1"
	"testing"

	"github.com/stretchr/testify/mock"
)

func TestIncSync(t *testing.T) {
	tr := &MockTransport{}
	r := &MockRing{transport: tr, numSuccessors: 2, hashfunc: sha1.New}
	vn := &MockLocalVnode{R: r}

	vnode1 := &Vnode{Host: "localnode1:1234", Id: []byte("vnode1")}
	vnode2 := &Vnode{Host: "localnode2:3456", Id: []byte("vnode2")}
	successors := []*Vnode{vnode1, vnode2}

	predNode := &Vnode{Host: "prednode:9876", Id: []byte("Pred")}
	localVnodeId := []byte("Local")

	bar := []byte("bar")
	foo := "foo"
	kvs := &KVStore{vn: vn}
	kvs.init()

	vn.On("Successors").Return(successors)
	vn.On("Predecessor").Return(predNode)
	vn.On("localVnodeId").Return(localVnodeId)
	tr.On("IsLocalVnode", mock.Anything).Return(false)
	tr.On("Set", vnode1, foo, uint(2), bar).Return(nil).Once()
	tr.On("Set", vnode2, foo, uint(2), bar).Return(nil).Once()

	kvs.set(foo, 2, bar)

	tr.AssertExpectations(t)
	vn.AssertExpectations(t)
}

func TestGlobalReplication(t *testing.T) {
	tr := &MockTransport{}
	r := &MockRing{transport: tr, numSuccessors: 2, hashfunc: sha1.New}
	vn := &MockLocalVnode{R: r}

	abc := &Vnode{Host: "localnode1:1234", Id: []byte("abc")}
	def := &Vnode{Host: "localnode2:3456", Id: []byte("def")}
	ghi := &Vnode{Host: "localnode3:4567", Id: []byte("ghi")}
	jkl := &Vnode{Host: "localnode4:6462", Id: []byte("jkl")} // local
	mno := &Vnode{Host: "localnode5:3534", Id: []byte("mno")}
	pqr := &Vnode{Host: "localnode6:6757", Id: []byte("pqr")}

	value := []byte("bar")

	// Keys belonging to predecessors
	key1 := "foo"
	key2 := "baz"

	// Keys belonging to successors
	key3 := "moo"
	key4 := "paz"

	kvs := &KVStore{vn: vn}
	kvs.init()

	// Add keys to get syncd
	kvs.kv[key1] = list.New()
	kvs.kv[key1].PushFront(&KVStoreValue{Ver: 2, Val: value})
	kvs.kv[key1].PushFront(&KVStoreValue{Ver: 1, Val: value})

	kvs.kv[key2] = list.New()
	kvs.kv[key2].PushFront(&KVStoreValue{Ver: 4, Val: value})
	kvs.kv[key2].PushFront(&KVStoreValue{Ver: 3, Val: value})

	kvs.kv[key3] = list.New()
	kvs.kv[key3].PushFront(&KVStoreValue{Ver: 6, Val: value})
	kvs.kv[key3].PushFront(&KVStoreValue{Ver: 5, Val: value})

	kvs.kv[key4] = list.New()
	kvs.kv[key4].PushFront(&KVStoreValue{Ver: 8, Val: value})
	kvs.kv[key4].PushFront(&KVStoreValue{Ver: 7, Val: value})

	kvs.updatePredSuccList([]*Vnode{abc, def, ghi}, []*Vnode{mno, pqr})

	vn.On("localVnodeId").Return(jkl.Id)
	r.On("Lookup", 1, []byte(key1)).Return([]*Vnode{def}, nil)
	r.On("Lookup", 1, []byte(key2)).Return([]*Vnode{abc}, nil)
	r.On("Lookup", 1, []byte(key3)).Return([]*Vnode{mno}, nil)
	r.On("Lookup", 1, []byte(key4)).Return([]*Vnode{pqr}, nil)

	tr.On("IsLocalVnode", mock.Anything).Return(false)
	vn.On("GetVnode").Return(jkl)

	tr.On("SyncKeys", def, jkl, key1, []uint{1, 2}).Return(nil)
	tr.On("SyncKeys", abc, jkl, key2, []uint{3, 4}).Return(nil)
	tr.On("SyncKeys", mno, jkl, key3, []uint{5, 6}).Return(nil)
	tr.On("SyncKeys", pqr, jkl, key4, []uint{7, 8}).Return(nil)

	kvs.globalRepl()

	tr.AssertExpectations(t)
	vn.AssertExpectations(t)
}
