package buddystore

import (
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
