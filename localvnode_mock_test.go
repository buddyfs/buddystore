package buddystore

import (
	"github.com/stretchr/testify/mock"
	"sync"
)

type MockLocalVnode struct {
	R RingIntf
	mock.Mock
	mockLock sync.Mutex
}

func (mlv *MockLocalVnode) Ring() RingIntf {
	return mlv.R
}

func (mlv *MockLocalVnode) Successors() []*Vnode {
	mlv.mockLock.Lock()
	defer mlv.mockLock.Unlock()
	args := mlv.Mock.Called()
	res, _ := args.Get(0).([]*Vnode)

	return res
}

func (mlv *MockLocalVnode) Predecessors() []*Vnode {
	panic("")
}

func (mlv *MockLocalVnode) Predecessor() *Vnode {
	mlv.mockLock.Lock()
	defer mlv.mockLock.Unlock()
	args := mlv.Mock.Called()
	res, _ := args.Get(0).(*Vnode)

	return res
}

func (mlv *MockLocalVnode) localVnodeId() []byte {
	mlv.mockLock.Lock()
	defer mlv.mockLock.Unlock()
	args := mlv.Mock.Called()
	res, _ := args.Get(0).([]byte)

	return res
}

func (mlv *MockLocalVnode) GetVnode() *Vnode {
	mlv.mockLock.Lock()
	defer mlv.mockLock.Unlock()
	args := mlv.Mock.Called()
	res, _ := args.Get(0).(*Vnode)

	return res
}

var _ localVnodeIface = new(MockLocalVnode)
