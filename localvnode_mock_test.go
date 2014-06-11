package buddystore

import "github.com/stretchr/testify/mock"

type MockLocalVnode struct {
	R RingIntf
	mock.Mock
}

func (mlv *MockLocalVnode) Ring() RingIntf {
	return mlv.R
}

func (mlv *MockLocalVnode) Successors() []*Vnode {
	args := mlv.Mock.Called()
	res, _ := args.Get(0).([]*Vnode)

	return res
}

func (mlv *MockLocalVnode) Predecessors() []*Vnode {
	panic("")
}

func (mlv *MockLocalVnode) Predecessor() *Vnode {
	args := mlv.Mock.Called()
	res, _ := args.Get(0).(*Vnode)

	return res
}

func (mlv *MockLocalVnode) localVnodeId() []byte {
	args := mlv.Mock.Called()
	res, _ := args.Get(0).([]byte)

	return res
}

func (mlv *MockLocalVnode) GetVnode() *Vnode {
	panic("")
}

var _ localVnodeIface = new(MockLocalVnode)
