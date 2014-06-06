package buddystore

import "github.com/stretchr/testify/mock"

type MockTransport struct {
	mock.Mock
}

func (mt *MockTransport) ListVnodes(string) ([]*Vnode, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) Ping(*Vnode) (bool, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) GetPredecessor(*Vnode) (*Vnode, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) ClearPredecessor(target, self *Vnode) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) SkipSuccessor(target, self *Vnode) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) Register(*Vnode, VnodeRPC) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) RLock(*Vnode, string, string) (string, uint, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) WLock(*Vnode, string, uint, uint, string) (string, uint, uint, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) CommitWLock(*Vnode, string, uint, string) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) AbortWLock(*Vnode, string, uint, string) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	args := mt.Mock.Called(target, key, version)
	res, ok := args.Get(0).([]byte)

	if !ok {
		return nil, args.Error(1)
	}
	return res, args.Error(1)
}

func (mt *MockTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) List(target *Vnode) ([]string, error) {
	panic("Mock method not implemented")
}

var _ Transport = new(MockTransport)
