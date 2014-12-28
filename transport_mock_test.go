package buddystore

import (
	"github.com/stretchr/testify/mock"
	"sync"
)

type MockTransport struct {
	mock.Mock
	mockLock sync.Mutex // Ideally Mock by itself should be threadsafe - workaround.
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

func (mt *MockTransport) GetPredecessorList(*Vnode) ([]*Vnode, error) {
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

func (mt *MockTransport) RLock(*Vnode, string, string, *OpsLogEntry) (string, uint, uint64, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) WLock(*Vnode, string, uint, uint, string, *OpsLogEntry) (string, uint, uint, uint64, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) CommitWLock(*Vnode, string, uint, string, *OpsLogEntry) (uint64, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) AbortWLock(*Vnode, string, uint, string, *OpsLogEntry) (uint64, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) InvalidateRLock(*Vnode, string) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	mt.mockLock.Lock()
	defer mt.mockLock.Unlock()
	args := mt.Mock.Called(target, key, version)
	res, ok := args.Get(0).([]byte)

	if !ok {
		return nil, args.Error(1)
	}
	return res, args.Error(1)
}

func (mt *MockTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	mt.mockLock.Lock()
	defer mt.mockLock.Unlock()
	args := mt.Mock.Called(target, key, version, value)
	return args.Error(0)
}

func (mt *MockTransport) List(target *Vnode) ([]string, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) BulkSet(target *Vnode, key string, valLst []KVStoreValue) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) SyncKeys(target *Vnode, ownerVn *Vnode, key string, ver []uint) error {
	mt.mockLock.Lock()
	defer mt.mockLock.Unlock()
	args := mt.Mock.Called(target, ownerVn, key, ver)
	return args.Error(0)
}

func (mt *MockTransport) MissingKeys(target *Vnode, replVn *Vnode, key string, ver []uint) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) PurgeVersions(target *Vnode, key string, maxVersion uint) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) JoinRing(target *Vnode, ringId string, self *Vnode) ([]*Vnode, error) {
	panic("Mock method not implemented")
}

func (mt *MockTransport) LeaveRing(target *Vnode, ringId string) error {
	panic("Mock method not implemented")
}

func (mt *MockTransport) IsLocalVnode(vn *Vnode) bool {
	mt.mockLock.Lock()
	defer mt.mockLock.Unlock()
	args := mt.Mock.Called(vn)
	return args.Bool(0)
}

var _ Transport = new(MockTransport)
