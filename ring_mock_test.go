package buddystore

import (
	"hash"
	"github.com/stretchr/testify/mock"
	"sync"
)

type MockRing struct {
	mock.Mock
	transport     Transport
	numSuccessors int
	hashfunc      func() hash.Hash
	mockLock	  sync.Mutex
}

func (m MockRing) GetNumSuccessors() int {
	return m.numSuccessors
}

func (m *MockRing) Leave() error {
	m.mockLock.Lock()
	defer m.mockLock.Unlock()
	args := m.Mock.Called()
	res, _ := args.Get(0).(error)

	return res
}

func (m *MockRing) Lookup(n int, key []byte) ([]*Vnode, error) {
	m.mockLock.Lock()
	defer m.mockLock.Unlock()
	args := m.Mock.Called(n, key)
	res, ok := args.Get(0).([]*Vnode)

	if !ok {
		return nil, args.Error(1)
	}
	return res, args.Error(1)
}

func (m *MockRing) Shutdown() {
	m.mockLock.Lock()
	defer m.mockLock.Unlock()
	m.Mock.Called()
}

func (m *MockRing) Transport() Transport {
	m.mockLock.Lock()
	defer m.mockLock.Unlock()
	return m.transport
}

func (m *MockRing) GetLocalVnode() *Vnode {
	panic("TODO: MockRing.GetLocalVnode")
}

func (m *MockRing) GetLocalLocalVnode() *localVnode {
	panic("TODO: MockRing.GetLocalVnode")
}

func (m *MockRing) GetRingId() string {
	panic("TODO: MockRing.GetRingId")
}

func (m *MockRing) GetHashFunc() func() hash.Hash {
	return m.hashfunc
}

var _ RingIntf = new(MockRing)
