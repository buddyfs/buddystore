package buddystore

import (
	"hash"

	"github.com/stretchr/testify/mock"
)

type MockRing struct {
	mock.Mock
	transport     Transport
	numSuccessors int
	hashfunc      func() hash.Hash
}

func (m MockRing) GetNumSuccessors() int {
	return m.numSuccessors
}

func (m *MockRing) Leave() error {
	args := m.Mock.Called()
	res, _ := args.Get(0).(error)

	return res
}

func (m *MockRing) Lookup(n int, key []byte) ([]*Vnode, error) {
	args := m.Mock.Called(n, key)
	res, ok := args.Get(0).([]*Vnode)

	if !ok {
		return nil, args.Error(1)
	}
	return res, args.Error(1)
}

func (m *MockRing) Shutdown() {
	m.Mock.Called()
}

func (m MockRing) Transport() Transport {
	return m.transport
}

func (m MockRing) GetLocalVnode() *Vnode {
	panic("TODO: MockRing.GetLocalVnode")
}

func (m MockRing) GetRingId() string {
	panic("TODO: MockRing.GetRingId")
}

func (m MockRing) GetHashFunc() func() hash.Hash {
	return m.hashfunc
}

var _ RingIntf = new(MockRing)
