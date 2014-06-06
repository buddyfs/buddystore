package buddystore

import "github.com/stretchr/testify/mock"

type MockRing struct {
	mock.Mock
	transport     Transport
	numSuccessors int
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

var _ RingIntf = new(MockRing)
