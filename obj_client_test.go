package chord

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

type MockLM struct {
	mock.Mock
}

func (m *MockLM) AbortWLock(key string, version uint) error {
	args := m.Mock.Called(key, version)
	res, _ := args.Get(0).(error)

	return res
}

func (m *MockLM) CommitWLock(key string, version uint) error {
	args := m.Mock.Called(key, version)
	res, _ := args.Get(0).(error)

	return res
}

func (m *MockLM) RLock(key string, forceNoCache bool) (version uint, err error) {
	args := m.Mock.Called(key, forceNoCache)
	res, ok := args.Get(0).(uint)

	if !ok {
		return 0, args.Error(1)
	}
	return res, args.Error(1)
}

func (m *MockLM) WLock(key string, version uint, timeout uint) (uint, error) {
	args := m.Mock.Called(key, version, timeout)
	res, ok := args.Get(0).(uint)

	if !ok {
		return 0, args.Error(1)
	}
	return res, args.Error(1)
}

var _ LMClientIntf = new(MockLM)

func CreateKVClientWithMocks() (*MockRing, *MockLM, KVStoreClient) {
	// Set up mock Ring and mock Lock Manager.
	r := &MockRing{transport: nil, numSuccessors: 2}
	lm := new(MockLM)

	// Create KVStore client.
	kvsClient := NewKVStoreClientWithLM(r, lm)

	return r, lm, kvsClient
}

func TestKVGetNonExistentKey(t *testing.T) {
	r, lm, kvsClient := CreateKVClientWithMocks()

	lm.On("RLock", TEST_KEY, false).Return(0, fmt.Errorf("Key not found")).Once()

	v, err := kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithRingErrors(t *testing.T) {
	r, lm, kvsClient := CreateKVClientWithMocks()

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]Vnode{}, nil)

	v, err := kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}
