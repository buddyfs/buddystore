package chord

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var TEST_VALUE = []byte("FOOBAR")

func CreateKVClientWithMocks() (*MockTransport, *MockRing, *MockLM, KVStoreClient) {
	// Set up mock Ring and mock Lock Manager.
	t := &MockTransport{}
	r := &MockRing{transport: t, numSuccessors: 2}
	lm := new(MockLM)

	// Create KVStore client.
	kvsClient := NewKVStoreClientWithLM(r, lm)

	return t, r, lm, kvsClient
}

func TestKVGetNonExistentKey(t *testing.T) {
	_, r, lm, kvsClient := CreateKVClientWithMocks()

	lm.On("RLock", TEST_KEY, false).Return(0, fmt.Errorf("Key not found")).Once()

	v, err := kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithRingErrors(t *testing.T) {
	_, r, lm, kvsClient := CreateKVClientWithMocks()

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return(nil, fmt.Errorf("Lookup failed")).Once()

	v, err := kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]Vnode{}, nil).Once()

	v, err = kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithNodeErrors(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("Get", vnode1, TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error")).Once()

	v, err := kvsClient.Get(TEST_KEY)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithoutErrors(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("Get", vnode1, TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()

	v, err := kvsClient.Get(TEST_KEY)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}
