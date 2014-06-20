package buddystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithRingErrors(t *testing.T) {
	_, r, lm, kvsClient := CreateKVClientWithMocks()

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return(nil, fmt.Errorf("Lookup failed")).Once()

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]Vnode{}, nil).Once()

	v, err = kvsClient.Get(TEST_KEY, false)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithAllNodeReadsFailing(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("IsLocalVnode", mock.Anything).Return(false)
	tr.On("Get", vnode1, TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error")).Once()
	tr.On("Get", vnode2, TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error")).Once()

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Nil(t, v)
	assert.Error(t, err, "Could not read data")

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithSomeNodeReadsFailing(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	tr.On("IsLocalVnode", mock.Anything).Return(false)
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()

	// TODO: Ideally this would check that the vnode being called
	// is either vnode1 or vnode2.
	tr.On("Get", mock.AnythingOfType("*buddystore.Vnode"), TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error")).Once()
	tr.On("Get", mock.AnythingOfType("*buddystore.Vnode"), TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithLocalNodeProritized(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("IsLocalVnode", vnode1).Return(false)
	tr.On("IsLocalVnode", vnode2).Return(true)
	tr.On("Get", vnode2, TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithFallbackToRemoteNode(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("IsLocalVnode", vnode1).Return(false)
	tr.On("IsLocalVnode", vnode2).Return(true)
	tr.On("Get", vnode1, TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()
	tr.On("Get", vnode2, TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error"))

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetWithRetryableErrors(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(0, TransientError("Temporary Lock Manager error")).Once()
	lm.On("RLock", TEST_KEY, false).Return(1, nil)

	r.On("Lookup", 2, []byte(TEST_KEY)).Return(nil, TransientError("Temporary lookup error")).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()

	tr.On("IsLocalVnode", vnode1).Return(false)
	tr.On("IsLocalVnode", vnode2).Return(true)

	tr.On("Get", vnode1, TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()
	tr.On("Get", vnode2, TEST_KEY, uint(1)).Return(nil, fmt.Errorf("Node read error"))

	v, err := kvsClient.Get(TEST_KEY, true)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVGetExistingKeyWithoutErrors(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("RLock", TEST_KEY, false).Return(1, nil).Once()
	tr.On("IsLocalVnode", mock.Anything).Return(false)
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()

	// TODO: Ideally this would check that the vnode being called
	// is either vnode1 or vnode2.
	tr.On("Get", mock.AnythingOfType("*buddystore.Vnode"), TEST_KEY, uint(1)).Return([]byte(TEST_VALUE), nil).Once()

	v, err := kvsClient.Get(TEST_KEY, false)
	assert.Equal(t, TEST_VALUE, v)
	assert.Nil(t, err)

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVSetWriteLockedKey(t *testing.T) {
	_, r, lm, kvsClient := CreateKVClientWithMocks()

	bar := []byte("bar")

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, fmt.Errorf("Key write locked")).Once()

	err := kvsClient.Set(TEST_KEY, bar)
	assert.Error(t, err, "Write-locked key")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVSetWithRingErrors(t *testing.T) {
	_, r, lm, kvsClient := CreateKVClientWithMocks()

	bar := []byte("bar")

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return(nil, fmt.Errorf("Lookup failed")).Once()

	err := kvsClient.Set(TEST_KEY, bar)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]Vnode{}, nil).Once()

	err = kvsClient.Set(TEST_KEY, bar)
	assert.Error(t, err, "Could not read data")

	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVSetAndAbort(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	bar := []byte("bar")

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("Set", vnode1, TEST_KEY, uint(1), bar).Return(fmt.Errorf("Node read error")).Once()

	// Abort command is strictly not necessary here.
	// In case the test breaks, check if the Abort command is being
	// sent in an async goroutine, because thaat is a possible optimization.
	lm.On("AbortWLock", TEST_KEY, uint(1)).Return(1, nil).Once()

	err := kvsClient.Set(TEST_KEY, bar)
	assert.Error(t, err, "Could not read data")

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVSetAndCommitFailed(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	bar := []byte("bar")

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("Set", vnode1, TEST_KEY, uint(1), bar).Return(nil).Once()
	lm.On("CommitWLock", TEST_KEY, uint(1)).Return(fmt.Errorf("Commit failed")).Once()

	err := kvsClient.Set(TEST_KEY, bar)
	assert.Error(t, err, "Could not read data")

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}

func TestKVSetAndCommitSucceeded(t *testing.T) {
	tr, r, lm, kvsClient := CreateKVClientWithMocks()

	bar := []byte("bar")

	vnode1 := &Vnode{Id: []byte("abcdef"), Host: "vnode1"}
	vnode2 := &Vnode{Id: []byte("123456"), Host: "vnode2"}

	lm.On("WLock", TEST_KEY, uint(0), uint(10)).Return(1, nil).Once()
	r.On("Lookup", 2, []byte(TEST_KEY)).Return([]*Vnode{vnode1, vnode2}, nil).Once()
	tr.On("Set", vnode1, TEST_KEY, uint(1), bar).Return(nil).Once()
	lm.On("CommitWLock", TEST_KEY, uint(1)).Return(nil).Once()

	err := kvsClient.Set(TEST_KEY, bar)
	assert.NoError(t, err, "Commit succeeded")

	tr.AssertExpectations(t)
	r.AssertExpectations(t)
	lm.AssertExpectations(t)
}
