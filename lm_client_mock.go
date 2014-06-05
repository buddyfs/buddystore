package chord

import "github.com/stretchr/testify/mock"

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
	return uint(args.Int(0)), args.Error(1)
}

func (m *MockLM) WLock(key string, version uint, timeout uint) (uint, error) {
	args := m.Mock.Called(key, version, timeout)
	return uint(args.Int(0)), args.Error(1)
}

var _ LMClientIntf = new(MockLM)
