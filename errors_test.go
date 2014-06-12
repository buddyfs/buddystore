package buddystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorsBuddyStoreErrorRetryable(t *testing.T) {
	assert.True(t, isRetryable(TransientError("foo")))
}

func TestErrorsBuddyStoreErrorNotRetryable(t *testing.T) {
	assert.False(t, isRetryable(PermanentError("foo")))
}

func TestErrorsStringErrorRetryable(t *testing.T) {
	assert.True(t, isRetryable(fmt.Errorf("[Retryable] foo")))
}

func TestErrorsStringErrorNotRetryable(t *testing.T) {
	assert.False(t, isRetryable(fmt.Errorf("foo")))
}

func TestErrorsNil(t *testing.T) {
	assert.False(t, isRetryable(nil))
}
