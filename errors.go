package buddystore

import (
	"fmt"
	"strings"
)

type BuddyStoreError struct {
	Err       error
	Transient bool
}

func PermanentError(str string, args ...interface{}) BuddyStoreError {
	return BuddyStoreError{Err: fmt.Errorf(str, args), Transient: false}
}

func TransientError(str string, args ...interface{}) BuddyStoreError {
	return BuddyStoreError{Err: fmt.Errorf(str, args), Transient: true}
}

func (bse BuddyStoreError) Error() string {
	return bse.Err.Error()
}

func (bse BuddyStoreError) Temporary() bool {
	return bse.Transient
}

func (bse BuddyStoreError) Timeout() bool {
	return false
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	if nerr, ok := err.(BuddyStoreError); ok && nerr.Temporary() {
		return true
	}

	// To support pure string errors
	return strings.Contains(err.Error(), "[Retryable]")
}
