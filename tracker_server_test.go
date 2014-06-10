package buddystore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTrackerHandleJoin(t *testing.T) {
	ringId := "ring1"
	tr := NewTracker()
	vnode1 := &Vnode{Host: "localnode1:1234", Id: []byte("vnode1")}
	vnode2 := &Vnode{Host: "localnode2:3456", Id: []byte("vnode2")}
	vnode3 := &Vnode{Host: "localnode3:9876", Id: []byte("vnode3")}

	existing, err := tr.handleJoinRing(ringId, vnode1)
	assert.NoError(t, err)
	assert.Empty(t, existing)

	existing, err = tr.handleJoinRing(ringId, vnode2)
	assert.NoError(t, err)
	assert.NotNil(t, existing)
	assert.Equal(t, len(existing), 1)
	assert.Equal(t, existing[0], vnode1)

	existing, err = tr.handleJoinRing(ringId, vnode3)
	assert.NoError(t, err)
	assert.NotNil(t, existing)
	assert.Equal(t, len(existing), 2)

	// TODO: Shouldn't be testing order of vnodes here.
	assert.Equal(t, existing[0], vnode1)
	assert.Equal(t, existing[1], vnode2)
}

func TestTrackerHandleReJoin(t *testing.T) {
	ringId := "ring1"
	tr := NewTracker()
	vnode1 := &Vnode{Host: "localnode1:1234", Id: []byte("vnode1")}

	existing, err := tr.handleJoinRing(ringId, vnode1)
	assert.NoError(t, err)
	assert.Empty(t, existing)

	existing, err = tr.handleJoinRing(ringId, vnode1)
	assert.NoError(t, err)
	assert.NotNil(t, existing)
	assert.Equal(t, len(existing), 1)
	assert.Equal(t, existing[0], vnode1)

	existing, err = tr.handleJoinRing(ringId, vnode1)
	assert.NoError(t, err)
	assert.NotNil(t, existing)
	assert.Equal(t, len(existing), 1)
	assert.Equal(t, existing[0], vnode1)
}

func TestTrackerJoinTimeout(t *testing.T) {
	ringId := "ring1"
	c := NewFrozenClock()
	tr := NewTrackerWithClock(c)
	vnode1 := &Vnode{Host: "localnode1:1234", Id: []byte("vnode1")}
	vnode2 := &Vnode{Host: "localnode2:3456", Id: []byte("vnode2")}

	c.On("AfterFunc", mock.Anything, mock.Anything).Return(nil)

	existing, err := tr.(*TrackerImpl).handleJoinRing(ringId, vnode1)
	assert.NoError(t, err)
	assert.Empty(t, existing)

	c.Advance(650 * time.Second)

	existing, err = tr.handleJoinRing(ringId, vnode2)
	assert.NoError(t, err)
	assert.Empty(t, existing)
}
