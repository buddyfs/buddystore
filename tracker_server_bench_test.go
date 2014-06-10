package buddystore

import (
	"math/rand"
	"net"
	"strconv"
	"testing"
)

func BenchmarkTrackerJoinRing(b *testing.B) {
	ringId := "ring1"
	tr := NewTracker()

	mod := uint(1000)

	vnodes := make([]*Vnode, mod)
	for i := uint(0); i < mod; i++ {
		vnodes[i] = &Vnode{Host: net.JoinHostPort("localnode"+strconv.Itoa(rand.Int()), strconv.Itoa(rand.Intn(65536))), Id: []byte("vnode1")}
	}

	for i = 0; i < uint(b.N); i++ {
		_, _ = tr.handleJoinRing(ringId, vnodes[i%mod])
	}
}
