package buddystore

import (
	"crypto/rand"
	mrand "math/rand"
	"strconv"
	"testing"
	"time"
)

func BenchmarkObjStoreSet_1Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 4, 1)
}

func BenchmarkObjStoreSet_2Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 4, 2)
}

func BenchmarkObjStoreSet_4Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 4, 4)
}

func benchmarkObjStoreSet_NReplicas_MSuccessors(b *testing.B, numVnodes, numSuccessors int) {
	mrand.Seed(time.Now().UTC().UnixNano())

	fastConfWithStr := func(str string) *Config { return fastConf() }

	_, t1, conf := CreateNewTCPTransportWithConfig(true, fastConfWithStr)
	_, t2, conf2 := CreateNewTCPTransportWithConfig(true, fastConfWithStr)

	conf.NumVnodes = numVnodes
	conf.NumSuccessors = numSuccessors
	r, err := Create(conf, t1)
	if err != nil {
		b.Fatalf("Failed to join the remote ring! Got %s", err)
	}

	// Second "ring" instance
	conf2.NumVnodes = numVnodes
	conf2.NumSuccessors = numSuccessors
	r2, err := Join(conf2, t2, conf.Hostname)
	if err != nil {
		b.Fatalf("Failed to join the remote ring! Got %s", err)
	}

	time.Sleep(500 * time.Millisecond)

	kvsClient := NewKVStoreClient(r2)

	l := 10
	value := make([]byte, l)

	rand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvsClient.Set(TEST_KEY+strconv.Itoa(i), value)
	}

	r.Shutdown()
}
