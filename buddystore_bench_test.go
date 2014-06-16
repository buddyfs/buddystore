package buddystore

import (
	"crypto/rand"
	mrand "math/rand"
	"strconv"
	"testing"
	"time"
)

func BenchmarkObjStoreSet_2Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 2, 1000)
}

func BenchmarkObjStoreSet_4Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 4, 1000)
}

func BenchmarkObjStoreSet_8Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 8, 1000)
}

func BenchmarkObjStoreSet_2Replicas_10000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 2, 10000)
}

func BenchmarkObjStoreSet_4Replicas_10000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 4, 10000)
}

func BenchmarkObjStoreSet_8Replicas_10000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 8, 10000)
}

func BenchmarkObjStoreSet_2Replicas_100000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 2, 100000)
}

func BenchmarkObjStoreSet_4Replicas_100000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 4, 100000)
}

func BenchmarkObjStoreSet_8Replicas_100000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 8, 100000)
}

func BenchmarkObjStoreSet_1Replicas_100000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 1, 100000)
}

func BenchmarkObjStoreSet_1Replicas_10000(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 1, 10000)
}

func BenchmarkObjStoreSet_1Replicas(b *testing.B) {
	benchmarkObjStoreSet_NReplicas_MSuccessors(b, 8, 1, 1000)
}

func benchmarkObjStoreSet_NReplicas_MSuccessors(b *testing.B, numVnodes, numSuccessors int, l int) {
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

	value := make([]byte, l)

	rand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvsClient.Set(TEST_KEY+strconv.Itoa(i), value)
	}

	r.Shutdown()
}

func BenchmarkObjStoreGet_2Replicas_2Vnodes_10000(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 2, 2, 10000)
}

func BenchmarkObjStoreGet_4Replicas_4Vnodes_10000(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 4, 4, 10000)
}

func BenchmarkObjStoreGet_6Replicas_6Vnodes_10000(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 6, 6, 10000)
}

func BenchmarkObjStoreGet_8Replicas_8Vnodes_10000(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 8, 8, 10000)
}

func BenchmarkObjStoreGet_2Replicas_2Vnodes(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 2, 2, 1000)
}

func BenchmarkObjStoreGet_4Replicas_4Vnodes(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 4, 4, 1000)
}

func BenchmarkObjStoreGet_6Replicas_6Vnodes(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 6, 6, 1000)
}

func BenchmarkObjStoreGet_8Replicas_8Vnodes(b *testing.B) {
	benchmarkObjStoreGet_NReplicas_MSuccessors(b, 8, 8, 1000)
}

func benchmarkObjStoreGet_NReplicas_MSuccessors(b *testing.B, numVnodes, numSuccessors int, l int) {
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

	value := make([]byte, l)
	rand.Read(value)

	kvsClient.Set(TEST_KEY, value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvsClient.Get(TEST_KEY, true)
	}

	r.Shutdown()
}
