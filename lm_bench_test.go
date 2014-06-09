package buddystore

import (
	"fmt"
	"testing"
)

var i uint = 10000

/* Execute these benchmarks separately for graphing purposes */
func BenchmarkRead_NoReplica(b *testing.B) {
	fmt.Println("Running", b.N)
	var listen string = fmt.Sprintf("localhost:%d", i)
	i++
	trans, err := InitTCPTransport(listen, timeout)
	if err != nil {
		fmt.Println("TCP Transport err : ", err)
	}
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, _ := lm.WLock(TEST_KEY, 1, 10)
	_ = lm.CommitWLock(TEST_KEY, version)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lm.RLock(TEST_KEY, true)
	}
	r.Shutdown()
}

func BenchmarkWriteAndCommit_NoReplica(b *testing.B) {
	fmt.Println("Running", b.N)
	var listen string = fmt.Sprintf("localhost:%d", i)
	i++
	trans, err := InitTCPTransport(listen, timeout)
	if err != nil {
		fmt.Println("TCP Transport err : ", err)
	}
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version, _ := lm.WLock(TEST_KEY, uint(i+1), 10)
		_ = lm.CommitWLock(TEST_KEY, version)
	}
	r.Shutdown()
}
