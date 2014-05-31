package chord

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

var PORT uint = 9000

func TestRLockTCP(t *testing.T) {
	TEST_KEY := "test_key"
	conf := fastConf()
	numGo := runtime.NumGoroutine()

	listen := fmt.Sprintf("localhost:%d", PORT)
	timeout := time.Duration(20 * time.Millisecond)
	trans, err := InitTCPTransport(listen, timeout)
	r, err := Create(conf, trans)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	//  We have a Ring with TCPTransport. Create a LockManager using this ring
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	if err != nil {
		t.Fatalf("Error while getting write lock : ", err)
	}
	err := lm.CommitWLock(TEST_KEY, 1)
	if err != nil {
		t.Fatalf("Error while committing the write locked key")
	}
	retVersion, err := lm.RLock(TEST_KEY, false)
	if err != nil {
		t.Fatalf("Error while getting Read Lock ", err)
	}
	fmt.Println(retVersion)
	r.Shutdown()
	after := runtime.NumGoroutine()
	if after != numGo {
		t.Fatalf("unexpected routines! A:%d B:%d", after, numGo)
	}
}

func TestRLockLocal(t *testing.T) {
	conf := fastConf()
	numGo := runtime.NumGoroutine()

	r, err := Create(conf, nil)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	r.Shutdown()
	after := runtime.NumGoroutine()
	if after != numGo {
		t.Fatalf("unexpected routines! A:%d B:%d", after, numGo)
	}
}
