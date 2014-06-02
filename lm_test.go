package chord

import (
	"fmt"
	"testing"
	"time"
)

/* Testing setup initializations */

var PORT uint = 9000
var TEST_KEY string = "test_key"
var TEST_KEY_1 string = "test_key_1"
var timeout time.Duration = time.Duration(100 * time.Millisecond)

func TestWriteLock(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}

	version, err := lm.WLock(TEST_KEY, 1, 10)
	if err != nil {
		t.Fatalf("Error while getting write lock : ", err)
	}
	if version != 1 {
		t.Fatalf("Version mismatch : Expected version is 1, got ", version, " instead")
	}
	r.Shutdown()
}

func TestCommitLock(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+1)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)

	err = lm.CommitWLock(TEST_KEY, version)
	if err != nil {
		t.Fatalf("Error while committing the write locked key")
	}
	r.Shutdown()

}

func TestReadLock(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+2)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)

	readVersion, err := lm.RLock(TEST_KEY, false)
	if err != nil {
		t.Fatalf("Error while getting Read Lock ", err)
	}
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	r.Shutdown()
}

func TestAbortLock(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+3)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)

	readVersion, err := lm.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	version, err = lm.WLock(TEST_KEY, 2, 10)
	err = lm.CommitWLock(TEST_KEY, version)
	readVersion, err = lm.RLock(TEST_KEY, true)
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}
	version, err = lm.WLock(TEST_KEY, 3, 10)
	err = lm.AbortWLock(TEST_KEY, version)
	if err != nil {
		t.Fatalf("Error while trying to Abort a write lock : ", err)
	}

	readVersion, err = lm.RLock(TEST_KEY, true)
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}

	r.Shutdown()
}

func TestReadLockCached(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+4)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)

	readVersion, err := lm.RLock(TEST_KEY, false)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	version, err = lm.WLock(TEST_KEY, 2, 10)
	err = lm.CommitWLock(TEST_KEY, version)
	readVersion, err = lm.RLock(TEST_KEY, false)
	if err != nil {
		t.Fatalf("Error while reading from Read Cache")
	}
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	r.Shutdown()
}

func TestWLockTimeTicker(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+5)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)

	readVersion, err := lm.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	version, err = lm.WLock(TEST_KEY_1, 2, 2)
	time.Sleep(3 * time.Second)
	err = lm.CommitWLock(TEST_KEY_1, version)
	if err == nil {
		t.Fatalf("Expected : WLock should not be committed due to timeout")
	}
	r.Shutdown()
}

/* Remote Connection test */
/*
func TestJoinLock(t *testing.T) {
	listen1 := fmt.Sprintf("localhost:%d", PORT+6)
	listen2 := fmt.Sprintf("localhost:%d", PORT+7)

	ml1 := InitMLTransport(listen1, &timeout)
	ml2 := InitMLTransport(listen2, &timeout)

	// Create the initial ring
	conf := fastConf()
	conf.Hostname = "localhost:9006"
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "localhost:9007"
	r2, err := Join(conf2, ml2, conf.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}
	lm := &LManagerClient{Ring: r2, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	_, _ = lm.WLock(TEST_KEY, 1, 10)
	/*
		err = lm.CommitWLock(TEST_KEY, version)

		readVersion, err := lm.RLock(TEST_KEY, true)
		if readVersion != 1 {
			t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
		}
		 version, err = lm.WLock(TEST_KEY, 2, 10)
		err = lm.CommitWLock(TEST_KEY, version)
		readVersion, err = lm.RLock(TEST_KEY, true)
		if err != nil {
			t.Fatalf("Error while reading from Read Cache")
		}
		if readVersion != 2 {
			t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
		}

	// Shutdown
	r.Shutdown()
	r2.Shutdown()
}
*/
