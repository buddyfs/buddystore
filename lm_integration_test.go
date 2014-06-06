package buddystore

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

func TestUpdateKey(t *testing.T) {
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
	version, err = lm.WLock(TEST_KEY, 0, 10)
	if version != 2 {
		t.Fatalf("Expected version number is 2, But received ", version)
	}
	err = lm.CommitWLock(TEST_KEY, version)
	readVersion, err = lm.RLock(TEST_KEY, true)
	if err != nil {
		t.Fatalf("Error while getting RLock : ", err)
	}
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}
	r.Shutdown()
}

func TestWLockTimeTicker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var listen string = fmt.Sprintf("localhost:%d", PORT+6)
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
	time.Sleep(5 * time.Second)
	err = lm.CommitWLock(TEST_KEY_1, version)
	if err == nil {
		t.Fatalf("Expected : WLock should not be committed due to timeout")
	}
	r.Shutdown()
}

// Remote Test : Create a couple of rings, try to make a Write, Commit, Read and Abort
func TestJoinLockRemote(t *testing.T) {
	listen1 := fmt.Sprintf("localhost:%d", PORT+7)
	listen2 := fmt.Sprintf("localhost:%d", PORT+8)

	t1, err1 := InitTCPTransport(listen1, timeout)
	t2, err2 := InitTCPTransport(listen2, timeout)
	if err1 != nil || err2 != nil {
		t.Fatalf("Error while trying to create TCP transports")
	}

	ml1 := InitLocalTransport(t1)
	ml2 := InitLocalTransport(t2)

	// Create the initial ring
	conf := fastConf()
	conf.Hostname = "localhost:9007" // I know who am going to bootstrap with
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "localhost:9008" //  I know where I reside
	r2, err := Join(conf2, ml2, conf.Hostname)
	if err != nil {
		t.Fatalf("Failed to join the remote ring! Got %s", err)
	}
	// lm is the LockManagerClient for the new combined ring
	lm := &LManagerClient{Ring: r2, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)
	readVersion, err := lm.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}

	version, err = lm.WLock(TEST_KEY_1, 1, 10)
	err = lm.CommitWLock(TEST_KEY_1, version)
	readVersion, err = lm.RLock(TEST_KEY_1, true)
	if err != nil {
		t.Fatalf("Error while reading version 1 of key_1 from remote server")
	}
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}

	r.Shutdown()
	r2.Shutdown()
}

//  This is a end-to-end integration test. From Vnode to Vnode.
//  TODO : Incomplete. How to reach the LMClient from the LMserver
/*
func TestRLockInvalidate(t *testing.T) {
	listen1 := fmt.Sprintf("localhost:%d", PORT+1000)
	listen2 := fmt.Sprintf("localhost:%d", PORT+1001)

	t1, err1 := InitTCPTransport(listen1, timeout)
	t2, err2 := InitTCPTransport(listen2, timeout)
	if err1 != nil || err2 != nil {
		t.Fatalf("Error while trying to create TCP transports")
	}

	ml1 := InitLocalTransport(t1)
	ml2 := InitLocalTransport(t2)

	// Create the initial ring
	conf := fastConf()
	conf.Hostname = "localhost:10000" // I know who am going to bootstrap with
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "localhost:10001" //  I know where I reside
	r2, err := Join(conf2, ml2, conf.Hostname)
	if err != nil {
		t.Fatalf("Failed to join the remote ring! Got %s", err)
	}
	// lm is the LockManagerClient for the new combined ring
	version, err := r2.vnodes[0].lm_client.WLock(TEST_KEY, 1, 10)
	err = r2.vnodes[0].lm_client.CommitWLock(TEST_KEY, version)
	readVersion, err := r2.vnodes[0].lm_client.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}

	version, err = r2.vnodes[0].lm_client.WLock(TEST_KEY, 2, 10)
	// Check version in cache
	err = r2.vnodes[0].lm_client.CommitWLock(TEST_KEY, version)
	// Sleep and check invalidation
	readVersion, err = r2.vnodes[0].lm_client.RLock(TEST_KEY, true)
	if err != nil {
		t.Fatalf("Error while reading version 1 of key_1 from remote server")
	}
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}

	r.Shutdown()
	r2.Shutdown()
}
*/
