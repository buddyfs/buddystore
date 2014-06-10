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
	if err != nil {
		t.Fatal(err)
	}

	var conf *Config = fastConf()
	conf.NumVnodes = 5
	conf.StabilizeMin = 50 * time.Millisecond
	conf.StabilizeMax = 100 * time.Millisecond
	r, err := Create(conf, trans)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for stabilization
	time.Sleep(150 * time.Millisecond)

	version, err := r.vnodes[0].lm_client.WLock(TEST_KEY, 1, 10)
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
	trans, _ := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, _ := lm.WLock(TEST_KEY, 1, 10)
	_ = lm.CommitWLock(TEST_KEY, version)

	readVersion, _ := lm.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	version, _ = lm.WLock(TEST_KEY, 2, 10)
	_ = lm.CommitWLock(TEST_KEY, version)
	readVersion, _ = lm.RLock(TEST_KEY, true)
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}
	version, _ = lm.WLock(TEST_KEY, 3, 10)
	err := lm.AbortWLock(TEST_KEY, version)
	if err != nil {
		t.Fatalf("Error while trying to Abort a write lock : ", err)
	}

	readVersion, _ = lm.RLock(TEST_KEY, true)
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
	version, err := lm.WLock(TEST_KEY, 0, 2)
	err = lm.CommitWLock(TEST_KEY, version)
	if err != nil {
		t.Fatalf("Commit failed : ", err)
	}
	readVersion, err := lm.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	version, err = lm.WLock(TEST_KEY, 0, 2)
	time.Sleep(3 * time.Second)
	err = lm.CommitWLock(TEST_KEY, version)
	if err == nil {
		t.Fatalf("Expected : WLock should not be committed due to timeout")
	}
	t.Log(err)
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
func TestRLockInvalidate(t *testing.T) {
	<-time.After(100 * time.Millisecond)
	listen1 := fmt.Sprintf("localhost:%d", PORT+1000)
	listen2 := fmt.Sprintf("localhost:%d", PORT+1001)

	t1, err1 := InitTCPTransport(listen1, timeout)
	t2, err2 := InitTCPTransport(listen2, timeout)
	if err1 != nil || err2 != nil {
		t.Fatalf("Error while trying to create TCP transports")
	}

	ml1 := InitLocalTransport(t1)
	ml2 := InitLocalTransport(t2)

	conf := fastConf()
	conf.NumVnodes = 2
	conf.Hostname = "localhost:10000" // I know who am going to bootstrap with
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	conf2 := fastConf()
	conf2.NumVnodes = 2
	conf2.Hostname = "localhost:10001" //  I know where I reside
	r2, err := Join(conf2, ml2, conf.Hostname)
	time.Sleep(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to join the remote ring! Got %s", err)
	}
	version, err := r2.vnodes[0].lm_client.WLock(TEST_KEY, 1, 10)
	err = r2.vnodes[0].lm_client.CommitWLock(TEST_KEY, version)
	readVersion, err := r2.vnodes[0].lm_client.RLock(TEST_KEY, true)
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}

	version, err = r2.vnodes[0].lm_client.WLock(TEST_KEY, 2, 10)
	err = r2.vnodes[0].lm_client.CommitWLock(TEST_KEY, version) //  Will invalidate the RLock acquired for version 1
	time.Sleep(200 * time.Millisecond)
	if r2.vnodes[0].lm_client.keyInRLocksCache(TEST_KEY) {
		t.Fatalf("Expected : Key should not be present in RLocks cache, but it is present even after invalidation")
	}
	readVersion, err = r2.vnodes[0].lm_client.RLock(TEST_KEY, true)
	if err != nil {
		t.Fatalf("Error while reading version 1 of key_1 from remote server")
	}
	if readVersion != 2 {
		t.Fatalf("Version mismatch : Expected version 2, got ", readVersion, " instead")
	}
	if !r2.vnodes[0].lm_client.keyInRLocksCache(TEST_KEY) {
		t.Fatalf("Expected : Key should be present in RLocks cache, but it is not present after the recent RLock request")
	}

	r.Shutdown()
	r2.Shutdown()
}

/* Check if there is only one lockManager irrespective of the number of members in the ring */
func LMDetector(t *testing.T) {
	<-time.After(100 * time.Millisecond)
	var numRings uint = 5
	var i uint
	var err error
	var trans *TCPTransport
	r := make([]*Ring, numRings)
	for i = 0; i < numRings; i++ {
		go func(i uint) {
			listen := fmt.Sprintf("localhost:%d", PORT+1020+i)
			trans, err = InitTCPTransport(listen, timeout)
			ml := InitLocalTransport(trans)
			if err != nil {
				t.Fatalf("Error while trying to create TCP transports")
			}
			conf := fastConf()
			conf.Hostname = fmt.Sprintf("localhost:%d", PORT+1020+i)
			conf.RingId = "a"
			conf.NumVnodes = 3
			if i != 0 {
				r[i], err = Join(conf, ml, "localhost:10020")
				time.Sleep(100 * time.Millisecond)
			} else {
				r[i], err = Create(conf, ml)
			}
			if err != nil {
				t.Fatalf("Error while creating and joining rings : ", err)
			}
		}(i)
	}
	LMStatePoller := time.NewTicker(600 * time.Millisecond)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-LMStatePoller.C:
				var lmCounter uint = 0
				for i = 0; i < numRings; i++ {
					for j := 0; r[i] != nil && j < len(r[i].vnodes); j++ {
						if r[i].vnodes[j].lm.CurrentLM {
							lmCounter++
						}
					}
				}
				if lmCounter != 1 {
					t.Fatalf("Expected : One LockManager. Got ", lmCounter, " LockManagers instead")
				}
			case <-quit:
				LMStatePoller.Stop()
				return
			}
		}
	}()
	time.Sleep(700 * time.Millisecond)
	quit <- true
	for i = 0; i < numRings; i++ {
		r[i].Shutdown()
	}
}

func TestReadReplication(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+1050)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	// Sleep for 100 ms for LM to be selected
	time.Sleep(110 * time.Millisecond)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)

	readVersion, err := lm.RLock(TEST_KEY, true)
	if err != nil {
		t.Fatalf("Error while getting Read Lock ", err)
	}
	if readVersion != 1 {
		t.Fatalf("Version mismatch : Expected version 1, got ", readVersion, " instead")
	}
	r.Shutdown()
}

func TestWriteReplication(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+1051)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	// Sleep for 100 ms for LM to be selected
	time.Sleep(110 * time.Millisecond)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	if err != nil {
		t.Fatalf("Unexpected Error : ", err)
	}
	if version != 1 {
		t.Fatalf("Expected version : 1, but got ", version, " from the LMserver")
	}
	// Actual replication check
	LMVnodes, err := lm.Ring.Lookup(1, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error in getting the LM node : But got ", err)
	}
	LMLocalVnode, _ := r.Transport().(*LocalTransport).get(LMVnodes[0]) // We will surely get a localVnode since all are local
	succNodes, err := LMLocalVnode.FindSuccessors(2, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error while finding successors for the current LM, but got ", err)
	}
	if len(succNodes) != 2 {
		t.Fatalf("Not enough replicas, need 2 but found ", len(succNodes))
	}
	for i := range succNodes {
		localVnode, _ := r.Transport().(*LocalTransport).get(succNodes[i])
		present, version, err := localVnode.CheckWLock(TEST_KEY)
		if err != nil {
			t.Fatalf("Not a replication error : Error occurred while trying to get localVnode : got ", err)
		}
		if !present {
			t.Fatalf("Replication failed : Key not present in replica ", i)
		}
		if version != 1 {
			t.Fatalf("Replication failed : Key present, but the version number is ", version, " instead of 1 in replica ", i)
		}
	}

	r.Shutdown()
}

/* Same as write, expected behavior from LM replica are different */
func TestCommitReplication(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+1052)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	// Sleep for 100 ms for LM to be selected
	time.Sleep(110 * time.Millisecond)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.CommitWLock(TEST_KEY, version)
	// Actual replication check
	LMVnodes, err := lm.Ring.Lookup(1, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error in getting the LM node : But got ", err)
	}
	LMLocalVnode, _ := r.Transport().(*LocalTransport).get(LMVnodes[0]) // We will surely get a localVnode since all are local
	succNodes, err := LMLocalVnode.FindSuccessors(2, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error while finding successors for the current LM, but got ", err)
	}
	if len(succNodes) != 2 {
		t.Fatalf("Not enough replicas, need 2 but found ", len(succNodes))
	}
	for i := range succNodes {
		localVnode, _ := r.Transport().(*LocalTransport).get(succNodes[i])
		present, _, err := localVnode.CheckWLock(TEST_KEY)
		if err != nil {
			t.Fatalf("Not a replication error : Error occurred while trying to get localVnode : got ", err)
		}
		if present {
			t.Fatalf("Commit Replication failed : Key still present in WriteLocks in replica ", i)
		}
		id, _ := localVnode.GetId()
		_, ver, err := localVnode.RLock(TEST_KEY, id, "") // Dummy values, don't need to bother about invalidation.
		if err != nil {
			t.Fatalf("Commit Replication failed : Got ", err, " : Unable to get RLock on a committed key ", TEST_KEY, " on replica ", i)
		}
		if ver != 1 {
			t.Fatalf("Commit Replication failed : Expected version number is 1, but got ", ver, " on replica", i)
		}
	}

	r.Shutdown()
}

/* Same as commit, expected behavior slightly from LM replica are different */
func TestAbortReplication(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+1053)
	trans, err := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, err := Create(conf, trans)
	// Sleep for 100 ms for LM to be selected
	time.Sleep(110 * time.Millisecond)
	lm := &LManagerClient{Ring: r, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
	version, err := lm.WLock(TEST_KEY, 1, 10)
	err = lm.AbortWLock(TEST_KEY, version)
	// Actual replication check
	LMVnodes, err := lm.Ring.Lookup(1, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error in getting the LM node : But got ", err)
	}
	LMLocalVnode, _ := r.Transport().(*LocalTransport).get(LMVnodes[0]) // We will surely get a localVnode since all are local
	succNodes, err := LMLocalVnode.FindSuccessors(2, []byte(lm.Ring.GetRingId()))
	if err != nil {
		t.Fatalf("Expected no error while finding successors for the current LM, but got ", err)
	}
	if len(succNodes) != 2 {
		t.Fatalf("Not enough replicas, need 2 but found ", len(succNodes))
	}
	for i := range succNodes {
		localVnode, _ := r.Transport().(*LocalTransport).get(succNodes[i])
		present, _, err := localVnode.CheckWLock(TEST_KEY)
		if err != nil {
			t.Fatalf("Not a replication error : Error occurred while trying to get localVnode : got ", err)
		}
		if present {
			t.Fatalf("Abort Replication failed : Key still present in WriteLocks in replica ", i)
		}
		id, _ := localVnode.GetId()
		_, ver, err := localVnode.RLock(TEST_KEY, id, "") // Dummy values, don't need to bother about invalidation.
		if err == nil {
			t.Fatalf("Abort Replication failed : Should get RLock not available error for the aborted key ", TEST_KEY, " on replica ", i)
		}
		if ver != 0 {
			t.Fatalf("Abort Replication failed : Expected version number is 0, but got ", ver, " on replica", i)
		}
	}

	r.Shutdown()
}
