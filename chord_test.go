package buddystore

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

type MultiLocalTrans struct {
	remote Transport
	hosts  map[string]*LocalTransport
}

func InitMLTransport(listen string, timeout *time.Duration) *MultiLocalTrans {
	hosts := make(map[string]*LocalTransport)
	var remote Transport
	var err error
	if listen == "" && timeout == nil {
		remote = &BlackholeTransport{}
	} else {
		remote, err = InitTCPTransport(listen, *timeout)
		if err != nil {
			return nil
		}
	}
	ml := &MultiLocalTrans{hosts: hosts}
	ml.remote = remote
	return ml
}

func (ml *MultiLocalTrans) ListVnodes(host string) ([]*Vnode, error) {
	if local, ok := ml.hosts[host]; ok {
		return local.ListVnodes(host)
	}
	return ml.remote.ListVnodes(host)
}

// Ping a Vnode, check for liveness
func (ml *MultiLocalTrans) Ping(v *Vnode) (bool, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.Ping(v)
	}
	return ml.remote.Ping(v)
}

// Request a nodes predecessor
func (ml *MultiLocalTrans) GetPredecessor(v *Vnode) (*Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.GetPredecessor(v)
	}
	return ml.remote.GetPredecessor(v)
}

// Notify our successor of ourselves
func (ml *MultiLocalTrans) Notify(target, self *Vnode) ([]*Vnode, error) {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.Notify(target, self)
	}
	return ml.remote.Notify(target, self)
}

// Find a successor
func (ml *MultiLocalTrans) FindSuccessors(v *Vnode, n int, k []byte) ([]*Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.FindSuccessors(v, n, k)
	}
	return ml.remote.FindSuccessors(v, n, k)
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (ml *MultiLocalTrans) ClearPredecessor(target, self *Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.ClearPredecessor(target, self)
	}
	return ml.remote.ClearPredecessor(target, self)
}

// Instructs a node to skip a given successor. Used to leave.
func (ml *MultiLocalTrans) SkipSuccessor(target, self *Vnode) error {
	if local, ok := ml.hosts[target.Host]; ok {
		return local.SkipSuccessor(target, self)
	}
	return ml.remote.SkipSuccessor(target, self)
}

// Request a nodes predecessor list
func (ml *MultiLocalTrans) GetPredecessorList(v *Vnode) ([]*Vnode, error) {
	if local, ok := ml.hosts[v.Host]; ok {
		return local.GetPredecessorList(v)
	}
	return ml.remote.GetPredecessorList(v)
}

func (ml *MultiLocalTrans) Register(v *Vnode, o VnodeRPC) {
	local, ok := ml.hosts[v.Host]
	if !ok {
		local = InitLocalTransport(nil).(*LocalTransport)
		ml.hosts[v.Host] = local
	}
	local.Register(v, o)
}

func (ml *MultiLocalTrans) Deregister(host string) {
	delete(ml.hosts, host)
}

func (ml *MultiLocalTrans) RLock(v *Vnode, key string, nodeID string, opsLogEntry *OpsLogEntry) (string, uint, uint64, error) {
	//  TODO : Are we going to use this transport. Placeholder
	return "", 0, 0, fmt.Errorf("RLock in MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) WLock(v *Vnode, key string, version uint, timeout uint, nodeID string, opsLogEntry *OpsLogEntry) (string, uint, uint, uint64, error) {
	return "", 0, 0, 0, fmt.Errorf("WLock in MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) CommitWLock(v *Vnode, key string, version uint, nodeID string, opsLogEntry *OpsLogEntry) (uint64, error) {
	return 0, fmt.Errorf("CommitWLock in MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) AbortWLock(v *Vnode, key string, version uint, nodeID string, opsLogEntry *OpsLogEntry) (uint64, error) {
	return 0, fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) InvalidateRLock(v *Vnode, lockID string) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) Get(target *Vnode, key string, version uint) ([]byte, error) {
	return nil, fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) Set(target *Vnode, key string, version uint, value []byte) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) List(target *Vnode) ([]string, error) {
	return nil, fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) JoinRing(target *Vnode, ringId string, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) LeaveRing(target *Vnode, ringId string) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) BulkSet(target *Vnode, key string, valLst []KVStoreValue) error {
	return nil
}

func (ml *MultiLocalTrans) SyncKeys(target *Vnode, ownerVn *Vnode, key string, ver []uint) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) MissingKeys(target *Vnode, replVn *Vnode, key string, ver []uint) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) PurgeVersions(target *Vnode, key string, maxVersion uint) error {
	return fmt.Errorf("MultiLocalTransport not implemented yet")
}

func (ml *MultiLocalTrans) IsLocalVnode(vn *Vnode) bool {
	panic("THIS METHOD SHOULD NEVER BE CALLED")
}

/*
func TestDefaultConfig(t *testing.T) {
	conf := DefaultConfig("test")
	if conf.Hostname != "test" {
		t.Fatalf("bad hostname")
	}
	if conf.NumVnodes != 8 {
		t.Fatalf("bad num vnodes")
	}
	if conf.NumSuccessors != 8 {
		t.Fatalf("bad num succ")
	}
	if conf.HashFunc == nil {
		t.Fatalf("bad hash")
	}
	if conf.hashBits != 160 {
		t.Fatalf("bad hash bits")
	}
	if conf.StabilizeMin != time.Duration(15*time.Second) {
		t.Fatalf("bad min stable")
	}
	if conf.StabilizeMax != time.Duration(45*time.Second) {
		t.Fatalf("bad max stable")
	}
	if conf.Delegate != nil {
		t.Fatalf("bad delegate")
	}
}
*/

func fastConf() *Config {
	conf := DefaultConfig("test")
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)
	return conf
}

func TestCreateShutdown(t *testing.T) {
	// Start the timer thread
	time.After(15)
	conf := fastConf()
	_ = runtime.NumGoroutine()
	r, err := Create(conf, nil)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	r.Shutdown()
	_ = runtime.NumGoroutine()
	// The following will not be true because of the multiple polling mechanisms and replications that might be going on
	/*
		    if after != numGo {
				t.Fatalf("unexpected routines! A:%d B:%d", after, numGo)
			}
	*/
}

func TestJoin(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Shutdown
	r.Shutdown()
	r2.Shutdown()
}

/* Ignored for now : Added to check the case when the "created" ring has only one vnode */
func SingleNodeJoin(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	conf.NumVnodes = 1
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Shutdown
	r.Shutdown()
	r2.Shutdown()
}

func TestJoinDeadHost(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	_, err := Join(conf, ml, "noop")
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLeave(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	// Node 1 should leave
	r.Leave()
	ml.Deregister("test")

	// Wait for stabilization
	<-time.After(100 * time.Millisecond)

	// Verify r2 ring is still in tact
	num := len(r2.vnodes)
	for idx, vn := range r2.vnodes {
		if vn.successors[0] != &r2.vnodes[(idx+1)%num].Vnode {
			t.Fatalf("bad successor! Got:%s:%s", vn.successors[0].Host,
				vn.successors[0])
		}
	}
}

func TestLookupBadN(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	_, err = r.Lookup(10, []byte("test"))
	if err == nil {
		t.Fatalf("expected err!")
	}
}

func TestLookup(t *testing.T) {
	// Create a multi transport
	ml := InitMLTransport("", nil)

	// Create the initial ring
	conf := fastConf()
	r, err := Create(conf, ml)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create a second ring
	conf2 := fastConf()
	conf2.Hostname = "test2"
	r2, err := Join(conf2, ml, "test")
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	// Try key lookup
	keys := [][]byte{[]byte("test"), []byte("foo"), []byte("bar")}
	for _, k := range keys {
		vn1, err := r.Lookup(3, k)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		vn2, err := r2.Lookup(3, k)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if len(vn1) != len(vn2) {
			t.Fatalf("result len differs!")
		}
		for idx := range vn1 {
			if vn1[idx].String() != vn2[idx].String() {
				t.Fatalf("results differ!")
			}
		}
	}
}
