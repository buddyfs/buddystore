package chord

import (
	"fmt"
	"sync"
)

// Wraps vnode and object
type localRPC struct {
	vnode *Vnode
	obj   VnodeRPC
}

// LocalTransport is used to provides fast routing to Vnodes running
// locally using direct method calls. For any non-local vnodes, the
// request is passed on to another transport.
type LocalTransport struct {
	host   string
	remote Transport
	lock   sync.RWMutex
	local  map[string]*localRPC

	// Implements:
	Transport
}

// Creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local}
}

// Checks for a local vnode
func (lt *LocalTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	w, ok := lt.local[key]
	if ok {
		return w.obj, ok
	} else {
		return nil, ok
	}
}

func (lt *LocalTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Check if this is a local host
	if host == lt.host {
		// Generate all the local clients
		res := make([]*Vnode, 0, len(lt.local))

		// Build list
		lt.lock.RLock()
		for _, v := range lt.local {
			res = append(res, v.vnode)
		}
		lt.lock.RUnlock()

		return res, nil
	}

	// Pass onto remote
	return lt.remote.ListVnodes(host)
}

func (lt *LocalTransport) Ping(vn *Vnode) (bool, error) {
	// Look for it locally
	_, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return true, nil
	}

	// Pass onto remote
	return lt.remote.Ping(vn)
}

func (lt *LocalTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}

	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

func (lt *LocalTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.Notify(self)
	}

	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

func (lt *LocalTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessors(n, key)
	}

	// Pass onto remote
	return lt.remote.FindSuccessors(vn, n, key)
}

func (lt *LocalTransport) ClearPredecessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.ClearPredecessor(self)
	}

	// Pass onto remote
	return lt.remote.ClearPredecessor(target, self)
}

func (lt *LocalTransport) SkipSuccessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.SkipSuccessor(self)
	}

	// Pass onto remote
	return lt.remote.SkipSuccessor(target, self)
}

func (lt *LocalTransport) Register(v *Vnode, o VnodeRPC) {
	// Register local instance
	key := v.String()
	lt.lock.Lock()
	lt.host = v.Host
	lt.local[key] = &localRPC{v, o}
	lt.lock.Unlock()

	// Register with remote transport
	lt.remote.Register(v, o)
}

func (lt *LocalTransport) Deregister(v *Vnode) {
	key := v.String()
	lt.lock.Lock()
	delete(lt.local, key)
	lt.lock.Unlock()
}

func (lt *LocalTransport) RLock(targetLm *Vnode, key string, nodeID string) (string, uint, error) {
	lmVnodeRpc, _ := lt.get(targetLm)
	lockID, version, err := lmVnodeRpc.RLock(key, nodeID, "self") //  If its local it means that the sender is in the same physical machine. Possible when two instances of buddystore for same ring are running on same machine.
	return lockID, version, err
}

func (lt *LocalTransport) WLock(targetLm *Vnode, key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {
    fmt.Println("*** INSIDE WLock of LocalTrasport ***", targetLm)
	lmVnodeRpc, _ := lt.get(targetLm)
	lockID, version, timeout, err := lmVnodeRpc.WLock(key, version, timeout, nodeID)
	return lockID, version, timeout, err
}

func (lt *LocalTransport) CommitWLock(targetLm *Vnode, key string, version uint, nodeID string) error {
	lmVnodeRpc, _ := lt.get(targetLm)
	err := lmVnodeRpc.CommitWLock(key, version, nodeID)
	return err
}

func (lt *LocalTransport) AbortWLock(targetLm *Vnode, key string, version uint, nodeID string) error {
	lmVnodeRpc, _ := lt.get(targetLm)
	err := lmVnodeRpc.AbortWLock(key, version, nodeID)
	return err
}

func (lt *LocalTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	return nil, nil
}

func (lt *LocalTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	return nil
}

func (lt *LocalTransport) List(target *Vnode) ([]string, error) {
	return nil, nil
}

// BlackholeTransport is used to provide an implemenation of the Transport that
// does not actually do anything. Any operation will result in an error.
type BlackholeTransport struct {
	// Implements:
	Transport
}

func (*BlackholeTransport) ListVnodes(host string) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", host)
}

func (*BlackholeTransport) Ping(vn *Vnode) (bool, error) {
	return false, nil
}

func (*BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", vn.String())
}

func (*BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) ClearPredecessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) SkipSuccessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}

func (*BlackholeTransport) RLock(v *Vnode, key string, nodeID string) (string, uint, error) {
	return "", 0, fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) WLock(v *Vnode, key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {
	return "", 0, 0, fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) CommitWLock(v *Vnode, key string, version uint, nodeID string) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) AbortWLock(v *Vnode, key string, version uint, nodeID string) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	return nil, nil
}

func (*BlackholeTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	return nil
}

func (*BlackholeTransport) List(target *Vnode) ([]string, error) {
	return nil, nil
}
