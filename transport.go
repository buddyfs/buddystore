package buddystore

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

var _ Transport = new(LocalTransport)

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

func (lt *LocalTransport) GetPredecessorList(vn *Vnode) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessorList()
	}

	// Pass onto remote
	return lt.remote.GetPredecessorList(vn)
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
	lmVnodeRpc, ok := lt.get(targetLm)
	if !ok {
		return lt.remote.RLock(targetLm, key, "") //  Because the transport knows my nodeID better
	}
	return lmVnodeRpc.RLock(key, nodeID, "self") //  If its local it means that the sender is in the same physical machine. Possible when two instances of buddystore for same ring are running on same machine.
}

func (lt *LocalTransport) WLock(targetLm *Vnode, key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {
	lmVnodeRpc, ok := lt.get(targetLm)
	if !ok {
		return lt.remote.WLock(targetLm, key, version, timeout, nodeID)
	}
	return lmVnodeRpc.WLock(key, version, timeout, nodeID)
}

func (lt *LocalTransport) CommitWLock(targetLm *Vnode, key string, version uint, nodeID string) error {
	lmVnodeRpc, ok := lt.get(targetLm)
	if !ok {
		return lt.remote.CommitWLock(targetLm, key, version, nodeID)
	}
	return lmVnodeRpc.CommitWLock(key, version, nodeID)
}

func (lt *LocalTransport) InvalidateRLock(targetClient *Vnode, lockID string) error {
	lmVnodeRpc := lt.local[string(targetClient.Id)]
	if lmVnodeRpc == nil {
		return lt.remote.InvalidateRLock(targetClient, lockID)
	}
	return lmVnodeRpc.obj.InvalidateRLock(lockID)
}

func (lt *LocalTransport) AbortWLock(targetLm *Vnode, key string, version uint, nodeID string) error {
	lmVnodeRpc, ok := lt.get(targetLm)
	if !ok {
		return lt.remote.AbortWLock(targetLm, key, version, nodeID)
	}
	return lmVnodeRpc.AbortWLock(key, version, nodeID)
}

func (lt *LocalTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.Get(target, key, version)
	}

	return vnodeRpc.Get(key, version)
}

func (lt *LocalTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.Set(target, key, version, value)
	}

	return vnodeRpc.Set(key, version, value)
}

func (lt *LocalTransport) List(target *Vnode) ([]string, error) {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.List(target)
	}

	return vnodeRpc.List()
}

func (lt *LocalTransport) BulkSet(target *Vnode, key string, valLst []KVStoreValue) error {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.BulkSet(target, key, valLst)
	}

	return vnodeRpc.BulkSet(key, valLst)
}

func (lt *LocalTransport) SyncKeys(target *Vnode, ownerVn *Vnode, key string, ver []uint) error {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.SyncKeys(target, ownerVn, key, ver)
	}

	return vnodeRpc.SyncKeys(ownerVn, key, ver)
}

func (lt *LocalTransport) MissingKeys(target *Vnode, ownerVn *Vnode, key string, ver []uint) error {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.MissingKeys(target, ownerVn, key, ver)
	}

	return vnodeRpc.MissingKeys(ownerVn, key, ver)
}

func (lt *LocalTransport) PurgeVersions(target *Vnode, key string, maxVersion uint) error {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.PurgeVersions(target, key, maxVersion)
	}

	return vnodeRpc.PurgeVersions(key, maxVersion)
}

func (lt *LocalTransport) JoinRing(target *Vnode, ringId string, self *Vnode) ([]*Vnode, error) {
	vnodeRpc, ok := lt.get(target)

	if !ok {
		return lt.remote.JoinRing(target, ringId, self)
	}

	return vnodeRpc.JoinRing(ringId, self)
}

func (lt *LocalTransport) IsLocalVnode(target *Vnode) bool {
	_, ok := lt.get(target)
	return ok
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

func (*BlackholeTransport) GetPredecessorList(vn *Vnode) ([]*Vnode, error) {
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

func (*BlackholeTransport) Get(v *Vnode, key string, version uint) ([]byte, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) Set(v *Vnode, key string, version uint, value []byte) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) List(v *Vnode) ([]string, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) BulkSet(v *Vnode, key string, valLst []KVStoreValue) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) SyncKeys(v *Vnode, ownerVn *Vnode, key string, ver []uint) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) MissingKeys(v *Vnode, replVn *Vnode, key string, ver []uint) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}

func (*BlackholeTransport) PurgeVersions(v *Vnode, key string, maxVersion uint) error {
	return fmt.Errorf("Failed to connect! Blackhole : %s", v.String())
}
