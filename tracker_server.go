package buddystore

import (
	"sync"
	"time"

	"github.com/golang/glog"
)

const TRACKER_TIMEOUT_SECS = 3600

type Tracker interface {
	handleJoinRing(ringId string, joiner *Vnode) ([]*Vnode, error)
	handleLeaveRing(ringId string)
}

type TrackerImpl struct {
	timeoutQueue *IntHeap
	ringMembers  map[string][]*Vnode
	lock         sync.Mutex

	// Implements:
	Tracker
}

func NewTracker() Tracker {
	return &TrackerImpl{lock: sync.Mutex{}, ringMembers: make(map[string][]*Vnode), timeoutQueue: &IntHeap{}}
}

func (tr *TrackerImpl) handleJoinRing(ringId string, joiner *Vnode) ([]*Vnode, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	now := time.Now().Unix()
	if glog.V(2) {
		glog.Infof("Node %s joining ring %s at %d", joiner, ringId, now)
	}

	existingNodes := tr.ringMembers[ringId]
	if existingNodes == nil {
		existingNodes = []*Vnode{}
	}

	var newNodeList []*Vnode
	newNodeList = append(existingNodes, joiner)

	tr.ringMembers[ringId] = newNodeList

	return existingNodes, nil
}
