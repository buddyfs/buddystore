package buddystore

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
)

// TimeoutQueue based on PriorityQueue from http://golang.org/pkg/container/heap/
type TimeoutItem struct {
	ringId   string
	vnode    *Vnode
	priority time.Time // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A TimeoutQueue implements heap.Interface and holds TimeoutItems.
type TimeoutQueue []*TimeoutItem

func (pq TimeoutQueue) Len() int { return len(pq) }

func (pq TimeoutQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority.After(pq[j].priority)
}

func (pq TimeoutQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *TimeoutQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TimeoutItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TimeoutQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *TimeoutQueue) Peek() *TimeoutItem {
	n := len(*pq)
	if n > 0 {
		item := (*pq)[n-1]
		return item
	}

	return nil
}

func (pq *TimeoutQueue) Get(i int) *TimeoutItem {
	return (*pq)[i]
}

func (pq *TimeoutQueue) update(item *TimeoutItem, priority time.Time) {
	heap.Remove(pq, item.index)
	item.priority = priority
	heap.Push(pq, item)
}

const TRACKER_TIMEOUT_SECS = 600 * time.Second

type Tracker interface {
	handleJoinRing(ringId string, joiner *Vnode) ([]*Vnode, error)
	handleLeaveRing(ringId string)
}

type TrackerImpl struct {
	timeoutQueue *TimeoutQueue
	ringMembers  map[string][]*Vnode
	lock         sync.Mutex
	timer        *time.Timer
	clock        ClockIface
	kvClient     KVStoreClient

	// Implements:
	Tracker
}

func NewTrackerWithClockAndStore(clock ClockIface, kvClient KVStoreClient) Tracker {
	pq := &TimeoutQueue{}
	heap.Init(pq)
	return &TrackerImpl{lock: sync.Mutex{}, ringMembers: make(map[string][]*Vnode), timeoutQueue: pq, clock: clock, kvClient: kvClient}
}

func NewTrackerWithClock(clock ClockIface) Tracker {
	pq := &TimeoutQueue{}
	heap.Init(pq)
	return &TrackerImpl{lock: sync.Mutex{}, ringMembers: make(map[string][]*Vnode), timeoutQueue: pq, clock: clock}
}

func NewTrackerWithStore(store KVStoreClient) Tracker {
	return NewTrackerWithClockAndStore(new(RealClock), store)
}

func NewTracker() Tracker {
	return NewTrackerWithClock(new(RealClock))
}

func (tr *TrackerImpl) purgeHeadofQueueIfStale() {
	head := tr.timeoutQueue.Peek()

	if head == nil {
		return
	}

	nextTimer := head.priority
	now := tr.clock.Now()

	for now.After(nextTimer) {
		stale := tr.timeoutQueue.Pop()
		item := stale.(*TimeoutItem)

		if glog.V(2) {
			glog.Infof("Node %s timed out. Removing from ring.", item)
		}

		members := tr.ringMembers[item.ringId]
		posn := sort.Search(len(members), func(i int) bool {
			return members[i] == item.vnode
		})

		members = append(members[:posn], members[posn+1:]...)
		tr.ringMembers[item.ringId] = members
	}
}

func (tr *TrackerImpl) handleTimer() {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	defer tr.rescheduleTimer()

	tr.purgeHeadofQueueIfStale()
}

func (tr *TrackerImpl) rescheduleTimer() {
	if tr.timer != nil {
		tr.timer.Stop()
	}

	head := tr.timeoutQueue.Peek()
	if head == nil {
		return
	}

	nextTimer := head.priority

	timeToNextTimer := nextTimer.Sub(tr.clock.Now())
	tr.timer = tr.clock.AfterFunc(timeToNextTimer, func() {
		tr.handleTimer()
	})
}

func (tr *TrackerImpl) handleJoinRingWithTimeout(ringId string, joiner *Vnode, timeout time.Duration) ([]*Vnode, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	now := tr.clock.Now()
	if glog.V(2) {
		glog.Infof("Node %s joining ring %s at %d", joiner, ringId, now)
	}

	if len(joiner.Host) == 0 {
		return nil, fmt.Errorf("Joining node has not provided network information")
	}

	existingNodes := tr.ringMembers[ringId]
	if existingNodes == nil {
		existingNodes = []*Vnode{}
	}

	var newNodeList []*Vnode
	newNodeList = append(existingNodes, joiner)

	alreadyJoined := sort.Search(len(*tr.timeoutQueue), func(i int) bool {
		x := tr.timeoutQueue.Get(i)
		return x.ringId == ringId && x.vnode == joiner
	})

	if alreadyJoined < len(*tr.timeoutQueue) {
		tr.timeoutQueue.update(&TimeoutItem{priority: now.Add(timeout), ringId: ringId, vnode: joiner}, now.Add(timeout))
	} else {
		tr.ringMembers[ringId] = newNodeList
		tr.timeoutQueue.Push(&TimeoutItem{priority: now.Add(timeout), ringId: ringId, vnode: joiner})
	}

	tr.rescheduleTimer()

	return existingNodes, nil
}

func (tr *TrackerImpl) handleJoinRing(ringId string, joiner *Vnode) ([]*Vnode, error) {
	val, version, err := tr.kvClient.GetForSet(ringId, true)

	var nodesInRing []*Vnode
	var newNodesInRing []*Vnode

	if err != nil {
		glog.Errorf("Unable to get current tracker status")
		nodesInRing = []*Vnode{}
	} else {
		json.Unmarshal(val, nodesInRing)
	}

	newNodesInRing = append(nodesInRing, joiner)

	writeBack, err := json.Marshal(newNodesInRing)

	if err != nil {
		glog.Errorf("Marshalling error: %s", err)
		return nil, err
	}

	glog.Infof("Sending response %s", string(writeBack))

	err = tr.kvClient.SetVersion(ringId, version, writeBack)

	glog.Infof("Error setting ring data: %s", err)

	return nodesInRing, nil
}
