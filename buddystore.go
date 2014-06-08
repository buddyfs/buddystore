package buddystore

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
)

const LISTEN_TIMEOUT = 1000

const ENOTINITIALIZED = -1
const OK = 0

type BuddyStore struct {
	Config       *BuddyStoreConfig
	GlobalRing   RingIntf
	SubRings     map[string]RingIntf
	LockManagers map[string]LMClientIntf
	Tracker      TrackerClient

	initalized bool
	lock       sync.Mutex
}

type BuddyStoreConfig struct {
	MyID    string
	Friends []string
}

/*
 * Attach ring and associated lock manager to BuddyStore state.
 * Expected to be called with lock being held.
 */
func (bs *BuddyStore) addRing(ringId string, ring RingIntf) {
	bs.SubRings[ringId] = ring
	bs.LockManagers[ringId] = &LManagerClient{Ring: ring, RLocks: make(map[string]*RLockVal), WLocks: make(map[string]*WLockVal)}
}

/*
 * Join the global ring and all interested subrings.
 */
func (bs *BuddyStore) init() error {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	if bs.initalized {
		return fmt.Errorf("Attempting to initialize an already initialized store")
	}

	transport, conf := CreateNewTCPTransport()

	var err error

	// ring, err := Join(conf, transport, <host name from tracker>)
	bs.GlobalRing, err = Create(conf, transport)

	if err != nil {
		// Simply retry the init process
		return err
	}

	bs.Tracker = NewTrackerClient(bs.GlobalRing)

	// Join my own ring
	ring, err := bs.Tracker.JoinRing(bs.Config.MyID, bs.GlobalRing.GetLocalVnode())
	if err != nil {
		// If I'm not able to join my own ring, bail
		return err
	} else {
		bs.addRing(bs.Config.MyID, ring)
	}

	// Any errors from this point on will not prevent initialization
	// from completing successfully.

	// Join my friends' rings
	for _, friend := range bs.Config.Friends {
		ring, err := bs.Tracker.JoinRing(friend, bs.GlobalRing.GetLocalVnode())

		if err != nil {
			bs.addRing(friend, ring)
		}
	}

	bs.initalized = true
	return nil
}

func (bs BuddyStore) GetMyKVClient() (KVStoreClient, int) {
	return bs.GetKVClient(bs.Config.MyID)
}

func (bs BuddyStore) GetKVClient(ringId string) (KVStoreClient, int) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	if !bs.initalized {
		return nil, ENOTINITIALIZED
	}

	ring := bs.SubRings[ringId]
	lm := bs.LockManagers[ringId]

	kvClient := NewKVStoreClientWithLM(ring, lm)
	return kvClient, OK
}

func NewBuddyStore(bsConfig *BuddyStoreConfig) *BuddyStore {
	if len(bsConfig.MyID) == 0 {
		glog.Errorf("Cannot create BuddyStore instance without ID")
		return nil
	}

	bs := &BuddyStore{Config: bsConfig, lock: sync.Mutex{}, SubRings: make(map[string]RingIntf), LockManagers: make(map[string]LMClientIntf)}
	err := bs.init()

	if err != nil {
		// TODO: Should we retry initialization here?
		glog.Errorf("Error while initializing buddystore: %s", err)
	}

	return bs
}
