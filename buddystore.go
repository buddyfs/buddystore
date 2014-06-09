package buddystore

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/anupcshan/Taipei-Torrent/torrent"
	"github.com/golang/glog"
	"github.com/nictuku/nettools"
)

const ENOTINITIALIZED = -1
const OK = 0

const TRACKER_URL = "udp://tracker.openbittorrent.com:80/announce"
const BUDDYSTORE_INFOHASH_BASE = "BuddyStore"
const PEERLEN = 6

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

	port, transport, conf := CreateNewTCPTransport()

	var err error

	h := sha1.New()
	io.WriteString(h, BUDDYSTORE_INFOHASH_BASE)
	infohash := hex.EncodeToString(h.Sum([]byte(nil)))[:20]

	var peeridBase = bs.Config.MyID
	h = sha1.New()
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range interfaces {
			if iface.Flags&net.FlagLoopback == net.FlagLoopback {
				continue
			}
			peeridBase = iface.HardwareAddr.String()
			break
		}
	}
	glog.Infof("Peerid base: %s", peeridBase)
	io.WriteString(h, peeridBase)
	peerid := hex.EncodeToString(h.Sum([]byte(nil)))[:20]

	glog.Infof("Announcing to tracker %s about infohash '%s' using peerid '%s' on port %d", TRACKER_URL, infohash, peerid, port)
	tResp, err := torrent.QueryTracker(nil, torrent.ClientStatusReport{InfoHash: infohash, PeerId: peerid, Port: uint16(port), Downloaded: 100, Left: 10, Uploaded: 200}, TRACKER_URL)

	if err != nil {
		glog.Errorf("Error querying tracker: %s", err)
		return err
	}

	glog.Infof("Tracker Response Peers: %d, %d, %q", tResp.Complete, tResp.Incomplete, tResp.Peers)

	peers := tResp.Peers

	if len(peers) > 0 {
		log.Println("Tracker gave us", len(peers)/PEERLEN, "peers")
		for i := 0; i < len(peers); i += PEERLEN {
			peer := nettools.BinaryToDottedPort(peers[i : i+PEERLEN])
			glog.Infof("Trying to contact peer: %q, me: %d", peer, port)
			bs.GlobalRing, err = Join(conf, transport, peer)

			if err == nil {
				glog.Infof("Successfully joined chord ring using peer %s", peer)
				break
			} else {
				glog.Infof("Failed to contact peer with error: %s", err)
			}

			bs.GlobalRing = nil
		}
	}

	if bs.GlobalRing == nil {
		bs.GlobalRing, err = Create(conf, transport)

		if err != nil {
			// Simply retry the init process
			return err
		}
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
