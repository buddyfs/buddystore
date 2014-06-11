package buddystore

import (
	"fmt"

	"github.com/golang/glog"
)

type TrackerClient interface {
	JoinRing(string, bool) (*Ring, error)
	LeaveRing(string) error
}

type TrackerClientImpl struct {
	ring RingIntf
}

func NewTrackerClient(ring RingIntf) TrackerClient {
	return &TrackerClientImpl{ring: ring}
}

const NUM_TRACKER_REPLICAS = 2

func (tr *TrackerClientImpl) JoinRing(ringId string, localOnly bool) (*Ring, error) {
	trackerNodes, err := tr.ring.Lookup(NUM_TRACKER_REPLICAS, []byte(ringId))

	if err != nil {
		return nil, err
	}

	if len(trackerNodes) == 0 {
		return nil, fmt.Errorf("Unable to get any successors while trying to join ring")
	}

	_, transport, conf := CreateNewTCPTransport(localOnly)

	vnodes, err := tr.ring.Transport().JoinRing(trackerNodes[0], ringId, &Vnode{Host: conf.Hostname})

	if err != nil {
		return nil, err
	}

	if len(vnodes) == 0 {
		// I'm the first person joining the ring.
		// Create the ring and wait for others to join.

		ring, err := Create(conf, transport)
		if err != nil {
			return nil, err
		}

		return ring, nil
	}

	for _, vnode := range vnodes {
		if glog.V(2) {
			glog.Infof("[Ring: %s] Connecting to %s", ringId, vnode.Host)
		}

		ring, err := Join(conf, transport, vnode.Host)
		if err == nil {
			return ring, nil
		}
	}

	return nil, fmt.Errorf("Cannot connect to any existing nodes in the ring")
}

func (tr *TrackerClientImpl) LeaveRing(ringId string) error {
	_, err := tr.ring.Lookup(NUM_TRACKER_REPLICAS, []byte(ringId))

	if err != nil {
		return err
	}

	panic("TODO: LeaveRing")
}

var _ TrackerClient = new(TrackerClientImpl)
