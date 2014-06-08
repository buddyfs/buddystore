package buddystore

import "fmt"

type TrackerClient interface {
	JoinRing(string, *Vnode) (*Ring, error)
	LeaveRing(string) error
}

type TrackerClientImpl struct {
	ring RingIntf
}

func NewTrackerClient(ring RingIntf) TrackerClient {
	return &TrackerClientImpl{ring: ring}
}

const NUM_TRACKER_REPLICAS = 2

func (tr *TrackerClientImpl) JoinRing(ringId string, self *Vnode) (*Ring, error) {
	trackerNodes, err := tr.ring.Lookup(NUM_TRACKER_REPLICAS, []byte(ringId))

	if err != nil {
		return nil, err
	}

	if len(trackerNodes) == 0 {
		return nil, fmt.Errorf("Unable to get any successors while trying to join ring")
	}

	/*
		glog.Infof("Successors: %q", trackerNodes)
		glog.Infof("RingId: %q", ringId)
		glog.Infof("Self: %q", self)
		glog.Infof("Transport: %q", tr.ring.Transport())
	*/

	transport, conf := CreateNewTCPTransport()

	vnodes, err := tr.ring.Transport().JoinRing(trackerNodes[0], ringId, self)

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
		ring, err := Join(conf, transport, vnode.String())
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
