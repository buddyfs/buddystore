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
	TrackerNodes, err := tr.ring.Lookup(NUM_TRACKER_REPLICAS, []byte(ringId))

	if err != nil {
		return nil, err
	}

	vnodes, err := tr.ring.Transport().JoinRing(TrackerNodes[0], ringId, self)

	if err != nil {
		return nil, err
	}

	transport, conf := CreateNewTCPTransport()

	for _, vnode := range vnodes {
		ring, err := Join(conf, transport, vnode.String())
		if err == nil {
			return ring, nil
		}
	}

	return nil, fmt.Errorf("Cannot connect to any of the existing nodes")
}

func (tr *TrackerClientImpl) LeaveRing(ringId string) error {
	_, err := tr.ring.Lookup(NUM_TRACKER_REPLICAS, []byte(ringId))

	if err != nil {
		return err
	}

	panic("TODO: LeaveRing")
}

var _ TrackerClient = new(TrackerClientImpl)
