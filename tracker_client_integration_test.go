package buddystore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTrackerIntegrationJoinRing(t *testing.T) {
	var TEST_RING string = "TEST_RING"

	// Setting static port numbers here leads to brittle tests.
	// TODO: Is it possible to randomize port allocation, or use local transport?
	var listen string = fmt.Sprintf("localhost:%d", PORT+20)
	trans, _ := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)

	trackerClient := NewTrackerClient(r)
	v, err := trackerClient.JoinRing(TEST_RING, true)

	assert.NotNil(t, v)
	assert.NoError(t, err)

	r.Shutdown()
}

/*
 * Exercise the TCPTransport interface for Tracker.
 */
func TestTrackerIntegrationTCPTransportTest(t *testing.T) {
	var TEST_RING string = "TEST_RING"

	listen1 := fmt.Sprintf("localhost:%d", PORT+22)
	listen2 := fmt.Sprintf("localhost:%d", PORT+23)

	t1, err1 := InitTCPTransport(listen1, timeout)
	t2, err2 := InitTCPTransport(listen2, timeout)
	if err1 != nil || err2 != nil {
		t.Fatalf("Error while trying to create TCP transports")
	}

	ml1 := InitLocalTransport(t1)
	ml2 := InitLocalTransport(t2)

	// Bootstrap ring
	conf := fastConf()
	conf.Hostname = "localhost:9022" // Bootstrap node
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Second "ring" instance
	conf2 := fastConf()
	conf2.Hostname = "localhost:9023"
	r2, err := Join(conf2, ml2, conf.Hostname)
	time.Sleep(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to join the remote ring! Got %s", err)
	}

	trackerClient := NewTrackerClient(r2)
	v, err := trackerClient.JoinRing(TEST_RING, true)

	assert.NotNil(t, v)
	assert.NoError(t, err)

	r.Shutdown()
	r2.Shutdown()
}
