package buddystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVIntegrationGetNonExistentKey(t *testing.T) {
	// Setting static port numbers here leads to brittle tests.
	// TODO: Is it possible to randomize port allocation, or use local transport?
	var listen string = fmt.Sprintf("localhost:%d", PORT+10)
	trans, _ := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)

	kvsClient := NewKVStoreClient(r)
	v, err := kvsClient.Get(TEST_KEY)

	assert.Nil(t, v, "Expecting nil value for non-existent key")
	assert.Error(t, err, "Expecting error while reading non-existent key")

	r.Shutdown()
}

func TestKVIntegrationCreateThenGetKey(t *testing.T) {
	var listen string = fmt.Sprintf("localhost:%d", PORT+11)
	trans, _ := InitTCPTransport(listen, timeout)
	var conf *Config = fastConf()
	r, _ := Create(conf, trans)

	kvsClient := NewKVStoreClient(r)

	bar := []byte("bar")

	err := kvsClient.Set(TEST_KEY, bar)
	assert.NoError(t, err, "Writing to new key should have no failures")

	v, err := kvsClient.Get(TEST_KEY)

	assert.NoError(t, err, "Expecting no error while reading existing key")
	assert.Equal(t, v, bar, "Sequential consistency check")

	r.Shutdown()
}

/*
 * Exercise the TCPTransport interface for KVStore (and Lock Manager).
 */
func TestKVIntegrationTCPTransportTest(t *testing.T) {
	listen1 := fmt.Sprintf("localhost:%d", PORT+12)
	listen2 := fmt.Sprintf("localhost:%d", PORT+13)

	t1, err1 := InitTCPTransport(listen1, timeout)
	t2, err2 := InitTCPTransport(listen2, timeout)
	if err1 != nil || err2 != nil {
		t.Fatalf("Error while trying to create TCP transports")
	}

	ml1 := InitLocalTransport(t1)
	ml2 := InitLocalTransport(t2)

	// Bootstrap ring
	conf := fastConf()
	conf.Hostname = "localhost:9012" // Bootstrap node
	r, err := Create(conf, ml1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Second "ring" instance
	conf2 := fastConf()
	conf2.Hostname = "localhost:9013"
	r2, err := Join(conf2, ml2, conf.Hostname)
	if err != nil {
		t.Fatalf("Failed to join the remote ring! Got %s", err)
	}

	kvsClient := NewKVStoreClient(r2)

	bar := []byte("bar")

	err = kvsClient.Set(TEST_KEY, bar)
	assert.NoError(t, err, "Writing to new key should have no failures")

	v, err := kvsClient.Get(TEST_KEY)

	assert.NoError(t, err, "Expecting no error while reading existing key")
	assert.Equal(t, v, bar, "Sequential consistency check")

	r.Shutdown()
	r2.Shutdown()
}
