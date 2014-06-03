package chord

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVGetNonExistent(t *testing.T) {
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

func TestKVSetNewThenGet(t *testing.T) {
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
