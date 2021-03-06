package buddystore

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/huin/goupnp/dcps/internetgateway1"
)

const LISTEN_TIMEOUT = 30 * time.Second

// Generates a random stabilization time
func randStabilize(conf *Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) == 1
	}

	// Handle the normal case
	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) == 1
}

// Checks if a key is between two ID's, right inclusive
func betweenRightIncl(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) >= 0
	}

	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) >= 0
}

// Computes the offset by (n + 2^exp) % (2^mod)
func powerOffset(id []byte, exp int, mod int) []byte {
	// Copy the existing slice
	off := make([]byte, len(id))
	copy(off, id)

	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(id)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(mod)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}

// max returns the max of two ints
func max(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// min returns the min of two ints
func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

// Returns the vnode nearest a key
func nearestVnodeToKey(vnodes []*Vnode, key []byte) *Vnode {
	for i := len(vnodes) - 1; i >= 0; i-- {
		if bytes.Compare(vnodes[i].Id, key) == -1 { // TODO : How does this promise "nearest Vnode to Key"?
			return vnodes[i]
		}
	}
	// Return the last vnode
	return vnodes[len(vnodes)-1]
}

// Merges errors together
func mergeErrors(err1, err2 error) error {
	if err1 == nil {
		return err2
	} else if err2 == nil {
		return err1
	} else {
		return fmt.Errorf("%s\n%s", err1, err2)
	}
}

func GetLocalExternalAddresses() (localAddr string, externalAddr string) {
	upnpclient, _, err := internetgateway1.NewWANIPConnection1Clients()
	if err == nil && len(upnpclient) > 0 {
		externalAddr, err = upnpclient[0].GetExternalIPAddress()
		glog.Infof("External IP: %s, err: %s", externalAddr, err)
	} else {
		glog.Infof("No external IP")
	}

	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagLoopback == net.FlagLoopback {
				continue
			}
			addrs, _ := iface.Addrs()

			// TODO: Figure out how to handle IPv6 addresses.
			if len(addrs) > 0 && addrs[0].(*net.IPNet).IP.To4() != nil {
				glog.Infof("Local Address: %s", addrs[0].(*net.IPNet).IP.To4().String())
				localAddr = addrs[0].(*net.IPNet).IP.String()
				return
			}
		}
	}

	return
}

func CreateNewTCPTransport(localOnly bool) (int, Transport, *Config) {
	return CreateNewTCPTransportWithConfig(localOnly, DefaultConfig)
}

func CreateNewTCPTransportWithConfig(localOnly bool, configGen func(string) *Config) (int, Transport, *Config) {
	var localAddr, externalAddr string

	var transport Transport
	var err error = fmt.Errorf("Dummy error")
	var port int
	var listen string

	for err != nil {
		port = int(rand.Uint32()%(64512) + 1024)
		glog.Infof("PORT: %d", port)

		listen = net.JoinHostPort("0.0.0.0", strconv.Itoa(port))
		glog.Infof("Listen Address: %s", listen)

		transport, err = InitTCPTransport(listen, LISTEN_TIMEOUT)
	}

	if !localOnly {
		localAddr, externalAddr = GetLocalExternalAddresses()
		upnpclient, errs, err := internetgateway1.NewWANIPConnection1Clients()
		glog.Infof("Client: %q, errors: %q, error: %q", upnpclient, errs, err)

		if err == nil && len(upnpclient) > 0 {
			err = upnpclient[0].AddPortMapping("", uint16(port), "TCP", uint16(port), localAddr, true, "BuddyStore", 0)
			glog.Infof("Added port mapping: %s", err)
		}
	} else {
		localAddr = "localhost"
		externalAddr = "localhost"
	}

	conf := configGen(listen)
	if len(externalAddr) > 0 {
		conf.Hostname = net.JoinHostPort(externalAddr, strconv.Itoa(port))
	} else {
		conf.Hostname = net.JoinHostPort(localAddr, strconv.Itoa(port))
	}

	return port, transport, conf
}

func copyOfVnodesList(inList []*Vnode, n int) []*Vnode {
	nodesList := make([]*Vnode, n)
	copy(nodesList, inList[:n])
	return nodesList
}
