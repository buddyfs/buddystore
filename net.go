package buddystore

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

/*
TCPTransport provides a TCP based Chord transport layer. This allows Chord
to be implemented over a network, instead of only using the LocalTransport. It is
meant to be a simple implementation, optimizing for simplicity instead of performance.
Messages are sent with a header frame, followed by a body frame. All data is encoded
using the GOB format for simplicity.

Internally, there is 1 Goroutine listening for inbound connections, 1 Goroutine PER
inbound connection.
*/
type TCPTransport struct {
	sock     *net.TCPListener
	timeout  time.Duration
	maxIdle  time.Duration
	lock     sync.RWMutex
	local    map[string]*localRPC
	inbound  map[*net.TCPConn]struct{}
	poolLock sync.Mutex
	pool     map[string][]*tcpOutConn
	shutdown int32

	// Implements:
	Transport
}

var _ Transport = new(TCPTransport)

type tcpOutConn struct {
	host   string
	sock   *net.TCPConn
	header tcpHeader
	enc    *gob.Encoder
	dec    *gob.Decoder
	used   time.Time
}

const (
	tcpPing = iota
	tcpListReq
	tcpGetPredReq
	tcpNotifyReq
	tcpFindSucReq
	tcpClearPredReq
	tcpSkipSucReq
	tcpGet
	tcpSet
	tcpList
	tcpRLockReq
	tcpWLockReq
	tcpCommitWLockReq
	tcpAbortWLockReq
)

type tcpHeader struct {
	ReqType int
}

type tcpRequest interface {
}

type tcpResponseBody interface {
}

type tcpResponse interface {
	Error() error
	SetError(error)
}

type tcpResponseImpl struct {
	Err error

	// Implements:
	tcpResponse
}

func (t *tcpResponseImpl) Error() error {
	return t.Err
}

func (t *tcpResponseImpl) SetError(err error) {
	t.Err = err
}

var _ tcpResponse = new(tcpBodyVnodeListError)

// Potential body types
type tcpBodyError struct {
	Dummy bool

	// Extends:
	tcpResponseImpl
}
type tcpBodyString struct {
	S string
}
type tcpBodyVnode struct {
	Vn *Vnode
}
type tcpBodyTwoVnode struct {
	Target *Vnode
	Vn     *Vnode
}
type tcpBodyFindSuc struct {
	Target *Vnode
	Num    int
	Key    []byte
}

type tcpBodyVnodeError struct {
	Vnode *Vnode

	// Extends:
	tcpResponseImpl
}

type tcpBodyVnodeListError struct {
	Vnodes []*Vnode

	// Extends:
	tcpResponseImpl
}

type tcpBodyBoolError struct {
	B bool

	// Extends:
	tcpResponseImpl
}

/* TCP body for KV store requests */
type tcpBodyGet struct {
	Vnode   *Vnode
	Key     string
	Version uint
}

type tcpBodySet struct {
	Vnode   *Vnode
	Key     string
	Version uint
	Value   []byte
}

type tcpBodyList struct {
	Vnode *Vnode
}

/* TCP body for KV store responses */
type tcpBodyRespValue struct {
	Value []byte

	// Extends:
	tcpResponseImpl
}

type tcpBodyRespKeys struct {
	Keys []string

	// Extends:
	tcpResponseImpl
}

// Creates a new TCP transport on the given listen address with the
// configured timeout duration.
func InitTCPTransport(listen string, timeout time.Duration) (*TCPTransport, error) {
	// Try to start the listener
	sock, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	// allocate maps
	local := make(map[string]*localRPC)
	inbound := make(map[*net.TCPConn]struct{})
	pool := make(map[string][]*tcpOutConn)

	// Maximum age of a connection
	maxIdle := time.Duration(300 * time.Second)

	// Setup the transport
	tcp := &TCPTransport{sock: sock.(*net.TCPListener),
		timeout: timeout,
		maxIdle: maxIdle,
		local:   local,
		inbound: inbound,
		pool:    pool}

	// Listen for connections
	go tcp.listen()

	// Reap old connections
	go tcp.reapOld()

	// Done
	return tcp, nil
}

// Checks for a local vnode
func (t *TCPTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()
	t.lock.RLock()
	defer t.lock.RUnlock()
	w, ok := t.local[key]
	if ok {
		return w.obj, ok
	} else {
		return nil, ok
	}
}

// Gets an outbound connection to a host
func (t *TCPTransport) getConn(host string) (*tcpOutConn, error) {
	// Check if we have a conn cached
	var out *tcpOutConn
	t.poolLock.Lock()
	if atomic.LoadInt32(&t.shutdown) == 1 {
		t.poolLock.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}
	list, ok := t.pool[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		t.pool[host] = list
	}
	t.poolLock.Unlock()
	if out != nil {
		// Verify that the socket is valid. Might be closed.
		if _, err := out.sock.Read(nil); err == nil {
			return out, nil
		}
	}

	// Try to establish a connection
	conn, err := net.DialTimeout("tcp", host, t.timeout)
	if err != nil {
		return nil, err
	}

	// Setup the socket
	sock := conn.(*net.TCPConn)
	t.setupConn(sock)
	enc := gob.NewEncoder(sock)
	dec := gob.NewDecoder(sock)
	now := time.Now()

	// Wrap the sock
	out = &tcpOutConn{host: host, sock: sock, enc: enc, dec: dec, used: now}
	return out, nil
}

// Returns an outbound TCP connection to the pool
func (t *TCPTransport) returnConn(o *tcpOutConn) {
	// Update the last used time
	o.used = time.Now()

	// Push back into the pool
	t.poolLock.Lock()
	defer t.poolLock.Unlock()
	if atomic.LoadInt32(&t.shutdown) == 1 {
		o.sock.Close()
		return
	}
	list, _ := t.pool[o.host]
	t.pool[o.host] = append(list, o)
}

// Setup a connection
func (t *TCPTransport) setupConn(c *net.TCPConn) {
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
}

func (t *TCPTransport) networkCall(host string, tcpReqType int, req tcpRequest, resp tcpResponse) error {
	// Get a conn
	out, err := t.getConn(host)
	if err != nil {
		return err
	}

	// Response channels
	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpReqType
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(req); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		if err := out.dec.Decode(resp); err != nil {
			errChan <- err
		}

		// Return the connection
		t.returnConn(out)
		if resp.Error() == nil {
			respChan <- true
		} else {
			errChan <- resp.Error()
		}
	}()

	select {
	case <-time.After(t.timeout):
		return fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// Gets a list of the vnodes on the box
func (t *TCPTransport) ListVnodes(host string) ([]*Vnode, error) {
	resp := tcpBodyVnodeListError{}
	err := t.networkCall(host, tcpListReq, tcpBodyString{S: host}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Vnodes, nil
	}
}

// Ping a Vnode, check for liveness
func (t *TCPTransport) Ping(vn *Vnode) (bool, error) {
	resp := tcpBodyBoolError{}
	err := t.networkCall(vn.Host, tcpPing, tcpBodyVnode{Vn: vn}, &resp)

	if err != nil {
		return false, err
	} else {
		return resp.B, nil
	}
}

// Request a nodes predecessor
func (t *TCPTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	resp := tcpBodyVnodeError{}
	err := t.networkCall(vn.Host, tcpGetPredReq, tcpBodyVnode{Vn: vn}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Vnode, nil
	}
}

// Notify our successor of ourselves
func (t *TCPTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	resp := tcpBodyVnodeListError{}
	err := t.networkCall(target.Host, tcpNotifyReq, tcpBodyTwoVnode{Target: target, Vn: self}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Vnodes, nil
	}
}

// Find a successor
func (t *TCPTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	resp := tcpBodyVnodeListError{}
	err := t.networkCall(vn.Host, tcpFindSucReq, tcpBodyFindSuc{Target: vn, Num: n, Key: k}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Vnodes, nil
	}
}

/* Transport operation that gets the value of a given key - This operation is additional to what is there in the interface already
 */

func (t *TCPTransport) Get(target *Vnode, key string, version uint) ([]byte, error) {
	resp := tcpBodyRespValue{}
	err := t.networkCall(target.Host, tcpGet, tcpBodyGet{Vnode: target, Key: key, Version: version}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Value, nil
	}
}

/* Transport operation that sets the value of a given key - This operation is additional to what is there in the interface already
 */

func (t *TCPTransport) Set(target *Vnode, key string, version uint, value []byte) error {
	resp := tcpBodyError{}
	err := t.networkCall(target.Host, tcpSet, tcpBodySet{Vnode: target, Key: key, Version: version, Value: value}, &resp)

	if err != nil {
		return err
	} else {
		return nil
	}
}

/* Transport operation that lists the keys for a particular ring - This operation is additional to what is there in the interface already
 */

func (t *TCPTransport) List(target *Vnode) ([]string, error) {
	resp := tcpBodyRespKeys{}
	err := t.networkCall(target.Host, tcpList, tcpBodyList{Vnode: target}, &resp)

	if err != nil {
		return nil, err
	} else {
		return resp.Keys, nil
	}
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (t *TCPTransport) ClearPredecessor(target, self *Vnode) error {
	resp := tcpBodyError{}
	err := t.networkCall(target.Host, tcpClearPredReq, tcpBodyTwoVnode{Target: target, Vn: self}, &resp)

	if err != nil {
		return err
	} else {
		return nil
	}
}

// Instructs a node to skip a given successor. Used to leave.
func (t *TCPTransport) SkipSuccessor(target, self *Vnode) error {
	resp := tcpBodyError{}
	err := t.networkCall(target.Host, tcpSkipSucReq, tcpBodyTwoVnode{Target: target, Vn: self}, &resp)

	if err != nil {
		return err
	} else {
		return nil
	}
}

// Register for an RPC callbacks
func (t *TCPTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.String()
	t.lock.Lock()
	t.local[key] = &localRPC{v, o}
	t.lock.Unlock()
}

// Shutdown the TCP transport
func (t *TCPTransport) Shutdown() {
	atomic.StoreInt32(&t.shutdown, 1)
	t.sock.Close()

	// Close all the inbound connections
	t.lock.RLock()
	for conn := range t.inbound {
		conn.Close()
	}
	t.lock.RUnlock()

	// Close all the outbound
	t.poolLock.Lock()
	for _, conns := range t.pool {
		for _, out := range conns {
			out.sock.Close()
		}
	}
	t.pool = nil
	t.poolLock.Unlock()
}

// Closes old outbound connections
func (t *TCPTransport) reapOld() {
	for {
		if atomic.LoadInt32(&t.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		t.reapOnce()
	}
}

func (t *TCPTransport) reapOnce() {
	t.poolLock.Lock()
	defer t.poolLock.Unlock()
	for host, conns := range t.pool {
		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].used) > t.maxIdle {
				conns[i].sock.Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		t.pool[host] = conns[:max]
	}
}

// Listens for inbound connections
func (t *TCPTransport) listen() {
	for {
		conn, err := t.sock.AcceptTCP()
		if err != nil {
			if atomic.LoadInt32(&t.shutdown) == 0 {
				fmt.Printf("[ERR] Error accepting TCP connection! %s", err)
				continue
			} else {
				return
			}
		}

		// Setup the conn
		t.setupConn(conn)

		// Register the inbound conn
		t.lock.Lock()
		t.inbound[conn] = struct{}{}
		t.lock.Unlock()

		// Start handler
		go t.handleConn(conn)
	}
}

/*
RLock tranasport layer implementation
Param Vnode : The destination Vnode i.e. the Lock Manager
Param key : The key for which the read lock should be obtained
*/
func (t *TCPTransport) RLock(target *Vnode, key string, nodeID string) (string, uint, error) {
	resp := tcpBodyLMRLockResp{}
	err := t.networkCall(target.Host, tcpRLockReq, tcpBodyLMRLockReq{Vn: target, Key: key, SenderID: nodeID}, &resp)

	if err != nil {
		return "", 0, resp.Err
	} else {
		return resp.LockId, resp.Version, nil
	}
}

/*
WLock transport layer implementation
Param Vnode : The destination Vnode i.e. the Vnode with the Lock Manager
Param key : The key for which the write lock should be obtained
Param version : The version of the key
Param timeout : Requested Timeout value.
Param NodeID : NodeID of the requesting node
*/
func (t *TCPTransport) WLock(target *Vnode, key string, version uint, timeout uint, nodeID string) (string, uint, uint, error) {
	resp := tcpBodyLMWLockResp{}
	err := t.networkCall(target.Host, tcpWLockReq, tcpBodyLMWLockReq{Vn: target, Key: key, Version: version, Timeout: timeout, SenderID: nodeID}, &resp)

	if err != nil {
		return "", 0, 0, resp.Err
	} else {
		return resp.LockId, resp.Version, resp.Timeout, nil
	}
}

/*
CommitWLock transport layer implementation
Param Vnode : The destination Vnode i.e. the Lock Manager
Param key : The key for which the read lock should be obtained
Param version : The version of the key to be committed
*/
func (t *TCPTransport) CommitWLock(target *Vnode, key string, version uint, nodeID string) error {
	resp := tcpBodyLMCommitWLockResp{}
	body := tcpBodyLMCommitWLockReq{Vn: target, Key: key, Version: version, SenderID: nodeID}
	err := t.networkCall(target.Host, tcpCommitWLockReq, body, &resp)

	if err != nil {
		return resp.Err
	} else {
		return nil
	}
}

/*
AbortWLock transport layer implementation
Param Vnode : The destination Vnode i.e. the Lock Manager
Param key : The key for which the read lock should be obtained
*/
func (t *TCPTransport) AbortWLock(target *Vnode, key string, version uint, nodeID string) error {
	resp := tcpBodyLMAbortWLockResp{}
	body := tcpBodyLMAbortWLockReq{Vn: target, Key: key, Version: version, SenderID: nodeID}
	err := t.networkCall(target.Host, tcpAbortWLockReq, body, &resp)

	if err != nil {
		return resp.Err
	} else {
		return nil
	}
}

// Handles inbound TCP connections
func (t *TCPTransport) handleConn(conn *net.TCPConn) {
	// Defer the cleanup
	defer func() {
		t.lock.Lock()
		delete(t.inbound, conn)
		t.lock.Unlock()
		conn.Close()
	}()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	header := tcpHeader{}
	var sendResp tcpResponse
	for {
		// Get the header
		if err := dec.Decode(&header); err != nil {
			if atomic.LoadInt32(&t.shutdown) == 0 && err.Error() != "EOF" {
				log.Printf("[ERR] Failed to decode TCP header! Got %s", err)
			}
			return
		}

		// Read in the body and process request
		switch header.ReqType {
		case tcpPing:
			body := tcpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			_, ok := t.get(body.Vn)
			if ok {
				sendResp = &tcpBodyBoolError{B: ok}
			} else {
				sendResp = &tcpBodyBoolError{B: ok}
				sendResp.SetError(fmt.Errorf("Target VN not found! Target %s:%s", body.Vn.Host, body.Vn.String()))
			}

		case tcpListReq:
			body := tcpBodyString{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate all the local clients
			res := make([]*Vnode, 0, len(t.local))

			// Build list
			t.lock.RLock()
			for _, v := range t.local {
				res = append(res, v.vnode)
			}
			t.lock.RUnlock()

			// Make response
			sendResp = &tcpBodyVnodeListError{Vnodes: trimSlice(res)}

		case tcpGetPredReq:
			body := tcpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := tcpBodyVnodeError{}
			sendResp = &resp
			if ok {
				node, err := obj.GetPredecessor()
				resp.Vnode = node
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())
			}

		case tcpNotifyReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := tcpBodyVnodeListError{}
			sendResp = &resp
			if ok {
				nodes, err := obj.Notify(body.Vn)
				resp.Vnodes = trimSlice(nodes)
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Target.Host, body.Target.String())
			}

		case tcpFindSucReq:
			body := tcpBodyFindSuc{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := tcpBodyVnodeListError{}
			sendResp = &resp
			if ok {
				nodes, err := obj.FindSuccessors(body.Num, body.Key)
				resp.Vnodes = trimSlice(nodes)
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Target.Host, body.Target.String())
			}

		case tcpClearPredReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := tcpBodyError{}
			sendResp = &resp
			if ok {
				resp.Err = obj.ClearPredecessor(body.Vn)
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Target.Host, body.Target.String())
			}

		case tcpSkipSucReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Target)
			resp := tcpBodyError{}
			sendResp = &resp
			if ok {
				resp.Err = obj.SkipSuccessor(body.Vn)
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Target.Host, body.Target.String())
			}

		case tcpGet:
			body := tcpBodyGet{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vnode)
			resp := tcpBodyRespValue{}
			sendResp = &resp
			if ok {
				value, err := obj.Get(body.Key, body.Version)
				resp.Value = value
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vnode.Host, body.Vnode.String())
			}

		case tcpSet:
			body := tcpBodySet{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vnode)
			resp := tcpBodyError{}
			sendResp = &resp
			if ok {
				err := obj.Set(body.Key, body.Version, body.Value)

				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vnode.Host, body.Vnode.String())
			}

		case tcpList:
			body := tcpBodyList{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vnode)
			resp := tcpBodyRespKeys{}
			sendResp = &resp
			if ok {
				keys, err := obj.List()
				resp.Keys = keys
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vnode.Host, body.Vnode.String())
			}

		case tcpRLockReq:
			body := tcpBodyLMRLockReq{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := tcpBodyLMRLockResp{}
			sendResp = &resp
			if ok {
				lockId, version, err :=
					obj.RLock(body.Key, body.SenderID, conn.RemoteAddr().String())

				resp.Err = err
				resp.LockId = lockId
				resp.Version = version
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())
			}

		case tcpWLockReq:
			body := tcpBodyLMWLockReq{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := tcpBodyLMWLockResp{}
			sendResp = &resp
			if ok {
				lockId, version, timeout, err :=
					obj.WLock(body.Key, body.Version, body.Timeout, body.SenderID)

				resp.Err = err
				resp.LockId = lockId
				resp.Version = version
				resp.Timeout = timeout
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())
			}

		case tcpCommitWLockReq:
			body := tcpBodyLMCommitWLockReq{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := tcpBodyLMCommitWLockResp{}
			sendResp = &resp
			if ok {
				err :=
					obj.CommitWLock(body.Key, body.Version, body.SenderID)
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())
			}

		case tcpAbortWLockReq:
			body := tcpBodyLMAbortWLockReq{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.Vn)
			resp := tcpBodyLMAbortWLockResp{}
			sendResp = &resp
			if ok {
				err :=
					obj.AbortWLock(body.Key, body.Version, body.SenderID)
				resp.Err = err
			} else {
				resp.Err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())
			}

		default:
			log.Printf("[ERR] Unknown request type! Got %d", header.ReqType)
			return
		}

		// Send the response
		if err := enc.Encode(sendResp); err != nil {
			log.Printf("[ERR] Failed to send TCP body! Got %s", err)
			return
		}
	}
}

// Trims the slice to remove nil elements
func trimSlice(vn []*Vnode) []*Vnode {
	if vn == nil {
		return vn
	}

	// Find a non-nil index
	idx := len(vn) - 1

	for vn[idx] == nil {
		idx--
	}
	return vn[:idx+1]
}
