// TODO: implement pinger methods
// TODO: connect transport to store

package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	chord "github.com/armon/go-chord"
)

type localRPC struct {
	vnode *chord.Vnode
	obj   chord.VnodeRPC
}

type Transport struct {
	sock     *net.TCPListener
	timeout  time.Duration
	maxIdle  time.Duration
	lock     sync.RWMutex
	local    map[string]*localRPC
	inbound  map[*net.TCPConn]struct{}
	poolLock sync.Mutex
	pool     map[string][]*tcpOutConn
	shutdown int32
}

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
	tcpPingerReq
)

type tcpHeader struct {
	ReqType int
}

// Potential body types
type tcpBodyError struct {
	Err error
}
type tcpBodyString struct {
	S string
}
type tcpBodyVnode struct {
	Vn *chord.Vnode
}
type tcpBodyTwoVnode struct {
	Target *chord.Vnode
	Vn     *chord.Vnode
}
type tcpBodyFindSuc struct {
	Target *chord.Vnode
	Num    int
	Key    []byte
}
type tcpBodyVnodeError struct {
	Vnode *chord.Vnode
	Err   error
}
type tcpBodyVnodeListError struct {
	Vnodes []*chord.Vnode
	Err    error
}
type tcpBodyBoolError struct {
	B   bool
	Err error
}
type tcpBodyPingerReq struct {
	Action string
	Key    string
	Data   []byte
}

// Creates a new TCP transport on the given listen address with the
// configured timeout duration.
func NewTransport(listen string, timeout time.Duration) (*Transport, error) {
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
	tcp := &Transport{sock: sock.(*net.TCPListener),
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
func (t *Transport) get(vn *chord.Vnode) (chord.VnodeRPC, bool) {
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
func (t *Transport) getConn(host string) (*tcpOutConn, error) {
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
		out.sock.Close()
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
func (t *Transport) returnConn(o *tcpOutConn) {
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
func (t *Transport) setupConn(c *net.TCPConn) {
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
}

func (t *Transport) SendPingerRequest(host, action, key string, data []byte) error {
	out, err := t.getConn(host)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)
	go func() {
		out.header.ReqType = tcpPingerReq
		body := tcpBodyPingerReq{
			Action: action,
			Key:    key,
			Data:   data,
		}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			errChan <- nil
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return err
	}
}

// Gets a list of the vnodes on the box
func (t *Transport) ListVnodes(host string) ([]*chord.Vnode, error) {
	// Get a conn
	out, err := t.getConn(host)
	if err != nil {
		return nil, err
	}

	// Response channels
	respChan := make(chan []*chord.Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpListReq
		body := tcpBodyString{S: host}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- resp.Vnodes
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Ping a Vnode, check for liveness
func (t *Transport) Ping(vn *chord.Vnode) (bool, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return false, err
	}

	// Response channels
	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpPing
		body := tcpBodyVnode{Vn: vn}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyBoolError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- resp.B
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return false, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return false, err
	case res := <-respChan:
		return res, nil
	}
}

// Request a nodes predecessor
func (t *Transport) GetPredecessor(vn *chord.Vnode) (*chord.Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan *chord.Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpGetPredReq
		body := tcpBodyVnode{Vn: vn}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyVnodeError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- resp.Vnode
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Notify our successor of ourselves
func (t *Transport) Notify(target, self *chord.Vnode) ([]*chord.Vnode, error) {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*chord.Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpNotifyReq
		body := tcpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- resp.Vnodes
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Find a successor
func (t *Transport) FindSuccessors(vn *chord.Vnode, n int, k []byte) ([]*chord.Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*chord.Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpFindSucReq
		body := tcpBodyFindSuc{Target: vn, Num: n, Key: k}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyVnodeListError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- resp.Vnodes
		} else {
			errChan <- resp.Err
		}
	}()

	select {
	case <-time.After(t.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (t *Transport) ClearPredecessor(target, self *chord.Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpClearPredReq
		body := tcpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- true
		} else {
			errChan <- resp.Err
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

// Instructs a node to skip a given successor. Used to leave.
func (t *Transport) SkipSuccessor(target, self *chord.Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send a list command
		out.header.ReqType = tcpSkipSucReq
		body := tcpBodyTwoVnode{Target: target, Vn: self}
		if err := out.enc.Encode(&out.header); err != nil {
			errChan <- err
			return
		}
		if err := out.enc.Encode(&body); err != nil {
			errChan <- err
			return
		}

		// Read in the response
		resp := tcpBodyError{}
		if err := out.dec.Decode(&resp); err != nil {
			errChan <- err
			return
		}

		// Return the connection
		t.returnConn(out)
		if resp.Err == nil {
			respChan <- true
		} else {
			errChan <- resp.Err
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

// Register for an RPC callbacks
func (t *Transport) Register(v *chord.Vnode, o chord.VnodeRPC) {
	key := v.String()
	t.lock.Lock()
	t.local[key] = &localRPC{v, o}
	t.lock.Unlock()
}

// Shutdown the TCP transport
func (t *Transport) Shutdown() {
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
func (t *Transport) reapOld() {
	for {
		if atomic.LoadInt32(&t.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		t.reapOnce()
	}
}

func (t *Transport) reapOnce() {
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
func (t *Transport) listen() {
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

// Handles inbound TCP connections
func (t *Transport) handleConn(conn *net.TCPConn) {
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
	var sendResp interface{}
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
				sendResp = tcpBodyBoolError{B: ok, Err: nil}
			} else {
				sendResp = tcpBodyBoolError{B: ok, Err: fmt.Errorf("Target VN not found! Target %s:%s",
					body.Vn.Host, body.Vn.String())}
			}

		case tcpListReq:
			body := tcpBodyString{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate all the local clients
			res := make([]*chord.Vnode, 0, len(t.local))

			// Build list
			t.lock.RLock()
			for _, v := range t.local {
				res = append(res, v.vnode)
			}
			t.lock.RUnlock()

			// Make response
			sendResp = tcpBodyVnodeListError{Vnodes: trimSlice(res)}

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
			if body.Target == nil {
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

		case tcpPingerReq:
			body := tcpBodyPingerReq{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("[ERR] Failed to decode TCP body! Got %s", err)
			}
			//TODO: pass data to pinger
			fmt.Println("received pinger request for key:", body.Key)

			// Generate a response
			resp := tcpBodyError{}
			sendResp = &resp

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
func trimSlice(vn []*chord.Vnode) []*chord.Vnode {
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
