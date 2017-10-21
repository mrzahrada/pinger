package main

import (
	"errors"
	"fmt"
	"sort"
	"time"

	chord "github.com/armon/go-chord"
)

type delegate struct{}

func NewDelegate() (*delegate, error) {
	return &delegate{}, nil
}

// Implements chord.Delegate interface
func (d *delegate) NewPredecessor(local, remoteNew, remotePrev *chord.Vnode) {
	fmt.Println("new predecessor")
}

// Implements chord.Delegate interface
func (d *delegate) Leaving(local, pred, succ *chord.Vnode) {
	fmt.Println("leaving")
}

// Implements chord.Delegate interface
func (d *delegate) PredecessorLeaving(local, remote *chord.Vnode) {
	fmt.Println("predecessor leaving")
}

// Implements chord.Delegate interface
func (d *delegate) SuccessorLeaving(local, remote *chord.Vnode) {
	fmt.Println("successor leaving")
}

// Implements chord.Delegate interface
func (d *delegate) Shutdown() {
	fmt.Println("shutdown")
}

type PingerService interface {
	Get(key string) (*Job, error)
	Put(j *Job) error
	Update(key string, j *Job) error
	Delete(key string) error
	//List(start, end time.Time) ([]*Job, error)
}

type Pinger struct {
	ring      *chord.Ring
	transport *Transport
	store     *Store
	replicas  int
}

func NewPinger(nodeAddr, existingAddr, pingerDir string) (PingerService, error) {

	store, err := NewStore(pingerDir)
	if err != nil {
		return nil, err
	}

	delegate, _ := NewDelegate()
	config := chord.DefaultConfig(nodeAddr)
	{
		config.NumSuccessors = 3
		config.Delegate = delegate
	}
	timeout := time.Duration(time.Second)
	transport, err := NewTransport(nodeAddr, timeout, store)
	if err != nil {
		return nil, err
	}

	ring := &chord.Ring{}
	if existingAddr == "" {
		ring, err = chord.Create(config, transport)
	} else {
		ring, err = chord.Join(config, transport, existingAddr)
	}

	if err != nil {
		return nil, err
	}
	sort.Sort(ring)

	return Pinger{
		ring:      ring,
		transport: transport,
		store:     store,
		replicas:  2,
	}, nil
}

func (p *Pinger) Shutdown() {
	p.ring.Shutdown()
	p.transport.Shutdown()
}

func (p Pinger) Get(key string) (*Job, error) {
	// get all replicas
	nodes, err := p.ring.Lookup(p.replicas, []byte(key))
	if err != nil {
		return nil, err
	}
	job := &Job{}
	ok := false
	for _, node := range nodes {
		job, err = p.transport.SendPingerGetRequest(node.Host, key)
		if err != nil {
			continue
		}
		ok = true
	}
	if !ok {
		return nil, errors.New("Job not found")
	}
	return job, nil
}

func (p Pinger) Put(j *Job) error {
	key := j.Key()

	nodes, err := p.ring.Lookup(p.replicas, []byte(key))
	if err != nil {
		return err
	}

	for _, node := range nodes {
		err := p.transport.SendPingerPutRequest(node.Host, key, j)
		if err != nil {
			// TODO: failed to save data >> cleanup
			return err
		}
	}
	return nil
}

func (p Pinger) Update(key string, j *Job) error {
	if err := p.Delete(key); err != nil {
		return err
	}
	return p.Put(j)
}

func (p Pinger) Delete(key string) error {

	nodes, err := p.ring.Lookup(p.replicas, []byte(key))
	if err != nil {
		return err
	}

	for _, node := range nodes {
		err := p.transport.SendPingerDeleteRequest(node.Host, key)
		if err != nil {
			return err
		}
	}
	return nil

}

// Append state for job (SuccessState, ErrorState, TimeoutState, ...)
// func (p *Pinger) AppendState(key, state string) error {}

// func (p *Pinger) List(start, end time.Time) ([]*Job, error) {}
// func (p *Pinger) CountJobsForEndpoint(endpoint string) (int, error) {}
