package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	Id         uuid.UUID
	Endpoint   string
	Expression string    // crontab line expression
	Scheduled  time.Time // start date
	Payload    []byte    // TODO: limit size

	ctx    context.Context
	cancel context.CancelFunc
}

func NewRandomJob() *Job {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Job{
		Id:        uuid.New(),
		Endpoint:  "example.com/endpoint",
		Scheduled: time.Now().Add(time.Duration(r.Int63n(100)) * time.Minute),
	}
}

// NOTE: not safe to call concurrently
func (j *Job) Init(ctx context.Context) {
	ctxNew, cancel := context.WithCancel(ctx)
	j.ctx = ctxNew
	j.cancel = cancel
}

func (j *Job) Key() string {
	return fmt.Sprintf("%d.%s", j.Scheduled.UnixNano(), j.Id.String())
}

func (j *Job) Start() {
	select {
	case <-time.After(time.Until(j.Scheduled)):
		if err := j.ping(); err != nil {
			// TODO: job failed
		}
	case <-j.ctx.Done():
		// TODO: log exit
	}

}

func (j *Job) Cancel() {
	j.cancel()
}

func (j *Job) ping() error {
	fmt.Println("runnig job at:", time.Now(), "scheduled:", j.Scheduled, "")
	return nil
}
