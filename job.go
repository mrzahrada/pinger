package main

import (
	"context"
	"fmt"
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

// NOTE: not safe to call concurrently
func (j *Job) Init(ctx context.Context) {
	ctxNew, cancel := context.WithCancel(ctx)
	j.ctx = ctxNew
	j.cancel = cancel
}

func (j *Job) Key() string {
	return ""
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
