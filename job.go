package main

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	Id         uuid.UUID
	Endpoint   string
	Expression string    // crontab line expression
	Scheduled  time.Time // start date
	Payload    []byte    // TODO: limit size
}

func (j *Job) Key() string {
	return ""
}

func (j *Job) Run()     {}
func (j *Job) runCron() {}
func (j *Job) Cancel()  {}
