package main

import (
	"time"

	"github.com/go-kit/kit/log"
)

type loggingMiddleware struct {
	logger log.Logger
	next   PingerService
}

func (mw loggingMiddleware) Get(key string) (job *Job, err error) {
	defer func(begin time.Time) {
		_ = mw.logger.Log(
			"method", "get",
			"key", key,
			"endpoint", job.endpoint,
			"scheduled", job.scheduled,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	job, err = mw.next.Get(s)
	return
}

func (mw loggingMiddleware) Put(job *Job) (err error) {
	defer func(begin time.Time) {
		_ = mw.logger.Log(
			"method", "put",
			"endpoint", job.endpoint,
			"scheduled", job.scheduled,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	err = mw.next.Job(job)
	return
}
