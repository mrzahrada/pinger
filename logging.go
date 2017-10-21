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
			"endpoint", job.Endpoint,
			"scheduled", job.Scheduled,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	job, err = mw.next.Get(key)
	return
}

func (mw loggingMiddleware) Put(job *Job) (err error) {
	defer func(begin time.Time) {
		_ = mw.logger.Log(
			"method", "put",
			"endpoint", job.Endpoint,
			"scheduled", job.Scheduled,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	err = mw.next.Put(job)
	return
}

func (mw loggingMiddleware) Update(key string, job *Job) (err error) {
	defer func(begin time.Time) {
		_ = mw.logger.Log(
			"method", "update",
			"key", key,
			"endpoint", job.Endpoint,
			"scheduled", job.Scheduled,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	err = mw.next.Update(key, job)
	return
}

func (mw loggingMiddleware) Delete(key string) (err error) {
	defer func(begin time.Time) {
		_ = mw.logger.Log(
			"method", "delete",
			"key", key,
			"err", err,
			"took", time.Since(begin),
		)
	}(time.Now())

	err = mw.next.Delete(key)
	return
}
