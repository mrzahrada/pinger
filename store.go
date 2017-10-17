package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type Store struct {
	db *bolt.DB
}

func NewStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) CreateJob(key string, j *Job) error {
	stateKey := fmt.Sprintf("s.%s", key) // ??
	value := []byte("")                  // json encode

	return s.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte("jobs"))
		_, err := root.CreateBucket([]byte(stateKey))
		if err != nil {
			return err // job already exists
		}
		return root.Put([]byte(key), value)
	})
}

func (s *Store) Job(key string) (*Job, error) {
	job := new(Job)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("jobs"))
		v := b.Get([]byte(key))
		return json.Unmarshal(j, v)
	})
	return job, err
}

func (s *Store) JobsRange(from, to time.Time) ([]*Job, error) {
	min := []byte(from.Format(time.RFC3339))
	max := []byte(to.Format(time.RFC3339))

	jobs := []*Job{}
	err := s.db.View(func(tx *bolt.Tx) error {
		// Assume our events bucket exists and has RFC3339 encoded time keys.
		c := tx.Bucket([]byte("Events")).Cursor()

		// Iterate over the 90's.
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			fmt.Printf("%s: %s\n", k, v)
			job := new(Job)
			// Note single corrupted job should not end function
			if err := json.Unmarshal(job, v); err != nil {
				return err
			}
			jobs = append(jobs, job)
		}

		return nil
	})

	return jobs, err
}

// NOTE: what if job does not exists?
// key is job key
func (s *Store) SetState(key, state string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("jobs")).Bucket([]byte(fmt.Sprintf("s.%s", key)))
		stateId, err := b.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(stateId))
		return b.Put(buf, []byte(value))
	})
}

func (s *Store) ListStates(key string) ([]string, error) {
	states = []string{}
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("jobs")).Bucket([]byte(fmt.Sprintf("s.%s", key)))
		b.ForEach(func(_, v []byte) error {
			states = append(states, string(v))
			return nil
		})
		return nil
	})
}
